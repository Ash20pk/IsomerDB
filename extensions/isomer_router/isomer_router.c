/*-------------------------------------------------------------------------
 * isomer_router: route keyed reads onto IsomerDB projections (PRD FR-M4).
 *
 * A planner hook. When a SELECT's query id has an enabled route in isomer.routes, the query is
 * replaced by the route's replacement SQL (a keyed read of a sync cache projection that returns
 * the same columns), analysed with the same parameter types and planned in its place. Routes are
 * created by isomer.route_proposal(), which only enables one after checking that the replacement
 * returns the same rows on sample keys and is measurably faster.
 *
 * isomer.routing = on | off | shadow   (shadow: count matches, never route)
 * isomer.routing_min_speedup = 1.2     (routes measured below this are ignored)
 *
 * Routes are cached per backend and reloaded every two seconds.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "utils/rls.h"
#include "utils/resowner.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "parser/parser.h"
#include "parser/parse_coerce.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/syscache.h"
#include "utils/catcache.h"
#include "catalog/pg_proc.h"
#include "funcapi.h"

PG_MODULE_MAGIC;

typedef enum { ROUTING_OFF, ROUTING_ON, ROUTING_SHADOW } RoutingMode;

static const struct config_enum_entry routing_options[] = {
	{"off", ROUTING_OFF, false},
	{"on", ROUTING_ON, false},
	{"shadow", ROUTING_SHADOW, false},
	{NULL, 0, false}};

static int routing_mode = ROUTING_ON;
static double min_speedup = 1.2;

typedef struct Route
{
	int64		queryid;		/* hash key */
	char	   *sql;
	int			nparams;
	Oid		   *param_types;
	double		speedup;
} Route;

static HTAB *routes = NULL;
static MemoryContext route_cxt = NULL;
static TimestampTz routes_loaded_at = 0;
static bool in_router = false;
static int64 shadow_matches = 0;
static int64 routed_count = 0;

static planner_hook_type prev_planner_hook = NULL;

void _PG_init(void);
static Oid get_func_oid_simple(const char *nsp, const char *name);
static PlannedStmt *isomer_planner(Query *parse, const char *query_string,
								   int cursorOptions, ParamListInfo boundParams);

PG_FUNCTION_INFO_V1(isomer_router_stats);

/*
 * Load enabled routes into a backend-local hash table, through isomer._routes() (SECURITY DEFINER,
 * so any role can load them). It runs in a subtransaction: if anything goes wrong, routing is
 * skipped for a while and the query is planned normally, never failed.
 */
static void
load_routes(void)
{
	HASHCTL		ctl;
	MemoryContext caller_cxt = CurrentMemoryContext;
	ResourceOwner caller_owner = CurrentResourceOwner;

	if (route_cxt == NULL)
		route_cxt = AllocSetContextCreate(TopMemoryContext, "isomer_router routes",
										  ALLOCSET_SMALL_SIZES);
	else
		MemoryContextReset(route_cxt);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(int64);
	ctl.entrysize = sizeof(Route);
	ctl.hcxt = route_cxt;
	routes = hash_create("isomer_router routes", 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	routes_loaded_at = GetCurrentTimestamp();

	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(caller_cxt);
	PG_TRY();
	{
		/* No catalog (the extension isn't installed in this database): no routes. */
		if (OidIsValid(get_func_oid_simple("isomer", "_routes")) && SPI_connect() == SPI_OK_CONNECT)
		{
			bool		pushed = false;
			uint64		i;

			if (!ActiveSnapshotSet())
			{
				PushActiveSnapshot(GetTransactionSnapshot());
				pushed = true;
			}
			if (SPI_execute("SELECT queryid, replacement_sql, param_types, measured_speedup FROM isomer._routes()",
							true, 0) == SPI_OK_SELECT)
			{
				for (i = 0; i < SPI_processed; i++)
				{
					HeapTuple	tup = SPI_tuptable->vals[i];
					TupleDesc	desc = SPI_tuptable->tupdesc;
					bool		isnull;
					int64		qid = DatumGetInt64(SPI_getbinval(tup, desc, 1, &isnull));
					char	   *sql = SPI_getvalue(tup, desc, 2);
					ArrayType  *arr = DatumGetArrayTypeP(SPI_getbinval(tup, desc, 3, &isnull));
					Datum		speed = SPI_getbinval(tup, desc, 4, &isnull);
					int			n = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
					MemoryContext old = MemoryContextSwitchTo(route_cxt);
					bool		found;
					Route	   *r = (Route *) hash_search(routes, &qid, HASH_ENTER, &found);

					r->sql = pstrdup(sql);
					r->nparams = n;
					r->param_types = palloc(sizeof(Oid) * Max(n, 1));
					memcpy(r->param_types, ARR_DATA_PTR(arr), sizeof(Oid) * n);
					r->speedup = isnull ? 0 : DatumGetFloat8(speed);
					MemoryContextSwitchTo(old);
				}
			}
			if (pushed)
				PopActiveSnapshot();
			SPI_finish();
		}
		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(caller_cxt);
		CurrentResourceOwner = caller_owner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(caller_cxt);
		edata = CopyErrorData();
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(caller_cxt);
		CurrentResourceOwner = caller_owner;
		hash_destroy(routes);
		routes = hash_create("isomer_router routes", 4, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		ereport(WARNING, (errmsg("isomer_router: could not load routes, not routing: %s", edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
}

/* The caller may read every relation the original query reads, and none has row-level security. */
static bool
original_is_readable(Query *parse)
{
	ListCell   *lc;

	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION && check_enable_rls(rte->relid, InvalidOid, true) != RLS_NONE)
			return false;
	}
	return ExecCheckPermissions(parse->rtable, parse->rteperminfos, false);
}

static Oid
get_func_oid_simple(const char *nsp, const char *name)
{
	Oid			nspid = get_namespace_oid(nsp, true);
	CatCList   *catlist;
	Oid			result = InvalidOid;

	if (!OidIsValid(nspid))
		return InvalidOid;
	catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(name));
	for (int i = 0; i < catlist->n_members; i++)
	{
		HeapTuple	tup = &catlist->members[i]->tuple;
		Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(tup);

		if (proc->pronamespace == nspid)
		{
			result = proc->oid;
			break;
		}
	}
	ReleaseSysCacheList(catlist);
	return result;
}

/* Same visible columns, same types: the replacement can stand in for the original. */
static bool
targetlists_match(List *a, List *b)
{
	ListCell   *la;
	ListCell   *lb;
	List	   *va = NIL;
	List	   *vb = NIL;

	foreach(la, a)
		if (!((TargetEntry *) lfirst(la))->resjunk)
			va = lappend(va, lfirst(la));
	foreach(lb, b)
		if (!((TargetEntry *) lfirst(lb))->resjunk)
			vb = lappend(vb, lfirst(lb));
	if (list_length(va) != list_length(vb))
		return false;
	forboth(la, va, lb, vb)
	{
		TargetEntry *ta = (TargetEntry *) lfirst(la);
		TargetEntry *tb = (TargetEntry *) lfirst(lb);

		if (exprType((Node *) ta->expr) != exprType((Node *) tb->expr))
			return false;
	}
	return true;
}

static PlannedStmt *
next_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
	if (prev_planner_hook)
		return prev_planner_hook(parse, query_string, cursorOptions, boundParams);
	return standard_planner(parse, query_string, cursorOptions, boundParams);
}

static PlannedStmt *
isomer_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
	Route	   *route;
	int64		qid = parse->queryId;

	if (routing_mode == ROUTING_OFF || in_router || parse->commandType != CMD_SELECT || qid == 0
		|| !IsTransactionState() || IsInParallelMode())
		return next_planner(parse, query_string, cursorOptions, boundParams);

	if (routes == NULL || TimestampDifferenceExceeds(routes_loaded_at, GetCurrentTimestamp(), 2000))
	{
		in_router = true;
		PG_TRY();
		{
			load_routes();
		}
		PG_FINALLY();
		{
			in_router = false;
		}
		PG_END_TRY();
	}

	route = (Route *) hash_search(routes, &qid, HASH_FIND, NULL);
	if (route == NULL || route->speedup < min_speedup)
		return next_planner(parse, query_string, cursorOptions, boundParams);

	if (routing_mode == ROUTING_SHADOW)
	{
		shadow_matches++;
		return next_planner(parse, query_string, cursorOptions, boundParams);
	}

	/*
	 * Only route a read the caller could do on the source tables: otherwise the original runs and
	 * fails as it should. Row-level security is never bypassed: such queries are not routed.
	 */
	if (!original_is_readable(parse))
		return next_planner(parse, query_string, cursorOptions, boundParams);

	{
		List	   *raw = raw_parser(route->sql, RAW_PARSE_DEFAULT);
		List	   *queries = NIL;
		Query	   *replacement;
		Oid			save_userid;
		int			save_sec;
		MemoryContext caller_cxt = CurrentMemoryContext;
		ResourceOwner caller_owner = CurrentResourceOwner;
		bool		ok = true;

		if (list_length(raw) != 1)
			return next_planner(parse, query_string, cursorOptions, boundParams);

		/*
		 * Analyse the replacement as the bootstrap superuser: it names IsomerDB's internal storage,
		 * which application roles can't see. The caller's own rights were checked against the
		 * original query above. A subtransaction keeps a stale or broken route from failing the
		 * query: it is then planned as written.
		 */
		GetUserIdAndSecContext(&save_userid, &save_sec);
		in_router = true;
		BeginInternalSubTransaction(NULL);
		MemoryContextSwitchTo(caller_cxt);
		PG_TRY();
		{
			SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, save_sec | SECURITY_LOCAL_USERID_CHANGE);
			queries = pg_analyze_and_rewrite_fixedparams(linitial_node(RawStmt, raw), route->sql,
														 route->param_types, route->nparams, NULL);
			SetUserIdAndSecContext(save_userid, save_sec);
			ReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(caller_cxt);
			CurrentResourceOwner = caller_owner;
		}
		PG_CATCH();
		{
			ErrorData  *edata;

			SetUserIdAndSecContext(save_userid, save_sec);
			MemoryContextSwitchTo(caller_cxt);
			edata = CopyErrorData();
			FlushErrorState();
			RollbackAndReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(caller_cxt);
			CurrentResourceOwner = caller_owner;
			ereport(DEBUG1, (errmsg("isomer_router: route for query %lld not usable, planning as written: %s",
									(long long) qid, edata->message)));
			FreeErrorData(edata);
			ok = false;
		}
		PG_END_TRY();
		in_router = false;
		if (!ok || list_length(queries) != 1)
			return next_planner(parse, query_string, cursorOptions, boundParams);
		replacement = linitial_node(Query, queries);
		if (!targetlists_match(parse->targetList, replacement->targetList))
		{
			elog(DEBUG1, "isomer_router: route for query %lld no longer matches its columns; not routing",
				 (long long) qid);
			return next_planner(parse, query_string, cursorOptions, boundParams);
		}
		/*
		 * The caller's right to these rows was checked against the original query's tables above;
		 * the projection's internal storage is not itself granted to application roles.
		 */
		{
			ListCell   *lc;

			foreach(lc, replacement->rteperminfos)
				((RTEPermissionInfo *) lfirst(lc))->requiredPerms = 0;
		}
		replacement->queryId = qid;		/* statistics stay with the original statement */
		routed_count++;
		return next_planner(replacement, route->sql, cursorOptions, boundParams);
	}
}

/* isomer_router_stats(): what this backend has routed. */
Datum
isomer_router_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		nulls[3] = {false, false, false};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	values[0] = Int64GetDatum(routed_count);
	values[1] = Int64GetDatum(shadow_matches);
	values[2] = Int64GetDatum(routes ? hash_get_num_entries(routes) : 0);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

void
_PG_init(void)
{
	DefineCustomEnumVariable("isomer.routing",
							 "Route keyed reads onto IsomerDB projections: on, off, or shadow (count only).",
							 NULL, &routing_mode, ROUTING_ON, routing_options,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomRealVariable("isomer.routing_min_speedup",
							 "Only use routes whose measured speedup is at least this.",
							 NULL, &min_speedup, 1.2, 0.0, 1000.0,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	prev_planner_hook = planner_hook;
	planner_hook = isomer_planner;
}
