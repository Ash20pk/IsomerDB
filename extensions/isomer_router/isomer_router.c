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
 *
 * It also honours read-your-writes tokens on plain SQL reads (PRD FR-R4). With
 *
 *   SET isomer.read_after = <token>     (from isomer.last_write_token(); '' turns it off)
 *   SET isomer.read_after_timeout = 5s
 *
 * a top-level SELECT that reads a projection first waits, through isomer.wait_for(), until that
 * projection reflects the token, then runs with a snapshot taken after the wait. Under READ
 * COMMITTED the statement's snapshot is taken before the executor starts, so without the fresh
 * snapshot the read would not see what it waited for. A projection known to have reached a token
 * is not waited on again for that token in this backend.
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
#include "catalog/pg_type_d.h"
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

/* read-your-writes on SQL reads */
static char *read_after = NULL;
static int	read_after_timeout_ms = 5000;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static int	exec_nesting = 0;
static bool in_wait = false;

typedef struct ProjectionRel
{
	Oid			relid;			/* hash key */
	char		projection[NAMEDATALEN];
} ProjectionRel;

typedef struct ProjectionCollection
{
	char	   *database;
	char	   *collection;
	char	   *projection;
} ProjectionCollection;

typedef struct Reached
{
	char		projection[NAMEDATALEN];	/* hash key */
	int64		token;
} Reached;

static HTAB *projection_rels = NULL;
static List *projection_collections = NIL;
static MemoryContext projection_cxt = NULL;
static TimestampTz projection_rels_loaded_at = 0;
static bool projection_rels_ok = false;
static HTAB *reached = NULL;

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

/*
 * Load which relations (and documentdb_api.collection() arguments) belong to which projection,
 * through isomer._projection_relations(), as the bootstrap superuser. On failure the map is marked
 * unusable, and reads wait for every projection instead: never less than asked for.
 */
static void
load_projection_rels(void)
{
	HASHCTL		ctl;
	MemoryContext caller_cxt = CurrentMemoryContext;
	ResourceOwner caller_owner = CurrentResourceOwner;
	Oid			save_userid;
	int			save_sec;

	if (projection_cxt == NULL)
		projection_cxt = AllocSetContextCreate(TopMemoryContext, "isomer_router projections",
											   ALLOCSET_SMALL_SIZES);
	else
		MemoryContextReset(projection_cxt);
	projection_collections = NIL;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(ProjectionRel);
	ctl.hcxt = projection_cxt;
	projection_rels = hash_create("isomer_router projections", 64, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	projection_rels_loaded_at = GetCurrentTimestamp();
	projection_rels_ok = false;

	GetUserIdAndSecContext(&save_userid, &save_sec);
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(caller_cxt);
	PG_TRY();
	{
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, save_sec | SECURITY_LOCAL_USERID_CHANGE);
		if (OidIsValid(get_func_oid_simple("isomer", "_projection_relations"))
			&& SPI_connect() == SPI_OK_CONNECT)
		{
			bool		pushed = false;

			if (!ActiveSnapshotSet())
			{
				PushActiveSnapshot(GetTransactionSnapshot());
				pushed = true;
			}
			if (SPI_execute("SELECT relid, projection, mongo_database, collection FROM isomer._projection_relations()",
							true, 0) == SPI_OK_SELECT)
			{
				for (uint64 i = 0; i < SPI_processed; i++)
				{
					HeapTuple	tup = SPI_tuptable->vals[i];
					TupleDesc	desc = SPI_tuptable->tupdesc;
					bool		isnull;
					Datum		relid = SPI_getbinval(tup, desc, 1, &isnull);
					char	   *projection = SPI_getvalue(tup, desc, 2);
					MemoryContext old = MemoryContextSwitchTo(projection_cxt);

					if (!isnull)
					{
						Oid			oid = DatumGetObjectId(relid);
						ProjectionRel *r = (ProjectionRel *) hash_search(projection_rels, &oid, HASH_ENTER, NULL);

						strlcpy(r->projection, projection, NAMEDATALEN);
					}
					else
					{
						ProjectionCollection *c = palloc(sizeof(ProjectionCollection));

						c->database = pstrdup(SPI_getvalue(tup, desc, 3));
						c->collection = pstrdup(SPI_getvalue(tup, desc, 4));
						c->projection = pstrdup(projection);
						projection_collections = lappend(projection_collections, c);
					}
					MemoryContextSwitchTo(old);
				}
				projection_rels_ok = true;
			}
			if (pushed)
				PopActiveSnapshot();
			SPI_finish();
		}
		else
			projection_rels_ok = true;	/* no extension in this database: no projections */
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
		projection_collections = NIL;
		ereport(WARNING, (errmsg("isomer_router: could not load projections; isomer.read_after waits for all of them: %s",
								 edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
}

static const char *
const_text(Node *node)
{
	if (node && IsA(node, Const) && !((Const *) node)->constisnull
		&& ((Const *) node)->consttype == TEXTOID)
		return TextDatumGetCString(((Const *) node)->constvalue);
	return NULL;
}

/*
 * The projections a plan reads: its relations (permission entries cover views as well as the
 * tables under them), plus documentdb_api.collection(db, name) scans. *all is set when one can't
 * be told apart (a collection() call with non-constant arguments, or an unusable map).
 */
static List *
projections_read(PlannedStmt *stmt, bool *all)
{
	List	   *names = NIL;
	ListCell   *lc;

	*all = !projection_rels_ok;
	if (*all)
		return NIL;

	foreach(lc, stmt->permInfos)
	{
		RTEPermissionInfo *perm = (RTEPermissionInfo *) lfirst(lc);
		ProjectionRel *r = (ProjectionRel *) hash_search(projection_rels, &perm->relid, HASH_FIND, NULL);

		if (r)
			names = list_append_unique(names, makeString(pstrdup(r->projection)));
	}

	if (projection_collections == NIL)
		return names;
	foreach(lc, stmt->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
		ListCell   *lf;

		if (rte->rtekind != RTE_FUNCTION)
			continue;
		foreach(lf, rte->functions)
		{
			RangeTblFunction *rtf = (RangeTblFunction *) lfirst(lf);
			FuncExpr   *fe = (FuncExpr *) rtf->funcexpr;
			char	   *nsp;
			char	   *fname;
			const char *db;
			const char *coll;
			ListCell   *lp;

			if (!IsA(fe, FuncExpr) || list_length(fe->args) != 2)
				continue;
			fname = get_func_name(fe->funcid);
			nsp = get_namespace_name(get_func_namespace(fe->funcid));
			if (!fname || !nsp || strcmp(fname, "collection") != 0 || strcmp(nsp, "documentdb_api") != 0)
				continue;
			db = const_text(linitial(fe->args));
			coll = const_text(lsecond(fe->args));
			if (!db || !coll)
			{
				*all = true;
				return NIL;
			}
			foreach(lp, projection_collections)
			{
				ProjectionCollection *c = (ProjectionCollection *) lfirst(lp);

				if (strcmp(c->database, db) == 0 && strcmp(c->collection, coll) == 0)
					names = list_append_unique(names, makeString(pstrdup(c->projection)));
			}
		}
	}
	return names;
}

static bool
already_reached(const char *projection, int64 token)
{
	Reached    *r;
	char		key[NAMEDATALEN] = {0};

	if (reached == NULL)
		return false;
	strlcpy(key, projection, NAMEDATALEN);
	r = (Reached *) hash_search(reached, key, HASH_FIND, NULL);
	return r != NULL && r->token >= token;
}

static void
remember_reached(const char *projection, int64 token)
{
	Reached    *r;
	char		key[NAMEDATALEN] = {0};

	if (reached == NULL || hash_get_num_entries(reached) > 4096)
	{
		HASHCTL		ctl;

		if (reached)
			hash_destroy(reached);
		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = NAMEDATALEN;
		ctl.entrysize = sizeof(Reached);
		ctl.hcxt = TopMemoryContext;
		reached = hash_create("isomer_router reached tokens", 64, &ctl, HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	}
	strlcpy(key, projection, NAMEDATALEN);
	r = (Reached *) hash_search(reached, key, HASH_ENTER, NULL);
	r->token = token;
}

/*
 * Wait until every projection the statement reads reflects isomer.read_after. Errors (the "not
 * yet" timeout, IS001) reach the caller unchanged. Returns whether anything was waited for.
 */
static bool
wait_for_read_after(QueryDesc *queryDesc, int64 token)
{
	bool		all;
	List	   *names;
	List	   *pending = NIL;
	ListCell   *lc;
	Datum		args[3];
	Oid			argtypes[3] = {INT8OID, TEXTARRAYOID, TEXTOID};
	char		timeout[32];

	if (projection_rels == NULL
		|| TimestampDifferenceExceeds(projection_rels_loaded_at, GetCurrentTimestamp(), 2000))
		load_projection_rels();

	names = projections_read(queryDesc->plannedstmt, &all);
	if (!all)
	{
		foreach(lc, names)
			if (!already_reached(strVal(lfirst(lc)), token))
				pending = lappend(pending, lfirst(lc));
		if (pending == NIL)
			return false;
	}

	snprintf(timeout, sizeof(timeout), "%d milliseconds", read_after_timeout_ms);
	args[0] = Int64GetDatum(token);
	if (all)
		args[1] = (Datum) 0;
	else
	{
		Datum	   *elems = palloc(sizeof(Datum) * list_length(pending));
		int			n = 0;

		foreach(lc, pending)
			elems[n++] = CStringGetTextDatum(strVal(lfirst(lc)));
		args[1] = PointerGetDatum(construct_array_builtin(elems, n, TEXTOID));
	}
	args[2] = CStringGetTextDatum(timeout);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "isomer_router: SPI_connect failed");
	SPI_execute_with_args(all
						  ? "SELECT isomer.wait_for($1, NULL, $3::interval)"
						  : "SELECT isomer.wait_for($1, p, $3::interval) FROM unnest($2) p",
						  3, argtypes, args, all ? " n " : "   ", false, 0);
	SPI_finish();

	if (all)
	{
		/* everything reached it; remember nothing (the map may be what failed) */
		return true;
	}
	foreach(lc, pending)
		remember_reached(strVal(lfirst(lc)), token);
	return true;
}

static void
isomer_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (exec_nesting == 0 && !in_wait && !in_router && read_after != NULL && read_after[0] != '\0'
		&& queryDesc->operation == CMD_SELECT && (eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0
		&& IsTransactionState())
	{
		int64		token;
		char	   *end;
		bool		waited = false;

		errno = 0;
		token = strtoi64(read_after, &end, 10);
		if (errno != 0 || *end != '\0' || token < 0)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("isomer.read_after must be a token from isomer.last_write_token(), not \"%s\"",
								   read_after)));
		in_wait = true;
		PG_TRY();
		{
			waited = wait_for_read_after(queryDesc, token);
		}
		PG_FINALLY();
		{
			in_wait = false;
		}
		PG_END_TRY();

		/*
		 * Read with a snapshot taken after the wait. wait_for() refuses anything but READ
		 * COMMITTED, where each statement may take a new snapshot.
		 */
		if (waited && !IsolationUsesXactSnapshot())
		{
			UnregisterSnapshot(queryDesc->snapshot);
			queryDesc->snapshot = RegisterSnapshot(GetTransactionSnapshot());
		}
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

/* Track executor nesting, so only top-level statements wait (not queries inside functions). */
static void
isomer_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	exec_nesting++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
	}
	PG_FINALLY();
	{
		exec_nesting--;
	}
	PG_END_TRY();
}

static void
isomer_ExecutorFinish(QueryDesc *queryDesc)
{
	exec_nesting++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		exec_nesting--;
	}
	PG_END_TRY();
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

	DefineCustomStringVariable("isomer.read_after",
							   "Reads of projections wait until they reflect this write token ('' for no wait).",
							   NULL, &read_after, "", PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("isomer.read_after_timeout",
							"How long a read waits for isomer.read_after before failing with \"not yet\" (IS001).",
							NULL, &read_after_timeout_ms, 5000, 0, INT_MAX,
							PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);

	prev_planner_hook = planner_hook;
	planner_hook = isomer_planner;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = isomer_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = isomer_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = isomer_ExecutorFinish;
}
