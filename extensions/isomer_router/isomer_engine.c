/*-------------------------------------------------------------------------
 * isomer_engine: the compiler behind incrementally maintained projections (PRD FR-P6, FR-S5).
 *
 * A projection that aggregates a large table across shards is expensive to maintain by
 * re-deriving each affected key from the tables: every write re-reads the whole group, and a
 * table that only reaches the key through a join re-derives every key. The incremental strategy
 * keeps the projection's own state instead. Each source change is captured as a row delta; the
 * aggregated ("fact") table is folded into per-group partial aggregates, the tables it joins are
 * folded into copies of the columns the query needs, and the keys a delta can touch are
 * re-derived from that state, which is local to the coordinator and small.
 *
 * The rules are DBSP's: filters and projections are linear, so they apply to deltas directly;
 * sum and count are linear, so their partial aggregates are integrals of the deltas; min and max
 * are kept as integrated multisets; and a join over integrated state changes only where one of
 * its inputs changed, so recomputing the keys reached from each input's delta (before and after
 * folding it) covers every key whose value changed. Pre-aggregating the fact table before the
 * join is valid because every join edge leading away from it lands on a unique column, so each
 * fact row joins at most one row of the rest.
 *
 * This file does the parts that need Postgres's own parser and deparser; the extension's SQL
 * decides what is supported, creates the state, and runs the generated statements.
 *
 *   isomer._engine_analyze(query text) -> jsonb
 *     The shape of an analysed query: relations, joins, conjuncts (and which are equi-joins),
 *     aggregates, grouping, and the columns each relation contributes.
 *
 *   isomer._engine_sql(query, mode, fact, driving, rels, templates, fact_columns) -> text
 *     The query rewritten for one job and deparsed back to SQL, with relations swapped for state
 *     or staging relations whose columns line up with the originals (so every Var stays valid):
 *       'derive'  aggregates replaced by expressions over the fact's partial aggregates, and the
 *                 fact's own filters dropped (they were applied while folding);
 *       'derive_keyed'  the same, for the keys in $1 (text[]) only: the key column named in
 *                 fact_columns must equal one of them, a condition in WHERE, so a distributed state
 *                 is filtered on each shard before it is grouped;
 *       'keys'    aggregates replaced by NULL and HAVING dropped: which keys the rows of one
 *                 relation (driving) reach; the fact's filters are dropped unless it drives;
 *       'fact'    one row per fact row: its grouping and join columns, each aggregate's argument
 *                 and filter, and its weight.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parse_expr.h"
#include "parser/parse_coerce.h"
#include "parser/parse_node.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"

PG_FUNCTION_INFO_V1(isomer_engine_analyze);
PG_FUNCTION_INFO_V1(isomer_engine_sql);

/* ── parsing ─────────────────────────────────────────────────────────────── */

static Query *
analyze_select(const char *sql)
{
	List	   *raw = raw_parser(sql, RAW_PARSE_DEFAULT);
	Query	   *q;

	if (list_length(raw) != 1)
		ereport(ERROR, (errmsg("isomer: a projection's query must be a single statement")));
	q = parse_analyze_fixedparams(linitial_node(RawStmt, raw), sql, NULL, 0, NULL);
	if (q->commandType != CMD_SELECT || q->utilityStmt != NULL)
		ereport(ERROR, (errmsg("isomer: a projection's query must be a SELECT")));
	return q;
}

/* The rtable index of the grouping step's RTE (Postgres 18), or 0. */
static int
group_rti(Query *q)
{
	ListCell   *lc;
	int			i = 0;

	foreach(lc, q->rtable)
	{
		i++;
		if (((RangeTblEntry *) lfirst(lc))->rtekind == RTE_GROUP)
			return i;
	}
	return 0;
}

/* Replace references to the grouping step by the grouping expressions themselves. */
typedef struct FlattenCtx
{
	int			grp;
	List	   *exprs;
} FlattenCtx;

static Node *
flatten_mutator(Node *node, FlattenCtx *ctx)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *v = (Var *) node;

		if (v->varlevelsup == 0 && ctx->grp > 0 && v->varno == ctx->grp)
			return copyObject(list_nth(ctx->exprs, v->varattno - 1));
		return node;
	}
	return expression_tree_mutator(node, flatten_mutator, ctx);
}

static Node *
flatten_group(Query *q, Node *node)
{
	FlattenCtx	ctx;

	ctx.grp = group_rti(q);
	if (ctx.grp == 0)
		return node;
	ctx.exprs = rt_fetch(ctx.grp, q->rtable)->groupexprs;
	return flatten_mutator(copyObject(node), &ctx);
}

/* ── walkers ─────────────────────────────────────────────────────────────── */

typedef struct VarsCtx
{
	List	   *vars;			/* Var * at level 0 */
	bool		skip_aggs;		/* don't look inside aggregates */
} VarsCtx;

static bool
vars_walker(Node *node, VarsCtx *ctx)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup == 0)
			ctx->vars = lappend(ctx->vars, node);
		return false;
	}
	if (ctx->skip_aggs && IsA(node, Aggref))
		return false;
	return expression_tree_walker(node, vars_walker, ctx);
}

static List *
vars_of(Node *node, bool skip_aggs)
{
	VarsCtx		ctx = {NIL, skip_aggs};

	vars_walker(node, &ctx);
	return ctx.vars;
}

static Bitmapset *
varnos_of(Node *node)
{
	Bitmapset  *b = NULL;
	ListCell   *lc;

	foreach(lc, vars_of(node, false))
		b = bms_add_member(b, ((Var *) lfirst(lc))->varno);
	return b;
}

/* Aggregates in the order the target list, then HAVING, mention them. */
static bool
aggs_walker(Node *node, List **aggs)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref) && ((Aggref *) node)->agglevelsup == 0)
	{
		*aggs = lappend(*aggs, node);
		return false;
	}
	return expression_tree_walker(node, aggs_walker, aggs);
}

static List *
aggs_of(Query *q)
{
	List	   *aggs = NIL;

	aggs_walker((Node *) q->targetList, &aggs);
	aggs_walker(q->havingQual, &aggs);
	return aggs;
}

static Node *
strip_relabel(Node *n)
{
	while (n && IsA(n, RelabelType))
		n = (Node *) ((RelabelType *) n)->arg;
	return n;
}

/* ── JSON output ─────────────────────────────────────────────────────────── */

static void
json_key(StringInfo b, bool *first, const char *key)
{
	if (!*first)
		appendStringInfoChar(b, ',');
	*first = false;
	escape_json(b, key);
	appendStringInfoChar(b, ':');
}

static void
json_str(StringInfo b, const char *s)
{
	if (s == NULL)
		appendStringInfoString(b, "null");
	else
		escape_json(b, s);
}

static void
json_rtis(StringInfo b, Bitmapset *set)
{
	int			x = -1;
	bool		first = true;

	appendStringInfoChar(b, '[');
	while ((x = bms_next_member(set, x)) >= 0)
	{
		if (!first)
			appendStringInfoChar(b, ',');
		first = false;
		appendStringInfo(b, "%d", x);
	}
	appendStringInfoChar(b, ']');
}

static const char *
colname(Query *q, Var *v)
{
	RangeTblEntry *rte = rt_fetch(v->varno, q->rtable);

	if (rte->rtekind != RTE_RELATION || v->varattno <= 0)
		return NULL;
	return get_attname(rte->relid, v->varattno, false);
}

/* {"rti": n, "col": "name"} for a column reference, or null for anything else. */
static void
json_colref(StringInfo b, Query *q, Node *expr)
{
	Node	   *n = strip_relabel(expr);

	if (n && IsA(n, Var) && colname(q, (Var *) n) != NULL)
	{
		appendStringInfo(b, "{\"rti\":%d,\"col\":", ((Var *) n)->varno);
		json_str(b, colname(q, (Var *) n));
		appendStringInfoChar(b, '}');
	}
	else
		appendStringInfoString(b, "null");
}

/* Conjuncts of the WHERE clause and every ON clause, each tagged with the join it came from. */
typedef struct Conjunct
{
	Node	   *expr;
	int			join;			/* index into the joins list, or -1 for WHERE */
} Conjunct;

typedef struct JoinInfo
{
	JoinType	type;
	Bitmapset  *left;
	Bitmapset  *right;
	bool		using;
} JoinInfo;

static Bitmapset *
collect_from(Node *node, List **joins, List **conjs)
{
	if (IsA(node, RangeTblRef))
		return bms_make_singleton(((RangeTblRef *) node)->rtindex);
	if (IsA(node, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) node;
		JoinInfo   *info = palloc0(sizeof(JoinInfo));
		int			idx;
		ListCell   *lc;

		info->left = collect_from(j->larg, joins, conjs);
		info->right = collect_from(j->rarg, joins, conjs);
		info->type = j->jointype;
		info->using = (j->usingClause != NIL || j->isNatural || j->alias != NULL);
		*joins = lappend(*joins, info);
		idx = list_length(*joins) - 1;
		foreach(lc, make_ands_implicit((Expr *) j->quals))
		{
			Conjunct   *c = palloc0(sizeof(Conjunct));

			c->expr = lfirst(lc);
			c->join = idx;
			*conjs = lappend(*conjs, c);
		}
		return bms_union(info->left, info->right);
	}
	if (IsA(node, FromExpr))
	{
		Bitmapset  *all = NULL;
		ListCell   *lc;

		foreach(lc, ((FromExpr *) node)->fromlist)
			all = bms_union(all, collect_from(lfirst(lc), joins, conjs));
		return all;
	}
	ereport(ERROR, (errmsg("isomer: unexpected node in a query's FROM clause")));
	return NULL;
}

static List *
conjuncts_of(Query *q, List **joins)
{
	List	   *conjs = NIL;
	ListCell   *lc;

	collect_from((Node *) q->jointree, joins, &conjs);
	foreach(lc, make_ands_implicit((Expr *) q->jointree->quals))
	{
		Conjunct   *c = palloc0(sizeof(Conjunct));

		c->expr = lfirst(lc);
		c->join = -1;
		conjs = lappend(conjs, c);
	}
	return conjs;
}

/* An equality between two plain columns, possibly of different relations. */
static bool
is_equi(Node *expr, Var **l, Var **r)
{
	OpExpr	   *op;
	Node	   *a;
	Node	   *b;

	if (!IsA(expr, OpExpr))
		return false;
	op = (OpExpr *) expr;
	if (list_length(op->args) != 2)
		return false;
	a = strip_relabel(linitial(op->args));
	b = strip_relabel(lsecond(op->args));
	if (!IsA(a, Var) || !IsA(b, Var) || ((Var *) a)->varlevelsup != 0 || ((Var *) b)->varlevelsup != 0)
		return false;
	if (!op_mergejoinable(op->opno, exprType(a)) && !op_hashjoinable(op->opno, exprType(a)))
		return false;
	*l = (Var *) a;
	*r = (Var *) b;
	return true;
}

/* ── isomer._engine_analyze ──────────────────────────────────────────────── */

Datum
isomer_engine_analyze(PG_FUNCTION_ARGS)
{
	char	   *sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	Query	   *q = analyze_select(sql);
	StringInfoData b;
	bool		first = true;
	ListCell   *lc;
	List	   *joins = NIL;
	List	   *conjs;
	List	   *aggs = aggs_of(q);
	int			rti;
	Bitmapset  *join_rtes = NULL;
	Bitmapset  *other_rtes = NULL;
	int			grp = group_rti(q);
	List	   *nonagg = NIL;
	bool		group_expr = false;

	initStringInfo(&b);
	appendStringInfoChar(&b, '{');

	/* Statement-level features the engine does not maintain. */
	json_key(&b, &first, "flags");
	appendStringInfo(&b,
					 "{\"sublinks\":%s,\"window\":%s,\"distinct\":%s,\"setops\":%s,\"cte\":%s,"
					 "\"limit\":%s,\"srf\":%s,\"grouping_sets\":%s,\"having\":%s,\"order\":%s,"
					 "\"for_update\":%s}",
					 q->hasSubLinks ? "true" : "false",
					 q->hasWindowFuncs ? "true" : "false",
					 (q->distinctClause != NIL || q->hasDistinctOn) ? "true" : "false",
					 q->setOperations ? "true" : "false",
					 q->cteList != NIL ? "true" : "false",
					 (q->limitCount || q->limitOffset) ? "true" : "false",
					 q->hasTargetSRFs ? "true" : "false",
					 q->groupingSets != NIL ? "true" : "false",
					 q->havingQual ? "true" : "false",
					 q->sortClause != NIL ? "true" : "false",
					 q->rowMarks != NIL ? "true" : "false");

	/* Relations. */
	json_key(&b, &first, "rtes");
	appendStringInfoChar(&b, '[');
	rti = 0;
	{
		bool		f2 = true;

		foreach(lc, q->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
			bool		f3 = true;

			rti++;
			if (rte->rtekind == RTE_JOIN)
			{
				join_rtes = bms_add_member(join_rtes, rti);
				continue;
			}
			if (rte->rtekind == RTE_GROUP)
				continue;
			if (rte->rtekind != RTE_RELATION)
			{
				other_rtes = bms_add_member(other_rtes, rti);
				continue;
			}
			if (!f2)
				appendStringInfoChar(&b, ',');
			f2 = false;
			appendStringInfoChar(&b, '{');
			json_key(&b, &f3, "rti");
			appendStringInfo(&b, "%d", rti);
			json_key(&b, &f3, "relid");
			appendStringInfo(&b, "%u", rte->relid);
			json_key(&b, &f3, "relkind");
			appendStringInfo(&b, "\"%c\"", rte->relkind);
			json_key(&b, &f3, "alias");
			json_str(&b, rte->eref->aliasname);
			json_key(&b, &f3, "inh");
			appendStringInfoString(&b, rte->inh ? "true" : "false");
			appendStringInfoChar(&b, '}');
		}
	}
	appendStringInfoChar(&b, ']');

	json_key(&b, &first, "other_rtes");
	appendStringInfoString(&b, bms_is_empty(other_rtes) ? "false" : "true");

	/* Every column reference, by relation; and whether any goes through a join's own columns. */
	{
		List	   *all;
		List	   *jj = NIL;
		bool		join_vars = false;

		all = list_concat(vars_of(flatten_group(q, (Node *) q->targetList), false),
						  vars_of(flatten_group(q, q->havingQual), false));
		foreach(lc, conjuncts_of(q, &jj))
			all = list_concat(all, vars_of(((Conjunct *) lfirst(lc))->expr, false));
		if (grp)
			all = list_concat(all, vars_of((Node *) rt_fetch(grp, q->rtable)->groupexprs, false));
		foreach(lc, all)
		{
			if (bms_is_member(((Var *) lfirst(lc))->varno, join_rtes))
				join_vars = true;
		}
		json_key(&b, &first, "join_vars");
		appendStringInfoString(&b, join_vars ? "true" : "false");

		json_key(&b, &first, "columns");
		appendStringInfoChar(&b, '[');
		{
			bool		f2 = true;

			foreach(lc, all)
			{
				Var		   *v = (Var *) lfirst(lc);
				const char *name = colname(q, v);

				if (name == NULL)
					continue;
				if (!f2)
					appendStringInfoChar(&b, ',');
				f2 = false;
				appendStringInfo(&b, "{\"rti\":%d,\"col\":", v->varno);
				json_str(&b, name);
				appendStringInfoChar(&b, '}');
			}
		}
		appendStringInfoChar(&b, ']');
	}

	/* Joins and conjuncts. */
	conjs = conjuncts_of(q, &joins);
	json_key(&b, &first, "joins");
	appendStringInfoChar(&b, '[');
	{
		bool		f2 = true;

		foreach(lc, joins)
		{
			JoinInfo   *j = (JoinInfo *) lfirst(lc);
			const char *type;

			switch (j->type)
			{
				case JOIN_INNER:
					type = "inner";
					break;
				case JOIN_LEFT:
					type = "left";
					break;
				case JOIN_RIGHT:
					type = "right";
					break;
				case JOIN_FULL:
					type = "full";
					break;
				default:
					type = "other";
			}
			if (!f2)
				appendStringInfoChar(&b, ',');
			f2 = false;
			appendStringInfo(&b, "{\"type\":\"%s\",\"using\":%s,\"left\":", type, j->using ? "true" : "false");
			json_rtis(&b, j->left);
			appendStringInfoString(&b, ",\"right\":");
			json_rtis(&b, j->right);
			appendStringInfoChar(&b, '}');
		}
	}
	appendStringInfoChar(&b, ']');

	json_key(&b, &first, "conjuncts");
	appendStringInfoChar(&b, '[');
	{
		bool		f2 = true;

		foreach(lc, conjs)
		{
			Conjunct   *c = (Conjunct *) lfirst(lc);
			Var		   *l;
			Var		   *r;

			if (!f2)
				appendStringInfoChar(&b, ',');
			f2 = false;
			appendStringInfo(&b, "{\"join\":%d,\"rtis\":", c->join);
			json_rtis(&b, varnos_of(c->expr));
			appendStringInfoString(&b, ",\"cols\":[");
			{
				ListCell   *lv;
				bool		f3 = true;

				foreach(lv, vars_of(c->expr, false))
				{
					if (!f3)
						appendStringInfoChar(&b, ',');
					f3 = false;
					json_colref(&b, q, lfirst(lv));
				}
			}
			appendStringInfoChar(&b, ']');
			appendStringInfoString(&b, ",\"equi\":");
			if (is_equi(c->expr, &l, &r) && l->varno != r->varno
				&& colname(q, l) != NULL && colname(q, r) != NULL)
			{
				appendStringInfoString(&b, "[");
				json_colref(&b, q, (Node *) l);
				appendStringInfoChar(&b, ',');
				json_colref(&b, q, (Node *) r);
				appendStringInfoString(&b, "]");
			}
			else
				appendStringInfoString(&b, "null");
			appendStringInfoChar(&b, '}');
		}
	}
	appendStringInfoChar(&b, ']');

	/* Aggregates. */
	json_key(&b, &first, "aggs");
	appendStringInfoChar(&b, '[');
	{
		bool		f2 = true;
		int			i = 0;

		foreach(lc, aggs)
		{
			Aggref	   *a = (Aggref *) lfirst(lc);
			Oid			argtype = InvalidOid;
			Bitmapset  *rtis = bms_union(varnos_of((Node *) a->args), varnos_of((Node *) a->aggfilter));

			if (a->args != NIL)
				argtype = exprType((Node *) ((TargetEntry *) linitial(a->args))->expr);
			if (!f2)
				appendStringInfoChar(&b, ',');
			f2 = false;
			appendStringInfo(&b, "{\"i\":%d,\"fn\":", i++);
			json_str(&b, get_func_name(a->aggfnoid));
			appendStringInfoString(&b, ",\"schema\":");
			json_str(&b, get_namespace_name(get_func_namespace(a->aggfnoid)));
			appendStringInfo(&b, ",\"star\":%s,\"distinct\":%s,\"ordered\":%s,\"filter\":%s,\"kind\":\"%c\",\"nargs\":%d,",
							 a->aggstar ? "true" : "false",
							 a->aggdistinct != NIL ? "true" : "false",
							 a->aggorder != NIL ? "true" : "false",
							 a->aggfilter ? "true" : "false",
							 a->aggkind, list_length(a->args));
			appendStringInfoString(&b, "\"argtype\":");
			json_str(&b, OidIsValid(argtype) ? format_type_be(argtype) : NULL);
			appendStringInfoString(&b, ",\"restype\":");
			json_str(&b, format_type_be(a->aggtype));
			appendStringInfoString(&b, ",\"rtis\":");
			json_rtis(&b, rtis);
			/* The argument, when it is a plain column (count(DISTINCT c) keeps c's values). */
			appendStringInfoString(&b, ",\"argcol\":");
			if (list_length(a->args) == 1)
				json_colref(&b, q, (Node *) ((TargetEntry *) linitial(a->args))->expr);
			else
				appendStringInfoString(&b, "null");
			/* Every column it reads: arguments, ORDER BY keys, FILTER, direct arguments. */
			appendStringInfoString(&b, ",\"cols\":[");
			{
				ListCell   *lv;
				bool		f3 = true;
				List	   *vs = list_concat(list_concat(vars_of((Node *) a->args, false),
														 vars_of((Node *) a->aggfilter, false)),
											 vars_of((Node *) a->aggdirectargs, false));

				foreach(lv, vs)
				{
					if (colname(q, lfirst(lv)) == NULL)
						continue;
					if (!f3)
						appendStringInfoChar(&b, ',');
					f3 = false;
					json_colref(&b, q, lfirst(lv));
				}
			}
			appendStringInfoChar(&b, ']');
			appendStringInfoChar(&b, '}');
		}
	}
	appendStringInfoChar(&b, ']');

	/* Grouping. */
	json_key(&b, &first, "group");
	appendStringInfoChar(&b, '[');
	{
		bool		f2 = true;

		foreach(lc, q->groupClause)
		{
			SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
			TargetEntry *tle = get_sortgroupclause_tle(sgc, q->targetList);
			Node	   *e = flatten_group(q, (Node *) tle->expr);

			if (!f2)
				appendStringInfoChar(&b, ',');
			f2 = false;
			json_colref(&b, q, e);
			if (!(IsA(strip_relabel(e), Var)))
				group_expr = true;
		}
	}
	appendStringInfoChar(&b, ']');
	json_key(&b, &first, "group_expr");
	appendStringInfoString(&b, group_expr ? "true" : "false");
	json_key(&b, &first, "group_volatile");
	appendStringInfoString(&b, grp && contain_volatile_functions((Node *) rt_fetch(grp, q->rtable)->groupexprs)
						   ? "true" : "false");

	/* Columns read outside aggregates (grouping, output, HAVING). */
	nonagg = list_concat(vars_of(flatten_group(q, (Node *) q->targetList), true),
						 vars_of(flatten_group(q, q->havingQual), true));
	if (grp)
		nonagg = list_concat(nonagg, vars_of((Node *) rt_fetch(grp, q->rtable)->groupexprs, true));
	json_key(&b, &first, "nonagg");
	appendStringInfoChar(&b, '[');
	{
		bool		f2 = true;

		foreach(lc, nonagg)
		{
			if (!f2)
				appendStringInfoChar(&b, ',');
			f2 = false;
			json_colref(&b, q, lfirst(lc));
		}
	}
	appendStringInfoChar(&b, ']');

	/* Output columns: name, type, and the column it is when it is one. */
	json_key(&b, &first, "target");
	appendStringInfoChar(&b, '[');
	{
		bool		f2 = true;

		foreach(lc, q->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

			if (tle->resjunk)
				continue;
			if (!f2)
				appendStringInfoChar(&b, ',');
			f2 = false;
			appendStringInfoString(&b, "{\"name\":");
			json_str(&b, tle->resname);
			appendStringInfoString(&b, ",\"type\":");
			json_str(&b, format_type_be(exprType((Node *) tle->expr)));
			appendStringInfoString(&b, ",\"col\":");
			json_colref(&b, q, flatten_group(q, (Node *) tle->expr));
			appendStringInfo(&b, ",\"grouped\":%s",
							 tle->ressortgroupref != 0
							 && get_sortgroupref_clause_noerr(tle->ressortgroupref, q->groupClause) != NULL
							 ? "true" : "false");
			appendStringInfoChar(&b, '}');
		}
	}
	appendStringInfoChar(&b, ']');

	appendStringInfoChar(&b, '}');
	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(b.data)));
}

/* ── isomer._engine_sql ──────────────────────────────────────────────────── */

typedef struct ReplaceCtx
{
	List	   *aggs;			/* the original Aggrefs, in order */
	List	   *replacements;	/* Node * per aggregate */
} ReplaceCtx;

static Node *
replace_aggs_mutator(Node *node, ReplaceCtx *ctx)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Aggref) && ((Aggref *) node)->agglevelsup == 0)
	{
		int			i = 0;
		ListCell   *lc;

		foreach(lc, ctx->aggs)
		{
			if (equal(lfirst(lc), node))
				return copyObject(list_nth(ctx->replacements, i));
			i++;
		}
		elog(ERROR, "isomer: aggregate not found while rewriting");
	}
	return expression_tree_mutator(node, replace_aggs_mutator, ctx);
}

/* An expression over one relation, given as SQL text, analysed as if that relation were rti. */
static Node *
analyze_template(const char *tmpl, Oid relid, int rti)
{
	char	   *sql = psprintf("SELECT %s", tmpl);
	List	   *raw = raw_parser(sql, RAW_PARSE_DEFAULT);
	SelectStmt *stmt = (SelectStmt *) linitial_node(RawStmt, raw)->stmt;
	ResTarget  *rt = linitial_node(ResTarget, stmt->targetList);
	ParseState *pstate = make_parsestate(NULL);
	Relation	rel = relation_open(relid, AccessShareLock);
	ParseNamespaceItem *nsitem;
	Node	   *expr;

	pstate->p_sourcetext = sql;
	nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock, NULL, false, true);
	addNSItemToQuery(pstate, nsitem, true, true, true);
	expr = transformExpr(pstate, rt->val, EXPR_KIND_SELECT_TARGET);
	relation_close(rel, AccessShareLock);
	ChangeVarNodes(expr, 1, rti, 0);
	return expr;
}

static bool
only_fact(Node *expr, int fact)
{
	Bitmapset  *b = varnos_of(expr);

	return bms_num_members(b) == 1 && bms_is_member(fact, b);
}

/* Drop (or collect) the conjuncts that read only the fact relation, in WHERE and every ON. */
static Node *
split_fact_quals(Node *quals, int fact, List **taken)
{
	List	   *keep = NIL;
	ListCell   *lc;

	foreach(lc, make_ands_implicit((Expr *) quals))
	{
		if (only_fact(lfirst(lc), fact))
			*taken = lappend(*taken, lfirst(lc));
		else
			keep = lappend(keep, lfirst(lc));
	}
	return keep == NIL ? NULL : (Node *) make_ands_explicit(keep);
}

static void
split_fact_quals_from(Node *node, int fact, List **taken)
{
	if (IsA(node, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) node;

		split_fact_quals_from(j->larg, fact, taken);
		split_fact_quals_from(j->rarg, fact, taken);
		j->quals = split_fact_quals(j->quals, fact, taken);
		if (j->quals == NULL)
			j->quals = (Node *) makeBoolConst(true, false);	/* ON needs a condition */
	}
	else if (IsA(node, FromExpr))
	{
		ListCell   *lc;

		foreach(lc, ((FromExpr *) node)->fromlist)
			split_fact_quals_from(lfirst(lc), fact, taken);
		((FromExpr *) node)->quals = split_fact_quals(((FromExpr *) node)->quals, fact, taken);
	}
}

static Bitmapset *
rtis_under(Node *node)
{
	if (IsA(node, RangeTblRef))
		return bms_make_singleton(((RangeTblRef *) node)->rtindex);
	if (IsA(node, JoinExpr))
		return bms_union(rtis_under(((JoinExpr *) node)->larg), rtis_under(((JoinExpr *) node)->rarg));
	if (IsA(node, FromExpr))
	{
		Bitmapset  *all = NULL;
		ListCell   *lc;

		foreach(lc, ((FromExpr *) node)->fromlist)
			all = bms_union(all, rtis_under(lfirst(lc)));
		return all;
	}
	return NULL;
}

/*
 * Keys reached from one relation's changed rows: a LEFT JOIN that would null-extend the rows of
 * the other side when that relation has none of them becomes an inner join, or every key on the
 * preserved side would be reported for any change.
 */
static void
inner_where_nullable(Node *node, int driving)
{
	if (IsA(node, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) node;

		if (j->jointype == JOIN_LEFT && bms_is_member(driving, rtis_under(j->rarg)))
			j->jointype = JOIN_INNER;
		inner_where_nullable(j->larg, driving);
		inner_where_nullable(j->rarg, driving);
	}
	else if (IsA(node, FromExpr))
	{
		ListCell   *lc;

		foreach(lc, ((FromExpr *) node)->fromlist)
			inner_where_nullable(lfirst(lc), driving);
	}
}

/*
 * State relations have no primary key, so a column the original query could leave out of GROUP
 * BY (functionally dependent on a grouped primary key) has to be grouped explicitly. Grouping by
 * a column that depends on the groups changes nothing.
 */
static void
group_dependent_columns(Query *q)
{
	List	   *grouped = NIL;
	List	   *vars;
	ListCell   *lc;
	Index		maxref = 0;
	int			grp = group_rti(q);

	if (q->groupClause == NIL)
		return;
	foreach(lc, q->groupClause)
	{
		TargetEntry *tle = get_sortgroupclause_tle(lfirst(lc), q->targetList);

		grouped = lappend(grouped, flatten_group(q, (Node *) tle->expr));
	}
	foreach(lc, q->targetList)
		maxref = Max(maxref, ((TargetEntry *) lfirst(lc))->ressortgroupref);
	/*
	 * Grouped expressions in the target list and HAVING are references to the grouping step
	 * (Postgres 18); a column read inside one is grouped through it. Only the table columns
	 * left over are read without being grouped.
	 */
	vars = list_concat(vars_of((Node *) q->targetList, true), vars_of(q->havingQual, true));
	foreach(lc, vars)
	{
		Var		   *v = (Var *) lfirst(lc);
		TargetEntry *tle;
		SortGroupClause *sgc;
		ListCell   *lg;
		bool		found = false;

		if (grp > 0 && v->varno == grp)
			continue;

		foreach(lg, grouped)
		{
			Node	   *g = strip_relabel(lfirst(lg));

			if (IsA(g, Var) && ((Var *) g)->varno == v->varno && ((Var *) g)->varattno == v->varattno)
				found = true;
		}
		if (found)
			continue;
		tle = makeTargetEntry((Expr *) copyObject(v), list_length(q->targetList) + 1, NULL, true);
		tle->ressortgroupref = ++maxref;
		q->targetList = lappend(q->targetList, tle);
		sgc = makeNode(SortGroupClause);
		sgc->tleSortGroupRef = maxref;
		get_sort_group_operators(v->vartype, false, true, false, &sgc->sortop, &sgc->eqop, NULL, &sgc->hashable);
		q->groupClause = lappend(q->groupClause, sgc);
		grouped = lappend(grouped, v);
	}
}

static Var *
column_var(Oid relid, int rti, const char *name)
{
	AttrNumber	att = get_attnum(relid, name);
	Oid			type;
	int32		typmod;
	Oid			coll;

	if (att == InvalidAttrNumber)
		ereport(ERROR, (errmsg("isomer: relation %s has no column %s", get_rel_name(relid), name)));
	get_atttypetypmodcoll(relid, att, &type, &typmod, &coll);
	return makeVar(rti, att, type, typmod, coll, 0);
}

Datum
isomer_engine_sql(PG_FUNCTION_ARGS)
{
	char	   *sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *mode = text_to_cstring(PG_GETARG_TEXT_PP(1));
	int			fact = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT32(2);
	int			driving = PG_ARGISNULL(3) ? 0 : PG_GETARG_INT32(3);
	Query	   *q = analyze_select(sql);
	List	   *aggs = aggs_of(q);
	Datum	   *rels = NULL;
	bool	   *rels_null = NULL;
	int			nrels = 0;
	Datum	   *tmpls = NULL;
	bool	   *tmpls_null = NULL;
	int			ntmpls = 0;
	Datum	   *fcols = NULL;
	bool	   *fcols_null = NULL;
	int			nfcols = 0;
	int			i;

	if (!PG_ARGISNULL(4))
		deconstruct_array_builtin(PG_GETARG_ARRAYTYPE_P(4), OIDOID, &rels, &rels_null, &nrels);
	if (!PG_ARGISNULL(5))
		deconstruct_array_builtin(PG_GETARG_ARRAYTYPE_P(5), TEXTOID, &tmpls, &tmpls_null, &ntmpls);
	if (!PG_ARGISNULL(6))
		deconstruct_array_builtin(PG_GETARG_ARRAYTYPE_P(6), TEXTOID, &fcols, &fcols_null, &nfcols);

	/* Swap relations for state or staging relations whose columns line up with them. */
	for (i = 0; i < nrels && i < list_length(q->rtable); i++)
	{
		RangeTblEntry *rte = rt_fetch(i + 1, q->rtable);
		Oid			relid = rels_null[i] ? InvalidOid : DatumGetObjectId(rels[i]);

		if (!OidIsValid(relid))
			continue;
		if (rte->rtekind != RTE_RELATION)
			elog(ERROR, "isomer: range table entry %d is not a relation", i + 1);
		rte->relid = relid;
		rte->relkind = get_rel_relkind(relid);
	}

	if (strcmp(mode, "derive") == 0 || strcmp(mode, "derive_keyed") == 0 || strcmp(mode, "keys") == 0)
	{
		ReplaceCtx	ctx;
		ListCell   *lc;
		bool		keys = strcmp(mode, "keys") == 0;
		bool		keyed = strcmp(mode, "derive_keyed") == 0;

		group_dependent_columns(q);
		ctx.aggs = aggs;
		ctx.replacements = NIL;
		i = 0;
		foreach(lc, aggs)
		{
			Aggref	   *a = (Aggref *) lfirst(lc);

			if (keys)
				ctx.replacements = lappend(ctx.replacements,
										   makeNullConst(a->aggtype, -1, a->aggcollid));
			else
			{
				Oid			factrel;

				if (i >= ntmpls)
					elog(ERROR, "isomer: no replacement given for aggregate %d", i);
				factrel = rt_fetch(fact, q->rtable)->relid;
				/*
				 * No template: the aggregate is kept as written. Its state is a copy of every fact
				 * row it reads (the fact's key is among the kept columns), so it runs over the
				 * same rows as over the tables.
				 */
				if (tmpls_null[i])
					ctx.replacements = lappend(ctx.replacements, copyObject(a));
				else
					ctx.replacements = lappend(ctx.replacements,
											   analyze_template(TextDatumGetCString(tmpls[i]), factrel, fact));
			}
			i++;
		}
		q->targetList = (List *) replace_aggs_mutator((Node *) q->targetList, &ctx);
		q->havingQual = keys ? NULL : replace_aggs_mutator(q->havingQual, &ctx);
		q->sortClause = NIL;
		if (keys && driving > 0)
			inner_where_nullable((Node *) q->jointree, driving);
		/*
		 * The fact's own filters were applied while folding it into state, so they are dropped
		 * wherever the fact is read as state: always in 'derive', and in 'keys' unless its raw
		 * changed rows are what drives the query.
		 */
		if (fact > 0 && (!keys || driving != fact))
		{
			List	   *taken = NIL;

			split_fact_quals_from((Node *) q->jointree, fact, &taken);
		}

		/*
		 * The key is grouped, so filtering the rows by it before grouping gives the same groups
		 * for those keys: WHERE <key> = ANY ($1::<key type>[]).
		 */
		if (keyed)
		{
			char	   *key;
			Node	   *e = NULL;
			Param	   *prm = makeNode(Param);
			Node	   *arr;
			Expr	   *cond;

			if (nfcols != 1 || fcols_null[0])
				elog(ERROR, "isomer: mode 'derive_keyed' needs the key column");
			key = TextDatumGetCString(fcols[0]);
			foreach(lc, q->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc);

				if (!tle->resjunk && tle->resname && strcmp(tle->resname, key) == 0)
					e = flatten_group(q, (Node *) tle->expr);
			}
			if (e == NULL)
				elog(ERROR, "isomer: the query has no column %s", key);
			prm->paramkind = PARAM_EXTERN;
			prm->paramid = 1;
			prm->paramtype = TEXTARRAYOID;
			prm->paramtypmod = -1;
			prm->paramcollid = DEFAULT_COLLATION_OID;
			prm->location = -1;
			arr = coerce_to_target_type(NULL, (Node *) prm, TEXTARRAYOID, get_array_type(exprType(e)), -1,
										COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
			if (arr == NULL)
				elog(ERROR, "isomer: the key %s can't be compared with text values", key);
			cond = make_scalar_array_op(make_parsestate(NULL), list_make1(makeString("=")), true,
										copyObject(e), arr, -1);
			AddQual(q, (Node *) cond);
		}
	}
	else if (strcmp(mode, "fact") == 0)
	{
		/*
		 * A new single-relation query: SELECT <columns>, <args and filters>, <weight> FROM fact
		 * WHERE <the fact's own conjuncts>. Everything in it must read only the fact relation.
		 */
		Query	   *fq = makeNode(Query);
		RangeTblEntry *rte;
		List	   *quals = NIL;
		List	   *tlist = NIL;
		ListCell   *lc;
		Oid			factrel;
		AttrNumber	resno = 1;
		Node	   *where;

		if (fact <= 0)
			elog(ERROR, "isomer: mode 'fact' needs the fact relation");
		rte = copyObject(rt_fetch(fact, q->rtable));
		factrel = rte->relid;
		split_fact_quals_from((Node *) q->jointree, fact, &quals);

		for (i = 0; i < nfcols; i++)
		{
			char	   *name = TextDatumGetCString(fcols[i]);

			tlist = lappend(tlist, makeTargetEntry((Expr *) column_var(factrel, fact, name), resno++,
												   pstrdup(name), false));
		}
		i = 0;
		foreach(lc, aggs)
		{
			Aggref	   *a = (Aggref *) lfirst(lc);
			Expr	   *arg = a->args != NIL ? ((TargetEntry *) linitial(a->args))->expr
				: (Expr *) makeBoolConst(true, false);
			Expr	   *filter = a->aggfilter ? a->aggfilter : (Expr *) makeBoolConst(true, false);
			Node	   *bare = strip_relabel((Node *) arg);

			/* An aggregate kept as written (a NULL template) needs no partial aggregate. */
			if (i < ntmpls && tmpls_null[i])
			{
				i++;
				continue;
			}

			/*
			 * count(t.*) counts the rows of t: a whole-row value is never null for a row that
			 * exists, but IS NULL on a row tests every field, so count it as a constant.
			 */
			if (IsA(bare, Var) && ((Var *) bare)->varattno == InvalidAttrNumber)
				arg = (Expr *) makeBoolConst(true, false);
			tlist = lappend(tlist, makeTargetEntry(copyObject(arg), resno++, psprintf("__a%d", i), false));
			tlist = lappend(tlist, makeTargetEntry(copyObject(filter), resno++, psprintf("__f%d", i), false));
			i++;
		}
		if (get_attnum(factrel, "__w") != InvalidAttrNumber)
			tlist = lappend(tlist, makeTargetEntry((Expr *) column_var(factrel, fact, "__w"), resno++,
												   pstrdup("__w"), false));
		else
			tlist = lappend(tlist, makeTargetEntry((Expr *) makeConst(INT4OID, -1, InvalidOid, 4,
																	  Int32GetDatum(1), false, true),
												   resno++, pstrdup("__w"), false));
		where = quals == NIL ? NULL : (Node *) make_ands_explicit(quals);
		{
			Bitmapset  *used = bms_union(varnos_of((Node *) tlist), varnos_of(where));

			if (!bms_is_empty(used) && !(bms_num_members(used) == 1 && bms_is_member(fact, used)))
				ereport(ERROR, (errmsg("isomer: an aggregate reads more than the fact relation")));
		}
		ChangeVarNodes((Node *) tlist, fact, 1, 0);
		ChangeVarNodes(where, fact, 1, 0);

		fq->commandType = CMD_SELECT;
		fq->canSetTag = true;
		fq->rtable = list_make1(rte);
		rte->perminfoindex = 0;
		fq->jointree = makeFromExpr(list_make1(makeNode(RangeTblRef)), where);
		((RangeTblRef *) linitial(fq->jointree->fromlist))->rtindex = 1;
		fq->targetList = tlist;
		q = fq;
	}
	else
		elog(ERROR, "isomer: unknown mode %s", mode);

	PG_RETURN_TEXT_P(cstring_to_text(pg_get_querydef(q, false)));
}
