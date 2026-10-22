-- IsomerDB 0.22.0: incremental maintenance of correlated subqueries (PRD FR-P6).
--
--   * A projection whose query has subqueries (SELECT jsonb_agg(...) FROM orders o WHERE
--     o.customer_id = c.id, EXISTS (...), x IN (SELECT ...), ARRAY(SELECT ...)) can be maintained
--     incrementally. The query and each subquery in it are blocks, and each block keeps its own
--     state the way a whole projection does: a block that aggregates keeps partial aggregates,
--     grouped by the columns its conditions on the outer row read (its correlation), so a count
--     or sum of one customer's orders is one partial row however many orders there are; a block
--     that doesn't aggregate keeps copies of the columns it reads. Subqueries nest (an order's
--     lines inside each order inside the customer's document).
--   * A key is derived by the query as written, every block reading its state: a correlated
--     subquery runs once per outer row over that row's partial aggregates.
--   * The keys a change reaches: the changed rows of a subquery's table, through each subquery
--     on the way up turned into EXISTS (a row correlated with the outer one) in its parent. No
--     key mappings are needed.
--   * Where a subquery may be: in the output or WHERE of a block that doesn't aggregate, and
--     inside the arguments of an aggregate kept as written (jsonb_agg(... (SELECT ...) ...)). Not
--     in ON, GROUP BY, HAVING or LIMIT, nor outside the aggregates of a block that aggregates.
--     A subquery that doesn't aggregate may use ORDER BY, LIMIT and DISTINCT.
--   * Document and cache projections, with their state on the coordinator.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.22.0'" to load this file. \quit

-- The query and each subquery in it, as blocks (see isomer_engine.c).
CREATE FUNCTION isomer._engine_blocks(query text)
RETURNS jsonb LANGUAGE C STRICT AS 'isomer_router', 'isomer_engine_blocks';

CREATE FUNCTION isomer._engine_sql_blocks(query text, mode text, blocks jsonb, block integer, driving integer)
RETURNS text LANGUAGE C AS 'isomer_router', 'isomer_engine_sql_blocks';

-- ── Shape of one block ──────────────────────────────────────────────────────

-- What the engine needs to know about one block, from its analysis (see 0.21.0 for the rules).
-- p_role: 'query' (a query without subqueries), 'top' (the query, with subqueries) or 'sub' (a
-- subquery). New for blocks:
--   * a block that doesn't aggregate keeps copies of the columns it reads, from every table it
--     reads (they are all "dims"); a subquery that doesn't aggregate may use ORDER BY, LIMIT and
--     DISTINCT, since its derivation runs it as written over those copies;
--   * an aggregate that reads the outer row, or has a subquery in it, is kept as written;
--   * a condition that reads the outer row, or has a subquery in it, is not a filter of the
--     aggregated table: the columns it reads are kept, and it is applied when deriving.
CREATE FUNCTION isomer._engine_block_shape(a jsonb, p_key text, p_kind text, p_role text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_rte      jsonb;
    v_x        jsonb;
    v_fact     integer;
    v_rtis     integer[];
    v_aggrtis  integer[];
    v_keeprtis integer[];
    v_reason   text;
    v_node     integer;
    v_cols     text[];
    v_nullable boolean := false;
    v_pkeys    text[];
    v_aggs     jsonb := '[]';
    v_cls      text;
    v_stype    text;
    v_rtes     jsonb := '[]';
    v_capture  jsonb := '{}';
    v_dims     jsonb := '{}';
    v_relid    oid;
    v_key      jsonb;
    v_keep     jsonb := '[]';
    v_fpk      text[];
    v_extra    text[] := '{}';
    v_simple   boolean;
    v_noagg    boolean := jsonb_array_length(a -> 'aggs') = 0;
    v_numeric  text[] := ARRAY['smallint', 'integer', 'bigint', 'numeric', 'real', 'double precision'];
BEGIN
    IF p_kind NOT IN ('document', 'cache', 'analytics') THEN
        RETURN jsonb_build_object('ok', false, 'reason', format('%s projections are not maintained incrementally', p_kind));
    END IF;

    SELECT string_agg(k, ', ') INTO v_reason
    FROM jsonb_each(a -> 'flags') f(k, v)
    WHERE v = 'true' AND k NOT IN ('having', 'order', 'sublinks')
      AND NOT (p_role = 'sub' AND v_noagg AND k IN ('distinct', 'limit'));
    IF v_reason IS NOT NULL THEN
        RETURN jsonb_build_object('ok', false, 'reason', format('it uses %s', replace(replace(replace(replace(v_reason,
            'setops', 'UNION/INTERSECT/EXCEPT'), 'srf', 'set-returning functions'), 'cte', 'WITH'), 'window', 'window functions')));
    END IF;
    IF (a ->> 'other_rtes')::boolean THEN
        RETURN jsonb_build_object('ok', false, 'reason', 'it reads something other than tables (a subquery, function or VALUES in FROM)');
    END IF;
    IF (a ->> 'join_vars')::boolean OR EXISTS (SELECT 1 FROM jsonb_array_elements(a -> 'joins') j WHERE (j ->> 'using')::boolean) THEN
        RETURN jsonb_build_object('ok', false, 'reason', 'it joins with USING, NATURAL or a join alias; write the join with ON');
    END IF;
    SELECT string_agg(j ->> 'type', ', ') INTO v_reason
    FROM jsonb_array_elements(a -> 'joins') j WHERE j ->> 'type' NOT IN ('inner', 'left');
    IF v_reason IS NOT NULL THEN
        RETURN jsonb_build_object('ok', false, 'reason', format('it uses a %s join (inner and left joins are supported)', v_reason));
    END IF;
    FOR v_rte IN SELECT * FROM jsonb_array_elements(a -> 'rtes') LOOP
        IF v_rte ->> 'relkind' NOT IN ('r', 'p') THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('it reads %s, which is not a table', (v_rte ->> 'relid')::oid::regclass));
        END IF;
    END LOOP;
    v_rtis := ARRAY(SELECT (e ->> 'rti')::int FROM jsonb_array_elements(a -> 'rtes') e);

    -- Where its subqueries are.
    IF p_role <> 'query' THEN
        SELECT string_agg(k, ', ') INTO v_reason FROM jsonb_each(a -> 'sublinks') s(k, v)
        WHERE (v::text)::int > 0 AND k IN ('join', 'having', 'group', 'other');
        IF v_reason IS NOT NULL THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('it has a subquery in %s (subqueries are maintained in the output, in WHERE, and inside aggregates kept as written)',
                replace(replace(replace(v_reason, 'join', 'a JOIN''s ON'), 'group', 'GROUP BY'), 'other', 'LIMIT or OFFSET')));
        END IF;
        IF NOT v_noagg AND ((a -> 'sublinks' ->> 'target')::int > 0 OR (a -> 'sublinks' ->> 'where')::int > 0) THEN
            RETURN jsonb_build_object('ok', false, 'reason', 'it aggregates and has a subquery outside its aggregates (in its output or WHERE): only an aggregate kept as written can hold one');
        END IF;
    END IF;

    -- The key: one of the outputs, and grouped if the query groups.
    IF p_key IS NOT NULL THEN
        SELECT t -> 'col' INTO v_key FROM jsonb_array_elements(a -> 'target') t WHERE t ->> 'name' = p_key;
        IF jsonb_array_length(a -> 'group') > 0
           AND NOT coalesce((SELECT (t ->> 'grouped')::boolean FROM jsonb_array_elements(a -> 'target') t WHERE t ->> 'name' = p_key), false) THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('the key %s is not one of the grouping columns', p_key));
        END IF;
    END IF;

    IF v_noagg THEN
        IF p_role = 'query' THEN
            RETURN jsonb_build_object('ok', false, 'reason', 'it has no aggregate: re-deriving a key reads only that key''s rows, which is already the cheapest way');
        END IF;
    ELSE
        -- Aggregates: decomposed into sums, counts or a multiset where they can be, kept as
        -- written otherwise (and always when they read the outer row or hold a subquery).
        FOR v_x IN SELECT * FROM jsonb_array_elements(a -> 'aggs') LOOP
            v_simple := coalesce(v_x ->> 'schema' = 'pg_catalog' AND v_x ->> 'kind' = 'n'
                AND v_x ->> 'fn' IN ('count', 'sum', 'avg', 'min', 'max', 'bool_and', 'bool_or', 'every')
                AND NOT (v_x ->> 'ordered')::boolean AND (v_x ->> 'nargs')::int <= 1
                AND NOT coalesce((v_x ->> 'outer')::boolean, false) AND NOT coalesce((v_x ->> 'sublink')::boolean, false)
                AND NOT (v_x ->> 'fn' <> 'count' AND (SELECT t.typtype = 'c' OR t.oid = 'record'::regtype FROM pg_type t WHERE t.oid = (v_x ->> 'argtype')::regtype))
                AND NOT (v_x ->> 'fn' IN ('sum', 'avg') AND NOT (v_x ->> 'argtype' = ANY (v_numeric)))
                AND NOT (v_x ->> 'fn' IN ('min', 'max') AND NOT isomer._has_equality(v_x ->> 'argtype')), false);
            v_cls := CASE
                WHEN NOT v_simple THEN 'keep'
                WHEN (v_x ->> 'distinct')::boolean AND v_x ->> 'fn' IN ('count', 'sum', 'avg') THEN
                    CASE WHEN jsonb_typeof(v_x -> 'argcol') = 'object' AND jsonb_array_length(v_x -> 'rtis') = 1
                              AND (v_x -> 'rtis' ->> 0)::int = (v_x -> 'argcol' ->> 'rti')::int
                              AND isomer._has_equality(v_x ->> 'argtype')
                         THEN 'distinct' ELSE 'keep' END
                WHEN v_x ->> 'fn' = 'count' AND (v_x ->> 'star')::boolean AND NOT (v_x ->> 'filter')::boolean THEN 'count_star'
                WHEN v_x ->> 'fn' = 'count' THEN 'count'
                WHEN v_x ->> 'fn' IN ('bool_and', 'every') THEN 'bool_and'
                ELSE v_x ->> 'fn' END;
            v_stype := CASE
                WHEN v_cls = 'sum' AND v_x ->> 'argtype' IN ('smallint', 'integer') THEN 'bigint'
                WHEN v_cls = 'sum' AND v_x ->> 'argtype' = 'bigint' THEN 'numeric'
                WHEN v_cls = 'sum' THEN v_x ->> 'argtype'
                WHEN v_cls = 'avg' AND v_x ->> 'argtype' IN ('real', 'double precision') THEN 'double precision'
                WHEN v_cls = 'avg' THEN 'numeric'
                WHEN v_cls IN ('min', 'max') THEN v_x ->> 'argtype' END;
            v_aggs := v_aggs || jsonb_build_object('i', (v_x ->> 'i')::int, 'cls', v_cls, 'fn', v_x ->> 'fn', 'argtype', v_x ->> 'argtype',
                                                   'restype', v_x ->> 'restype', 'stype', v_stype,
                                                   'col', CASE WHEN v_cls = 'distinct' THEN v_x -> 'argcol' ->> 'col' END,
                                                   'rtis', v_x -> 'rtis', 'cols', v_x -> 'cols');
            IF v_cls IN ('keep', 'distinct') THEN
                v_keeprtis := v_keeprtis || ARRAY(SELECT jsonb_array_elements_text(v_x -> 'rtis')::int);
            ELSE
                v_aggrtis := v_aggrtis || ARRAY(SELECT jsonb_array_elements_text(v_x -> 'rtis')::int);
            END IF;
        END LOOP;
        v_aggrtis := ARRAY(SELECT DISTINCT unnest(v_aggrtis));
        v_keeprtis := ARRAY(SELECT DISTINCT unnest(v_keeprtis) ORDER BY 1);
        IF cardinality(v_aggrtis) > 1 THEN
            RETURN jsonb_build_object('ok', false, 'reason', 'its aggregates read more than one table');
        END IF;
        IF (a ->> 'group_volatile')::boolean THEN
            RETURN jsonb_build_object('ok', false, 'reason', 'it groups by a volatile expression');
        END IF;

        -- The fact: the table the decomposed aggregates read; or one of those the kept
        -- aggregates read; or (count(*) only) the one the joins lead away from.
        IF cardinality(v_aggrtis) = 1 THEN
            v_fact := v_aggrtis[1];
            v_reason := isomer._engine_tree(a, v_fact);
        ELSE
            FOREACH v_node IN ARRAY v_keeprtis || ARRAY(SELECT r FROM unnest(v_rtis) r WHERE NOT (r = ANY (v_keeprtis))) LOOP
                v_reason := isomer._engine_tree(a, v_node);
                IF v_reason IS NULL THEN
                    v_fact := v_node;
                    EXIT;
                END IF;
            END LOOP;
            IF v_fact IS NULL AND cardinality(v_rtis) = 1 THEN
                v_fact := v_rtis[1];
                v_reason := NULL;
            END IF;
        END IF;
        IF v_reason IS NOT NULL THEN
            RETURN jsonb_build_object('ok', false, 'reason', v_reason);
        END IF;
        IF v_fact IS NULL THEN
            RETURN jsonb_build_object('ok', false, 'reason', 'its aggregates read no table');
        END IF;

        -- Outer joins: the fact may be the optional side only on its own, with its conditions in ON.
        FOR v_x IN SELECT * FROM jsonb_array_elements(a -> 'joins') WHERE value ->> 'type' = 'left' LOOP
            IF v_x -> 'right' @> to_jsonb(v_fact) THEN
                IF jsonb_array_length(v_x -> 'right') > 1 THEN
                    RETURN jsonb_build_object('ok', false, 'reason', 'the aggregated table is on the optional side of a LEFT JOIN together with other tables');
                END IF;
                v_nullable := true;
            END IF;
        END LOOP;
        IF v_nullable AND EXISTS (SELECT 1 FROM jsonb_array_elements(a -> 'conjuncts') c
                                  WHERE (c ->> 'join')::int = -1 AND c -> 'rtis' = jsonb_build_array(v_fact)) THEN
            RETURN jsonb_build_object('ok', false, 'reason', 'a WHERE condition reads only the aggregated table, which a LEFT JOIN makes optional; move it into the ON clause');
        END IF;

        v_relid := (SELECT (e ->> 'relid')::oid FROM jsonb_array_elements(a -> 'rtes') e WHERE (e ->> 'rti')::int = v_fact);
        SELECT jsonb_agg(CASE WHEN g ->> 'cls' = 'distinct' AND (g -> 'rtis' ->> 0)::int <> v_fact
                              THEN g || '{"cls": "keep", "col": null}' ELSE g END ORDER BY (g ->> 'i')::int)
        INTO v_aggs FROM jsonb_array_elements(v_aggs) g;
        SELECT coalesce(jsonb_agg(g), '[]') INTO v_keep FROM jsonb_array_elements(v_aggs) g WHERE g ->> 'cls' = 'keep';
        -- Kept as written: one partial row per fact row, holding everything the aggregates read.
        IF jsonb_array_length(v_keep) > 0 THEN
            v_fpk := isomer._primary_key(v_relid);
            IF v_fpk IS NULL THEN
                RETURN jsonb_build_object('ok', false, 'reason', format('it uses %s, which is kept as written over a copy of %s''s rows, and %s has no primary key to tell its rows apart',
                    (SELECT string_agg(DISTINCT k ->> 'fn', ', ') FROM jsonb_array_elements(v_keep) k), v_relid::regclass, v_relid::regclass));
            END IF;
            v_extra := v_fpk || ARRAY(SELECT c ->> 'col' FROM jsonb_array_elements(v_keep) k, jsonb_array_elements(k -> 'cols') c
                                      WHERE jsonb_typeof(c) = 'object' AND (c ->> 'rti')::int = v_fact);
        END IF;
        -- DISTINCT over a column of the fact: its values are kept.
        v_extra := v_extra || ARRAY(SELECT g ->> 'col' FROM jsonb_array_elements(v_aggs) g WHERE g ->> 'cls' = 'distinct');

        -- The fact's columns kept in its partial aggregates: everything read outside aggregates
        -- and outside its own filters (grouping, output, join conditions, conditions shared with
        -- others or with the outer row), and what the aggregates above need.
        v_pkeys := ARRAY(
            SELECT DISTINCT col FROM (
                SELECT c ->> 'col' AS col FROM (
                    SELECT value AS c FROM jsonb_array_elements(a -> 'nonagg')
                    UNION ALL
                    SELECT col FROM jsonb_array_elements(a -> 'conjuncts') cj, jsonb_array_elements(cj -> 'cols') col
                    WHERE cj -> 'rtis' <> jsonb_build_array(v_fact)
                       OR coalesce((cj ->> 'outer')::boolean, false) OR coalesce((cj ->> 'sublink')::boolean, false)) s
                WHERE jsonb_typeof(c) = 'object' AND (c ->> 'rti')::int = v_fact
                UNION ALL
                SELECT unnest(v_extra)) k
            ORDER BY 1);
    END IF;

    -- Relations, the columns captured from each, and the columns kept for each joined table
    -- (every table, in a block that doesn't aggregate).
    FOR v_rte IN SELECT * FROM jsonb_array_elements(a -> 'rtes') LOOP
        v_relid := (v_rte ->> 'relid')::oid;
        v_cols := ARRAY(SELECT DISTINCT c ->> 'col' FROM jsonb_array_elements(a -> 'columns') c
                        WHERE (c ->> 'rti')::int = (v_rte ->> 'rti')::int ORDER BY 1);
        v_rtes := v_rtes || jsonb_build_object('rti', (v_rte ->> 'rti')::int, 'relid', v_relid,
                                               'table', isomer._qualified(v_relid::regclass),
                                               'role', CASE WHEN (v_rte ->> 'rti')::int IS NOT DISTINCT FROM v_fact THEN 'fact' ELSE 'dim' END);
        IF (v_rte ->> 'rti')::int IS NOT DISTINCT FROM v_fact THEN
            v_cols := ARRAY(SELECT DISTINCT e FROM unnest(v_cols || v_pkeys) e ORDER BY 1);
        END IF;
        v_capture := jsonb_set(v_capture, ARRAY[v_relid::text],
            to_jsonb(ARRAY(SELECT DISTINCT e FROM unnest(ARRAY(SELECT jsonb_array_elements_text(coalesce(v_capture -> v_relid::text, '[]')))
                                                  || v_cols) e ORDER BY 1)));
        IF (v_rte ->> 'rti')::int IS DISTINCT FROM v_fact THEN
            -- A joined table's state is keyed like the table, so its primary key is kept (and
            -- captured: a row whose key changes is a different row).
            v_cols := ARRAY(SELECT DISTINCT e FROM unnest(ARRAY(SELECT jsonb_array_elements_text(coalesce(v_dims -> v_relid::text, '[]')))
                                                         || v_cols || coalesce(isomer._primary_key(v_relid), '{}')) e ORDER BY 1);
            v_dims := jsonb_set(v_dims, ARRAY[v_relid::text], to_jsonb(v_cols));
            v_capture := jsonb_set(v_capture, ARRAY[v_relid::text],
                to_jsonb(ARRAY(SELECT DISTINCT e FROM unnest(ARRAY(SELECT jsonb_array_elements_text(v_capture -> v_relid::text)) || v_cols) e ORDER BY 1)));
        END IF;
    END LOOP;

    -- Grouping and keeping state need equality on every column kept.
    FOR v_x IN
        SELECT jsonb_build_object('rel', fr.relid, 'col', c) FROM
            (SELECT (e ->> 'relid')::oid AS relid FROM jsonb_array_elements(v_rtes) e WHERE (e ->> 'rti')::int = v_fact) fr,
            unnest(v_pkeys) c
        UNION ALL
        SELECT jsonb_build_object('rel', d.key::oid, 'col', c) FROM jsonb_each(v_dims) d, jsonb_array_elements_text(d.value) c
    LOOP
        IF NOT isomer._has_equality(format_type(
               (SELECT atttypid FROM pg_attribute WHERE attrelid = (v_x ->> 'rel')::oid AND attname = v_x ->> 'col'), NULL)) THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('%s.%s has no equality operator', (v_x ->> 'rel')::oid::regclass, v_x ->> 'col'));
        END IF;
    END LOOP;

    RETURN jsonb_build_object(
        'ok', true, 'fact', v_fact,
        'fact_relid', (SELECT (e ->> 'relid')::oid FROM jsonb_array_elements(v_rtes) e WHERE (e ->> 'rti')::int = v_fact),
        'fact_pk', to_jsonb(v_fpk),
        'nullable_fact', v_nullable, 'pkeys', coalesce(to_jsonb(v_pkeys), '[]'), 'aggs', v_aggs, 'rtes', v_rtes,
        'capture', v_capture, 'dims', v_dims, 'group', a -> 'group', 'key', v_key,
        'edges', (SELECT coalesce(jsonb_agg(c -> 'equi'), '[]') FROM jsonb_array_elements(a -> 'conjuncts') c
                  WHERE jsonb_typeof(c -> 'equi') = 'array'),
        'joins', a -> 'joins',
        'max_rti', (SELECT max(n) FROM (SELECT (e ->> 'rti')::int AS n FROM jsonb_array_elements(a -> 'rtes') e
                                      UNION ALL SELECT jsonb_array_length(a -> 'rtes')) s));
END $$;

-- What the engine needs to know about a query, or why it cannot maintain it. A query without
-- subqueries is one block, as before. One with subqueries: every block's shape, under "blocks",
-- and the query's own (its relations, joins and key) at the top, with the columns captured and
-- the joined tables' kept columns merged over every block (a table read by several blocks is
-- captured once, and its copy holds what each of them reads).
CREATE OR REPLACE FUNCTION isomer._engine_shape(p_query text, p_key text, p_kind text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a        jsonb;
    v_blocks jsonb;
    b        jsonb;
    s        jsonb;
    v_shapes jsonb := '[]';
    v_cap    jsonb := '{}';
    v_dims   jsonb := '{}';
    e        record;
BEGIN
    BEGIN
        a := isomer._engine_analyze(p_query);
    EXCEPTION WHEN OTHERS THEN
        RETURN jsonb_build_object('ok', false, 'reason', 'the query could not be analysed: ' || SQLERRM);
    END;
    IF NOT (a -> 'flags' ->> 'sublinks')::boolean THEN
        RETURN isomer._engine_block_shape(a, p_key, p_kind, 'query');
    END IF;

    IF p_kind NOT IN ('document', 'cache') THEN
        RETURN jsonb_build_object('ok', false, 'reason', format('it has subqueries, which are maintained incrementally in document and cache projections, not %s ones', p_kind));
    END IF;
    v_blocks := isomer._engine_blocks(p_query);
    FOR b IN SELECT * FROM jsonb_array_elements(v_blocks) LOOP
        IF b ->> 'type' IN ('multiexpr', 'cte') THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('it has a %s subquery', b ->> 'type'));
        END IF;
        s := isomer._engine_block_shape(b -> 'analysis', CASE WHEN (b ->> 'block')::int = 0 THEN p_key END, p_kind,
                                        CASE WHEN (b ->> 'block')::int = 0 THEN 'top' ELSE 'sub' END);
        IF NOT (s ->> 'ok')::boolean THEN
            RETURN jsonb_build_object('ok', false, 'reason',
                CASE WHEN (b ->> 'block')::int = 0 THEN s ->> 'reason'
                     ELSE format('a subquery (%s): %s', (SELECT string_agg(DISTINCT (r ->> 'relid')::oid::regclass::text, ', ')
                                                        FROM jsonb_array_elements(b -> 'analysis' -> 'rtes') r), s ->> 'reason') END);
        END IF;
        IF (b ->> 'block')::int > 0 AND b ->> 'place' = 'agg' AND NOT EXISTS (
               SELECT 1 FROM jsonb_array_elements(v_shapes -> (b ->> 'parent')::int -> 'aggs') g
               WHERE (g ->> 'i')::int = (b ->> 'agg')::int AND g ->> 'cls' = 'keep') THEN
            RETURN jsonb_build_object('ok', false, 'reason', 'a subquery inside an aggregate that is not kept as written');
        END IF;
        v_shapes := v_shapes || jsonb_build_array(s || jsonb_build_object('block', (b ->> 'block')::int, 'parent', b -> 'parent',
                                                                           'type', b ->> 'type', 'place', b ->> 'place'));
        FOR e IN SELECT * FROM jsonb_each(s -> 'capture') LOOP
            v_cap := jsonb_set(v_cap, ARRAY[e.key], to_jsonb(ARRAY(SELECT DISTINCT c FROM (
                SELECT jsonb_array_elements_text(coalesce(v_cap -> e.key, '[]')) c UNION SELECT jsonb_array_elements_text(e.value)) x ORDER BY 1)));
        END LOOP;
        FOR e IN SELECT * FROM jsonb_each(s -> 'dims') LOOP
            v_dims := jsonb_set(v_dims, ARRAY[e.key], to_jsonb(ARRAY(SELECT DISTINCT c FROM (
                SELECT jsonb_array_elements_text(coalesce(v_dims -> e.key, '[]')) c UNION SELECT jsonb_array_elements_text(e.value)) x ORDER BY 1)));
        END LOOP;
    END LOOP;
    -- A table kept as a copy for one block and aggregated in another is captured with both's columns.
    FOR e IN SELECT * FROM jsonb_each(v_dims) LOOP
        v_cap := jsonb_set(v_cap, ARRAY[e.key], to_jsonb(ARRAY(SELECT DISTINCT c FROM (
            SELECT jsonb_array_elements_text(v_cap -> e.key) c UNION SELECT jsonb_array_elements_text(e.value)) x ORDER BY 1)));
    END LOOP;
    RETURN (v_shapes -> 0) || jsonb_build_object('nested', true, 'blocks', v_shapes, 'capture', v_cap, 'dims', v_dims);
END $$;

-- ── Compiling a query with subqueries ───────────────────────────────────────

-- The partial aggregates of one block's fact (see 0.17.0 and 0.21.0): the table, the view the
-- derivation reads (lined up with the fact table column by column), the multisets behind min and
-- max, and each aggregate's template over the partial aggregates (NULL: kept as written).
CREATE FUNCTION isomer._engine_fact_state(p_name text, p_factrel oid, p_pkeys text[], p_aggs jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_p      text := format('isomer_state.%I', p_name || 'p');
    v_pv     text := format('isomer_state.%I', p_name || 'pv');
    v_defs   text := (SELECT string_agg(format('%I %s', c, isomer._coltype(p_factrel, c)), ', ') FROM unnest(p_pkeys) c);
    v_extra  text := '__n';
    v_tmpls  text[] := '{}';
    v_minmax jsonb := '[]';
    x        jsonb;
    v_i      integer;
BEGIN
    FOR x IN SELECT * FROM jsonb_array_elements(p_aggs) LOOP
        v_i := (x ->> 'i')::int;
        IF x ->> 'cls' = 'keep' THEN
            v_tmpls := v_tmpls || NULL::text;
            CONTINUE;
        END IF;
        v_defs := concat_ws(', ', v_defs, format('__c%s bigint NOT NULL DEFAULT 0', v_i));
        v_extra := v_extra || format(', __c%s', v_i);
        IF x ->> 'cls' IN ('sum', 'avg') THEN
            v_defs := v_defs || format(', __s%s %s NOT NULL DEFAULT 0', v_i, x ->> 'stype');
            v_extra := v_extra || format(', __s%s', v_i);
        ELSIF x ->> 'cls' IN ('min', 'max') THEN
            v_defs := v_defs || format(', __m%s %s', v_i, x ->> 'stype');
            v_extra := v_extra || format(', __m%s', v_i);
            v_minmax := v_minmax || jsonb_build_object('i', v_i, 'cls', x ->> 'cls', 'table', format('isomer_state.%I', p_name || 'm' || v_i));
            EXECUTE format('CREATE TABLE isomer_state.%I (__h text NOT NULL, value %s NOT NULL, mult bigint NOT NULL, PRIMARY KEY (__h, value))',
                           p_name || 'm' || v_i, x ->> 'stype');
        ELSIF x ->> 'cls' IN ('bool_and', 'bool_or') THEN
            v_defs := v_defs || format(', __t%s bigint NOT NULL DEFAULT 0', v_i);
            v_extra := v_extra || format(', __t%s', v_i);
        END IF;
        v_tmpls := v_tmpls || CASE x ->> 'cls'
            WHEN 'count_star' THEN 'coalesce(sum(coalesce(__n, 1)), 0)::bigint'
            WHEN 'count' THEN format('coalesce(sum(__c%s), 0)::bigint', v_i)
            WHEN 'sum' THEN format('(CASE WHEN coalesce(sum(__c%1$s), 0) = 0 THEN NULL ELSE sum(__s%1$s) END)::%2$s', v_i, x ->> 'restype')
            WHEN 'avg' THEN format('(CASE WHEN coalesce(sum(__c%1$s), 0) = 0 THEN NULL ELSE sum(__s%1$s)::%2$s / sum(__c%1$s)::%2$s END)::%2$s', v_i, x ->> 'restype')
            WHEN 'min' THEN format('min(__m%s)::%s', v_i, x ->> 'restype')
            WHEN 'max' THEN format('max(__m%s)::%s', v_i, x ->> 'restype')
            WHEN 'bool_and' THEN format('(CASE WHEN coalesce(sum(__c%1$s), 0) = 0 THEN NULL ELSE coalesce(sum(__t%1$s), 0) = sum(__c%1$s) END)', v_i)
            WHEN 'bool_or' THEN format('(CASE WHEN coalesce(sum(__c%1$s), 0) = 0 THEN NULL ELSE coalesce(sum(__t%1$s), 0) > 0 END)', v_i)
            WHEN 'distinct' THEN format('(%1$s(DISTINCT %2$I) FILTER (WHERE __c%3$s > 0))::%4$s', x ->> 'fn', x ->> 'col', v_i, x ->> 'restype') END;
    END LOOP;
    EXECUTE format('CREATE TABLE %s (%s)', v_p, concat_ws(', ', '__h text PRIMARY KEY', v_defs, '__n bigint NOT NULL'));
    FOR v_i IN 1 .. coalesce(cardinality(p_pkeys), 0) LOOP
        EXECUTE format('CREATE INDEX ON %s (%I)', v_p, p_pkeys[v_i]);
    END LOOP;
    EXECUTE format('CREATE VIEW %s AS SELECT %s, %s FROM %s p WHERE p.__n > 0',
                   v_pv, isomer._aligned(p_factrel, p_pkeys, 'p'),
                   (SELECT string_agg('p.' || e, ', ') FROM unnest(string_to_array(v_extra, ', ')) e), v_p);
    RETURN jsonb_build_object('p_table', v_p, 'p_view', v_pv, 'minmax', v_minmax, 'templates', to_jsonb(v_tmpls));
END $$;

-- Compile a projection whose query has subqueries. Every table any block reads gets one staging
-- table for its changed rows (captured with every block's columns), and a copy of the columns
-- read where some block joins it without aggregating it; each block that aggregates keeps its
-- fact's partial aggregates. The statements are generated per block (_engine_sql_blocks), each
-- block configured with the relations it reads instead of the tables:
--   derive  the query, every block over its state;
--   fact    each aggregating block's fact rows, from its staging table (and from the table, to build);
--   keys    for each relation of each block, the keys its changed rows reach.
CREATE FUNCTION isomer._engine_compile_nested(p isomer.projections, s jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_prefix  text := isomer._engine_prefix(p.name);
    v_sources jsonb := '{}';
    v_cfgs    jsonb := '[]';
    v_facts   jsonb := '[]';
    v_keys    jsonb := '[]';
    v_cfg     jsonb;
    v_rels    bigint[];
    v_derive  text;
    v_truth   text;
    v_stage   text;
    v_base    text;
    v_fd      text;
    v_n       integer;
    r         record;
    b         jsonb;
    x         jsonb;
    f         jsonb;
    v_cols    text[];
    v_pk      text[];
    v_i       integer;
BEGIN
    IF p.state_placement = 'distributed' THEN
        RAISE EXCEPTION 'isomer: projection % has subqueries, whose state is kept on the coordinator', p.name;
    END IF;

    -- Every table read: a staging table, and for joined tables the kept columns (as in 0.21.0).
    FOR r IN SELECT DISTINCT (e ->> 'relid')::oid AS relid, e ->> 'table' AS tbl
             FROM jsonb_array_elements(s -> 'blocks') bl, jsonb_array_elements(bl -> 'rtes') e LOOP
        x := jsonb_build_object('table', r.tbl,
                                'staging', format('isomer_state.%I', v_prefix || '_x' || r.relid),
                                'capture', s -> 'capture' -> r.relid::text);
        EXECUTE format('CREATE UNLOGGED TABLE %s (%s, __w integer NOT NULL)', x ->> 'staging', isomer._aligned(r.relid, NULL, NULL, true));
        IF s -> 'dims' ? r.relid::text THEN
            v_cols := ARRAY(SELECT jsonb_array_elements_text(s -> 'dims' -> r.relid::text));
            v_pk := isomer._primary_key(r.relid);
            IF v_pk IS NOT NULL AND NOT (v_pk <@ v_cols) THEN
                v_pk := NULL;
            END IF;
            x := x || jsonb_build_object('state', format('isomer_state.%I', v_prefix || '_s' || r.relid),
                                         'view', format('isomer_state.%I', v_prefix || '_v' || r.relid),
                                         'cols', to_jsonb(v_cols), 'pk', to_jsonb(v_pk));
            EXECUTE format('CREATE TABLE %s (%s%s, __w bigint NOT NULL, PRIMARY KEY (%s))',
                           x ->> 'state',
                           CASE WHEN v_pk IS NULL THEN '__h text NOT NULL, ' ELSE '' END,
                           (SELECT string_agg(format('%I %s', c, isomer._coltype(r.relid, c)), ', ') FROM unnest(v_cols) c),
                           CASE WHEN v_pk IS NULL THEN '__h'
                                ELSE (SELECT string_agg(format('%I', c), ', ') FROM unnest(v_pk) c) END);
            -- The columns joins, correlations and grouping look up by.
            FOR v_i IN 1 .. cardinality(v_cols) LOOP
                IF v_pk IS NULL OR NOT (v_cols[v_i] = ANY (v_pk)) OR cardinality(v_pk) > 1 THEN
                    EXECUTE format('CREATE INDEX ON %s (%I)', x ->> 'state', v_cols[v_i]);
                END IF;
            END LOOP;
            EXECUTE format('CREATE VIEW %s AS SELECT %s FROM %s s WHERE s.__w > 0',
                           x ->> 'view', isomer._aligned(r.relid, v_cols, 's'), x ->> 'state');
        END IF;
        v_sources := v_sources || jsonb_build_object(r.relid::text, x);
    END LOOP;

    -- Each block: its fact's partial aggregates, and the relations it reads as state.
    FOR b IN SELECT * FROM jsonb_array_elements(s -> 'blocks') ORDER BY (value ->> 'block')::int LOOP
        v_n := (b ->> 'block')::int;
        v_rels := array_fill(0::bigint, ARRAY[(b ->> 'max_rti')::int]);
        v_cfg := jsonb_build_object('fact', coalesce((b ->> 'fact')::int, 0));
        f := NULL;
        IF b ->> 'fact' IS NOT NULL THEN
            f := isomer._engine_fact_state(format('%s_b%s', v_prefix, v_n), (b ->> 'fact_relid')::oid,
                                           ARRAY(SELECT jsonb_array_elements_text(b -> 'pkeys')), b -> 'aggs');
            v_cfg := v_cfg || jsonb_build_object('templates', f -> 'templates', 'fact_columns', b -> 'pkeys');
            v_facts := v_facts || jsonb_build_array(f || jsonb_build_object(
                'block', v_n, 'fact', (b ->> 'fact')::int, 'fact_relid', (b ->> 'fact_relid')::oid,
                'pkeys', b -> 'pkeys', 'aggs', b -> 'aggs'));
        END IF;
        FOR x IN SELECT * FROM jsonb_array_elements(b -> 'rtes') LOOP
            v_rels[(x ->> 'rti')::int] := CASE WHEN x ->> 'role' = 'fact' THEN (f ->> 'p_view')::regclass::oid
                                               ELSE (v_sources -> (x ->> 'relid') ->> 'view')::regclass::oid END;
        END LOOP;
        v_cfgs := v_cfgs || jsonb_build_array(v_cfg || jsonb_build_object('rels', to_jsonb(v_rels)));
    END LOOP;

    v_derive := isomer._engine_sql_blocks(p.query, 'derive', v_cfgs, 0, NULL);

    -- Each fact's rows: from its staging table while folding, from the table when building.
    FOR v_i IN 0 .. jsonb_array_length(v_facts) - 1 LOOP
        f := v_facts -> v_i;
        v_n := (f ->> 'block')::int;
        v_cfg := jsonb_set(v_cfgs, ARRAY[v_n::text, 'rels', ((f ->> 'fact')::int - 1)::text],
                           to_jsonb((v_sources -> (f ->> 'fact_relid') ->> 'staging')::regclass::oid::bigint));
        v_stage := isomer._engine_sql_blocks(p.query, 'fact', v_cfg, v_n, NULL);
        v_base := isomer._engine_sql_blocks(p.query, 'fact', jsonb_set(v_cfgs, ARRAY[v_n::text, 'rels'], '[]'), v_n, NULL);
        v_fd := format('isomer_state.%I', format('%s_b%sfd', v_prefix, v_n));
        EXECUTE format('CREATE UNLOGGED TABLE %s AS SELECT * FROM (%s) d WITH NO DATA', v_fd, v_stage);
        v_facts := jsonb_set(v_facts, ARRAY[v_i::text], f || jsonb_build_object('fd_table', v_fd, 'fact_stage_sql', v_stage, 'fact_base_sql', v_base));
    END LOOP;

    -- The keys each relation's changed rows reach: that relation read from its staging table.
    FOR b IN SELECT * FROM jsonb_array_elements(s -> 'blocks') LOOP
        v_n := (b ->> 'block')::int;
        FOR x IN SELECT * FROM jsonb_array_elements(b -> 'rtes') LOOP
            v_cfg := jsonb_set(v_cfgs, ARRAY[v_n::text, 'rels', ((x ->> 'rti')::int - 1)::text],
                               to_jsonb((v_sources -> (x ->> 'relid') ->> 'staging')::regclass::oid::bigint));
            v_keys := v_keys || jsonb_build_object('block', v_n, 'rti', (x ->> 'rti')::int, 'relid', (x ->> 'relid')::oid,
                'sql', isomer._engine_sql_blocks(p.query, 'keys', v_cfg, v_n, (x ->> 'rti')::int));
        END LOOP;
    END LOOP;

    -- What the tables produce, for the auditor. Citus can't plan correlated subqueries between
    -- tables that are not co-located once the query is filtered or compared with something else,
    -- so each Citus table is read through a view that it plans on its own (OFFSET 0: an
    -- intermediate result on the coordinator), and the query runs over those.
    IF EXISTS (SELECT 1 FROM jsonb_object_keys(v_sources) k WHERE (isomer._distribution(k::oid::regclass)).method IS NOT NULL) THEN
        v_cfg := '[]';
        FOR b IN SELECT * FROM jsonb_array_elements(s -> 'blocks') ORDER BY (value ->> 'block')::int LOOP
            v_rels := array_fill(0::bigint, ARRAY[(b ->> 'max_rti')::int]);
            FOR x IN SELECT * FROM jsonb_array_elements(b -> 'rtes') LOOP
                IF (isomer._distribution((x ->> 'relid')::oid::regclass)).method IS NOT NULL THEN
                    IF to_regclass(format('isomer_state.%I', v_prefix || '_t' || (x ->> 'relid'))) IS NULL THEN
                        EXECUTE format('CREATE VIEW isomer_state.%I AS SELECT %s FROM %s t OFFSET 0', v_prefix || '_t' || (x ->> 'relid'),
                                       isomer._aligned((x ->> 'relid')::oid,
                                                       ARRAY(SELECT attname::text FROM pg_attribute WHERE attrelid = (x ->> 'relid')::oid AND attnum > 0 AND NOT attisdropped), 't'),
                                       x ->> 'table');
                    END IF;
                    v_rels[(x ->> 'rti')::int] := format('isomer_state.%I', v_prefix || '_t' || (x ->> 'relid'))::regclass::oid;
                END IF;
            END LOOP;
            v_cfg := v_cfg || jsonb_build_array(jsonb_build_object('rels', to_jsonb(v_rels)));
        END LOOP;
        v_truth := isomer._engine_sql_blocks(p.query, 'swap', v_cfg, 0, NULL);
    END IF;

    x := s || jsonb_build_object(
        'prefix', v_prefix, 'sources', v_sources, 'facts', v_facts, 'block_config', v_cfgs,
        'derive_sql', v_derive, 'key_sqls', v_keys, 'minmax', '[]'::jsonb, 'distributed', false);
    INSERT INTO isomer.engine_plans (projection, plan) VALUES (p.name, x)
    ON CONFLICT (projection) DO UPDATE SET plan = EXCLUDED.plan, compiled_at = clock_timestamp();
    UPDATE isomer.projections SET derive_query = v_derive, truth_query = v_truth WHERE name = p.name;
    RETURN x;
END $$;

-- Compile: a query with subqueries is compiled block by block, any other as in 0.21.0.
ALTER FUNCTION isomer._engine_compile(text) RENAME TO _engine_compile_single;

CREATE FUNCTION isomer._engine_compile(p_projection text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p isomer.projections;
    s jsonb;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    s := isomer._engine_shape(p.query, p.key_column, p.kind);
    IF NOT coalesce((s ->> 'nested')::boolean, false) THEN
        RETURN isomer._engine_compile_single(p_projection);
    END IF;
    IF NOT (s ->> 'ok')::boolean THEN
        RAISE EXCEPTION 'isomer: projection % cannot be maintained incrementally: %', p.name, s ->> 'reason';
    END IF;
    PERFORM isomer._engine_drop_state(p.name);
    RETURN isomer._engine_compile_nested(p, s);
END $$;

-- ── Folding and building, fact by fact ──────────────────────────────────────

-- The facts of a plan: one per aggregating block of a query with subqueries; the plan itself
-- (which carries the same fields) for any other.
CREATE FUNCTION isomer._engine_facts(plan jsonb)
RETURNS SETOF jsonb LANGUAGE sql IMMUTABLE AS $$
    SELECT f FROM jsonb_array_elements(CASE WHEN plan ? 'facts' THEN plan -> 'facts' ELSE jsonb_build_array(plan) END) f
    WHERE f ? 'p_table'
$$;

-- Build the state from the tables (see 0.17.0), every fact.
CREATE OR REPLACE FUNCTION isomer._engine_build(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    plan   jsonb := (SELECT e.plan FROM isomer.engine_plans e WHERE e.projection = p_projection);
    src    jsonb;
    f      jsonb;
    x      jsonb;
    v_cols text;
BEGIN
    FOR f IN SELECT * FROM isomer._engine_facts(plan) LOOP
        EXECUTE format('TRUNCATE %s', f ->> 'p_table');
        FOR x IN SELECT * FROM jsonb_array_elements(f -> 'minmax') LOOP
            EXECUTE format('TRUNCATE %s', x ->> 'table');
        END LOOP;
    END LOOP;
    FOR src IN SELECT value FROM jsonb_each(plan -> 'sources') WHERE value ? 'state' LOOP
        EXECUTE format('TRUNCATE %s', src ->> 'state');
        v_cols := (SELECT string_agg(format('%I', c), ', ') FROM jsonb_array_elements_text(src -> 'cols') c);
        IF jsonb_typeof(src -> 'pk') = 'array' THEN
            EXECUTE format('INSERT INTO %s (%s, __w) SELECT %s, 1 FROM %s', src ->> 'state', v_cols, v_cols, src ->> 'table');
        ELSE
            EXECUTE format('INSERT INTO %1$s (__h, %2$s, __w) SELECT md5(jsonb_build_array(%2$s)::text), %2$s, count(*) FROM %3$s GROUP BY %2$s',
                           src ->> 'state', v_cols, src ->> 'table');
        END IF;
    END LOOP;
    FOR f IN SELECT * FROM isomer._engine_facts(plan) LOOP
        PERFORM isomer._engine_fold_fact(f, '(' || (f ->> 'fact_base_sql') || ')');
    END LOOP;
END $$;

-- Fold queued deltas (see 0.21.0): every fact a changed table is the fact of.
CREATE OR REPLACE FUNCTION isomer._engine_fold(p_projection text, p_batch integer DEFAULT 20000)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        isomer.projections;
    plan     jsonb;
    v_rows   isomer.deltas[];
    v_relids oid[];
    v_keys   text[];
    v_token  bigint;
    v_oldest timestamptz;
    src      jsonb;
    f        jsonb;
    v_rel    oid;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    SELECT e.plan INTO plan FROM isomer.engine_plans e WHERE e.projection = p.name;
    IF plan IS NULL THEN
        RETURN 0;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtext('isomer.engine'), hashtext(p.name));
    -- A distributed state is written and then read again in this transaction (the keys a fold
    -- reaches, the derivation): one connection per node, or Citus can't see its own writes.
    IF coalesce((plan ->> 'distributed')::boolean, false) THEN
        PERFORM set_config('citus.multi_shard_modify_mode', 'sequential', true);
    END IF;

    WITH taken AS (
        DELETE FROM isomer.deltas WHERE id IN (
            SELECT id FROM isomer.deltas WHERE projection = p.name ORDER BY id LIMIT p_batch)
        RETURNING *)
    SELECT array_agg(t ORDER BY t.id) INTO v_rows FROM taken t;
    IF v_rows IS NULL THEN
        RETURN 0;
    END IF;
    IF EXISTS (SELECT 1 FROM unnest(v_rows) d WHERE d.image IS NULL) THEN
        PERFORM isomer._engine_rebuild(p.name);
        RETURN cardinality(v_rows);
    END IF;

    SELECT min(d.token) FILTER (WHERE d.token > 0), min(d.enqueued_at) INTO v_token, v_oldest FROM unnest(v_rows) d;
    v_relids := ARRAY(SELECT DISTINCT d.source::regclass::oid FROM unnest(v_rows) d);

    FOREACH v_rel IN ARRAY v_relids LOOP
        src := plan -> 'sources' -> v_rel::text;
        CONTINUE WHEN src IS NULL;
        EXECUTE format('TRUNCATE %s', src ->> 'staging');
        EXECUTE format('INSERT INTO %1$s SELECT (jsonb_populate_record(NULL::%1$s, d.image || jsonb_build_object(''__w'', d.weight))).* '
                       'FROM unnest($1) d WHERE d.source::regclass = $2', src ->> 'staging')
        USING v_rows, v_rel::regclass;
    END LOOP;

    v_keys := isomer._engine_touched(plan, p.key_column, v_relids);

    FOR f IN SELECT * FROM isomer._engine_facts(plan) LOOP
        CONTINUE WHEN NOT ((f ->> 'fact_relid')::oid = ANY (v_relids));
        EXECUTE format('TRUNCATE %s', f ->> 'fd_table');
        EXECUTE format('INSERT INTO %s SELECT * FROM (%s) d', f ->> 'fd_table', f ->> 'fact_stage_sql');
        PERFORM isomer._engine_fold_fact(f, f ->> 'fd_table');
    END LOOP;
    FOREACH v_rel IN ARRAY v_relids LOOP
        src := plan -> 'sources' -> v_rel::text;
        CONTINUE WHEN src IS NULL OR NOT (src ? 'state');
        PERFORM isomer._engine_fold_dim(src);
    END LOOP;

    v_keys := v_keys || isomer._engine_touched(plan, p.key_column, v_relids);

    INSERT INTO isomer.pending AS q (projection, key, token, enqueued_at)
    SELECT p.name, k, coalesce(v_token, 0), v_oldest FROM (SELECT DISTINCT k FROM unnest(v_keys) k) s
    ON CONFLICT (projection, key) DO UPDATE
        SET token = CASE WHEN q.token = 0 THEN EXCLUDED.token
                         WHEN EXCLUDED.token = 0 THEN q.token
                         ELSE least(q.token, EXCLUDED.token) END,
            enqueued_at = least(q.enqueued_at, EXCLUDED.enqueued_at);
    UPDATE isomer.projection_stats SET deltas_folded = deltas_folded + cardinality(v_rows) WHERE projection = p.name;
    RETURN cardinality(v_rows);
END $$;

-- Why an incremental projection's state can't be distributed, or NULL (see 0.21.0).
CREATE OR REPLACE FUNCTION isomer._distribute_blocker(p_shape jsonb)
RETURNS text LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
BEGIN
    IF NOT isomer._multi_node() THEN
        RETURN 'there are no worker nodes to distribute it across';
    END IF;
    IF NOT (p_shape ->> 'ok')::boolean THEN
        RETURN 'it isn''t maintained incrementally: ' || (p_shape ->> 'reason');
    END IF;
    IF coalesce((p_shape ->> 'nested')::boolean, false) THEN
        RETURN 'it has subqueries, whose state is kept on the coordinator';
    END IF;
    RETURN NULL;
END $$;

-- The certificate (see 0.21.0), for every fact, and the rules a query with subqueries relies on.
CREATE OR REPLACE FUNCTION isomer._engine_certificate(p isomer.projections, OUT rules jsonb, OUT assumptions jsonb, OUT checks jsonb)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    plan    jsonb := (SELECT e.plan FROM isomer.engine_plans e WHERE e.projection = p.name);
    v_fact  text;
    v_edges text;
    v_shape jsonb;
    v_ok    boolean;
    v_n     bigint;
    v_age   interval;
    s       record;
    v_dist  boolean;
    v_by    text;
    v_neg   bigint := 0;
    f       jsonb;
BEGIN
    checks := '[]';
    IF plan IS NULL THEN
        rules := '[]';
        assumptions := '[]';
        checks := jsonb_build_array(jsonb_build_object('check', 'compiled', 'passed', false,
            'detail', 'no plan: run isomer.alter_projection(' || quote_literal(p.name) || ', strategy => ''recompute'') then back'));
        RETURN;
    END IF;
    -- A query with subqueries has a fact per block that aggregates.
    SELECT string_agg((fx ->> 'fact_relid')::oid::regclass::text, ', '),
           string_agg(format('%s is pre-aggregated by (%s)', (fx ->> 'fact_relid')::oid::regclass,
                             coalesce((SELECT string_agg(c, ', ') FROM jsonb_array_elements_text(fx -> 'pkeys') c), 'nothing: one group')), '; ')
    INTO v_fact, v_by
    FROM isomer._engine_facts(plan) fx;
    rules := jsonb_build_array(
        'capture: each change to a source row is recorded in the writing transaction, on the node holding the row, as row images weighted -1 (old) and +1 (new); a reference table''s change is recorded once, by its first placement',
        'delivery: deltas recorded on a worker reach the coordinator exactly once (taken in a prepared transaction there, committed only after the coordinator has recorded the pull)',
        format('linearity: the conditions on %s alone are applied to each delta row (a filter of a change is the change of the filter)', v_fact),
        format('aggregation below the joins: %s; valid because every join leading away from it lands on a unique key, so each of its rows joins at most one row of the rest',
               coalesce(v_by, 'no table is aggregated')),
        'integration: count and sum are kept as running sums of the weighted deltas (the change of a sum is the sum of the changes); min and max as integrated multisets of values; avg and bool_and/bool_or from those',
        'distinct: count, sum and avg (DISTINCT c) keep c among the grouped columns, so each partial row is one value of c, present while any row holds it',
        'kept as written: an aggregate that is not decomposable (jsonb_agg, string_agg, ordered aggregates) runs as written over partial rows that are one per row of the fact (its primary key is grouped), so over the same rows as the tables give',
        'joins over integrated state: a key can change only if a delta of one input reaches it through the state of the others, before or after the fold (the change of A join B is dA join B + A join dB + dA join dB), so exactly those keys are re-derived, from the state');
    IF coalesce((plan ->> 'nested')::boolean, false) THEN
        rules := rules || jsonb_build_array(
            format('subqueries: the query and each of its %s subqueries are blocks with their own state (partial aggregates grouped by the columns a block''s conditions on the outer row read, or copies of the columns read); a key is derived by the query as written, every block over its state, so a correlated subquery runs once per outer row over that row''s state',
                   jsonb_array_length(plan -> 'blocks') - 1),
            'keys through subqueries: a changed row of a subquery''s table can change only the outer rows it is correlated with (it is one of their rows, or no longer is); those are found by turning each subquery on the way up into EXISTS (a correlated changed row) in its parent, before and after the fold, and re-derived');
    END IF;
    IF coalesce((plan ->> 'distributed')::boolean, false) THEN
        rules := rules || to_jsonb(format('distributed state: %s''s partial aggregates are hash-distributed by group across %s nodes and the joined tables'' kept columns are reference tables on every node; a key is derived by filtering on it on every shard, aggregating there, and combining on the coordinator',
            v_fact, (SELECT count(DISTINCT pl.groupid) FROM pg_dist_shard sh JOIN pg_dist_placement pl USING (shardid) WHERE sh.logicalrelid = (plan ->> 'p_table')::regclass)));
        checks := checks || jsonb_build_object('check', 'state distributed',
            'passed', EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = (plan ->> 'p_table')::regclass AND partmethod = 'h'),
            'detail', format('%s shards of partial aggregates', (SELECT count(*) FROM pg_dist_shard WHERE logicalrelid = (plan ->> 'p_table')::regclass)));
    END IF;
    assumptions := jsonb_build_array(
        'every relation the query reads has a capture trigger on every placement, ENABLE ALWAYS',
        'the unique keys the joins land on still exist (re-checked below)',
        CASE WHEN p.freshness = 'sync'
             THEN 'sync: each statement''s deltas are folded and the keys they reach applied before it returns, in the writing transaction; writers to the projection''s tables take turns from their first write to commit'
             ELSE format('bounded: deltas are folded and the keys they reach applied within the lag budget (%s); read-your-writes via isomer.wait_for', p.lag_budget) END);

    FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
        v_dist := (isomer._distribution(s.source_table::regclass)).method IN ('h', 'n');
        IF v_dist THEN
            v_ok := isomer._triggers_on_all_placements(s.source_table::regclass, p.name);
        ELSE
            v_ok := EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = s.source_table::regclass
                            AND tgname = 'isomer_' || p.name AND tgenabled = 'A');
        END IF;
        checks := checks || jsonb_build_object('check', 'capture on ' || s.source_table, 'passed', v_ok,
            'detail', CASE WHEN v_ok THEN 'row-delta and truncate triggers present, ENABLE ALWAYS' || CASE WHEN v_dist THEN ', on every shard placement on every node' ELSE '' END
                           ELSE 'missing or disabled: run isomer._create_source_triggers(' || quote_literal(p.name) || ')' END);
    END LOOP;

    v_shape := isomer._engine_shape(p.query, p.key_column, p.kind);
    checks := checks || jsonb_build_object('check', 'plan conditions', 'passed', (v_shape ->> 'ok')::boolean,
        'detail', CASE WHEN (v_shape ->> 'ok')::boolean THEN 'the joins still land on unique keys and every aggregate is still decomposable'
                       ELSE v_shape ->> 'reason' END);

    FOR f IN SELECT * FROM isomer._engine_facts(plan) LOOP
        EXECUTE format('SELECT count(*) FROM %s WHERE __n < 0', f ->> 'p_table') INTO v_n;
        v_neg := v_neg + v_n;
    END LOOP;
    v_n := v_neg;
    checks := checks || jsonb_build_object('check', 'state', 'passed', v_n = 0,
        'detail', CASE WHEN v_n = 0 THEN 'no group holds a negative count' ELSE format('%s group(s) hold a negative count', v_n) END);

    SELECT clock_timestamp() - min(enqueued_at) INTO v_age FROM isomer.deltas WHERE projection = p.name;
    checks := checks || jsonb_build_object('check', 'deltas flowing', 'passed', v_age IS NULL OR v_age < greatest(p.lag_budget * 10, interval '30 seconds'),
        'detail', CASE WHEN v_age IS NULL THEN 'nothing waiting' ELSE format('oldest waiting %s', date_trunc('second', v_age)) END);
END $$;

REVOKE EXECUTE ON FUNCTION isomer._engine_blocks(text), isomer._engine_sql_blocks(text, text, jsonb, integer, integer),
                           isomer._engine_block_shape(jsonb, text, text, text), isomer._engine_fact_state(text, oid, text[], jsonb),
                           isomer._engine_compile_nested(isomer.projections, jsonb), isomer._engine_compile_single(text),
                           isomer._engine_compile(text), isomer._engine_facts(jsonb)
    FROM PUBLIC;


-- ── The cost of re-deriving a key ──────────────────────────────────────────

-- _key_cost (see 0.17.0), with its EXPLAIN kept from affecting the rest of the transaction.
CREATE OR REPLACE FUNCTION isomer._key_cost(p_query text, p_key text, OUT cost double precision, OUT kind text)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    s       jsonb := isomer._engine_shape(p_query, p_key, 'cache');
    g       jsonb;
    v_plan  jsonb;
    v_rel   regclass;
    v_col   text;
    v_val   text;
    v_key   jsonb := s -> 'key';
BEGIN
    IF NOT isomer._runnable(s) THEN
        cost := 'Infinity';
        kind := 'impossible';
        RETURN;
    END IF;
    g := isomer._shard_groups(s);
    IF g -> 'hash' = '{}' THEN
        -- Nothing sharded: plain Postgres planning, which Citus leaves alone.
        IF jsonb_typeof(v_key) = 'object' THEN
            v_rel := (SELECT (e ->> 'relid')::oid::regclass FROM jsonb_array_elements(s -> 'rtes') e WHERE e ->> 'rti' = v_key ->> 'rti');
            v_col := v_key ->> 'col';
            EXECUTE format('SELECT %I::text FROM %s WHERE %I IS NOT NULL LIMIT 1', v_col, v_rel, v_col) INTO v_val;
        END IF;
        -- In a subtransaction that is then rolled back: once a transaction has run an EXPLAIN,
        -- Citus sends its later queries on reference tables to this node over a connection, which
        -- can't see what the transaction has written (create_projection's new collection).
        BEGIN
            EXECUTE format('EXPLAIN (FORMAT JSON) SELECT * FROM (%s) q %s', p_query,
                           CASE WHEN v_val IS NULL THEN '' ELSE format('WHERE q.%I = %L::%s', p_key, v_val, isomer._coltype(v_rel, v_col)) END)
            INTO v_plan;
            RAISE EXCEPTION USING ERRCODE = 'IS998';
        EXCEPTION WHEN SQLSTATE 'IS998' THEN
            NULL;
        END;
        cost := (v_plan -> 0 -> 'Plan' ->> 'Total Cost')::double precision;
        kind := 'local';
    ELSIF (SELECT count(DISTINCT value) FROM jsonb_each(g -> 'groups')) > 1 THEN
        cost := 'Infinity';
        kind := 'repartition';
    ELSIF jsonb_typeof(v_key) = 'object' AND g -> 'hash' ? (v_key ->> 'rti')
          AND g -> 'hash' -> (v_key ->> 'rti') ->> 'col' = v_key ->> 'col' THEN
        cost := 10;
        kind := 'router';
    ELSE
        cost := 10 * (SELECT count(*) FROM pg_dist_shard
                      WHERE logicalrelid = (SELECT (value ->> 'relid')::oid FROM jsonb_each(g -> 'hash') LIMIT 1));
        kind := 'fan-out';
    END IF;
END $$;

-- ── Folding touches only the groups it reached ─────────────────────────────
--
-- After a fold, emptied groups (and multiset values, and joined rows) were deleted by scanning
-- the whole state table; a state with a partial row per outer row (a correlated subquery's) is
-- as large as the outer table, so each fold paid for all of it. Only the groups this fold's
-- changes reached can have emptied.

CREATE OR REPLACE FUNCTION isomer._engine_fold_fact(plan jsonb, p_src text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_keys  text := (SELECT string_agg(format('d.%I', c), ', ') FROM jsonb_array_elements_text(plan -> 'pkeys') c);
    v_hash  text := format('md5(jsonb_build_array(%s)::text)', coalesce((SELECT string_agg(format('d.%I', c), ', ') FROM jsonb_array_elements_text(plan -> 'pkeys') c), ''));
    v_cols  text := '__h' || coalesce((SELECT string_agg(format(', %I', c), '') FROM jsonb_array_elements_text(plan -> 'pkeys') c), '') || ', __n';
    v_set   text := '__n = t.__n + EXCLUDED.__n';
    x       jsonb;
    v_i     integer;
BEGIN
    FOR x IN SELECT * FROM jsonb_array_elements(plan -> 'aggs') WHERE value ->> 'cls' <> 'keep' LOOP
        v_i := (x ->> 'i')::int;
        v_cols := v_cols || format(', __c%s', v_i);
        v_set := v_set || format(', __c%1$s = t.__c%1$s + EXCLUDED.__c%1$s', v_i);
        IF x ->> 'cls' IN ('sum', 'avg') THEN
            v_cols := v_cols || format(', __s%s', v_i);
            v_set := v_set || format(', __s%1$s = t.__s%1$s + EXCLUDED.__s%1$s', v_i);
        ELSIF x ->> 'cls' IN ('bool_and', 'bool_or') THEN
            v_cols := v_cols || format(', __t%s', v_i);
            v_set := v_set || format(', __t%1$s = t.__t%1$s + EXCLUDED.__t%1$s', v_i);
        END IF;
    END LOOP;
    EXECUTE format('INSERT INTO %1$s AS t (%2$s) %3$s ON CONFLICT (__h) DO UPDATE SET %4$s',
                   plan ->> 'p_table', v_cols, isomer._engine_p_select(plan, p_src), v_set);
    -- Only the groups this fold reached can have emptied.
    EXECUTE format('DELETE FROM %1$s t WHERE t.__n = 0 AND t.__h IN (SELECT %2$s FROM %3$s d)', plan ->> 'p_table', v_hash, p_src);

    FOR x IN SELECT * FROM jsonb_array_elements(plan -> 'minmax') LOOP
        v_i := (x ->> 'i')::int;
        EXECUTE format(
            'INSERT INTO %1$s AS m (__h, value, mult) SELECT %2$s, d.__a%3$s, sum(d.__w) FROM %4$s d '
            'WHERE d.__f%3$s AND d.__a%3$s IS NOT NULL GROUP BY %5$s d.__a%3$s HAVING sum(d.__w) <> 0 '
            'ON CONFLICT (__h, value) DO UPDATE SET mult = m.mult + EXCLUDED.mult',
            x ->> 'table', v_hash, v_i, p_src, coalesce(v_keys || ', ', ''));
        EXECUTE format('DELETE FROM %1$s m WHERE m.mult = 0 AND m.__h IN (SELECT %2$s FROM %3$s d)', x ->> 'table', v_hash, p_src);
        EXECUTE format(
            'UPDATE %1$s t SET __m%2$s = (SELECT %3$s(m.value) FROM %4$s m WHERE m.__h = t.__h AND m.mult > 0) '
            'WHERE t.__h IN (SELECT %5$s FROM %6$s d)',
            plan ->> 'p_table', v_i, x ->> 'cls', x ->> 'table', v_hash, p_src);
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION isomer._engine_fold_dim(src jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_cols  text := (SELECT string_agg(format('%I', c), ', ') FROM jsonb_array_elements_text(src -> 'cols') c);
    v_xcols text := (SELECT string_agg(format('x.%I', c), ', ') FROM jsonb_array_elements_text(src -> 'cols') c);
    v_pk    text := (SELECT string_agg(format('%I', c), ', ') FROM jsonb_array_elements_text(src -> 'pk') c);
    v_upd   text := (SELECT string_agg(format('%1$I = EXCLUDED.%1$I', c), ', ') FROM jsonb_array_elements_text(src -> 'cols') c
                     WHERE NOT (src -> 'pk' ? c));
    v_net   text;
    v_rows  jsonb;
BEGIN
    v_net := format('SELECT %s, sum(__w) AS __w FROM %s GROUP BY %s', v_cols, src ->> 'staging', v_cols);
    IF coalesce((src ->> 'replicated')::boolean, false) THEN
        EXECUTE format('SELECT jsonb_agg(x) FROM (%s) x WHERE x.__w <> 0', v_net) INTO v_rows;
        IF v_rows IS NULL THEN
            RETURN;
        END IF;
        v_net := format('SELECT * FROM jsonb_to_recordset($1) AS r(%s, __w bigint)',
                        (SELECT string_agg(format('%I %s', c, isomer._coltype((src ->> 'table')::regclass, c)), ', ')
                         FROM jsonb_array_elements_text(src -> 'cols') c));
    END IF;
    IF jsonb_typeof(src -> 'pk') = 'array' THEN
        EXECUTE format('UPDATE %1$s s SET __w = s.__w + x.__w FROM (%2$s) x WHERE x.__w < 0 AND (%3$s) = (%4$s)',
                       src ->> 'state', v_net,
                       (SELECT string_agg(format('s.%I', c), ', ') FROM jsonb_array_elements_text(src -> 'pk') c),
                       (SELECT string_agg(format('x.%I', c), ', ') FROM jsonb_array_elements_text(src -> 'pk') c))
        USING v_rows;
        -- Only rows of this fold's changes can have left (all of them, for a state on every node:
        -- a reference table isn't written with a subquery on a table here).
        IF coalesce((src ->> 'replicated')::boolean, false) THEN
            EXECUTE format('DELETE FROM %s WHERE __w <= 0', src ->> 'state');
        ELSE
            EXECUTE format('DELETE FROM %1$s s WHERE s.__w <= 0 AND (%2$s) IN (SELECT %3$s FROM %4$s x)', src ->> 'state',
                           (SELECT string_agg(format('s.%I', c), ', ') FROM jsonb_array_elements_text(src -> 'pk') c),
                           (SELECT string_agg(format('x.%I', c), ', ') FROM jsonb_array_elements_text(src -> 'pk') c),
                           src ->> 'staging');
        END IF;
        EXECUTE format('INSERT INTO %1$s AS s (%2$s, __w) SELECT %3$s, x.__w FROM (%4$s) x WHERE x.__w > 0 '
                       'ON CONFLICT (%5$s) DO UPDATE SET %6$s__w = s.__w + EXCLUDED.__w',
                       src ->> 'state', v_cols, v_xcols, v_net, v_pk, coalesce(v_upd || ', ', ''))
        USING v_rows;
    ELSE
        EXECUTE format('INSERT INTO %1$s AS s (__h, %2$s, __w) SELECT md5(jsonb_build_array(%3$s)::text), %3$s, x.__w FROM (%4$s) x WHERE x.__w <> 0 '
                       'ON CONFLICT (__h) DO UPDATE SET __w = s.__w + EXCLUDED.__w',
                       src ->> 'state', v_cols, v_xcols, v_net)
        USING v_rows;
        IF coalesce((src ->> 'replicated')::boolean, false) THEN
            EXECUTE format('DELETE FROM %s WHERE __w = 0', src ->> 'state');
        ELSE
            EXECUTE format('DELETE FROM %1$s s WHERE s.__w = 0 AND s.__h IN (SELECT md5(jsonb_build_array(%2$s)::text) FROM %3$s x)',
                           src ->> 'state', v_xcols, src ->> 'staging');
        END IF;
    END IF;
END $$;

-- ── Dropping a document projection's collection ─────────────────────────────

-- drop_projection (see 0.21.0), dropping the collection with DDL propagation on.
CREATE OR REPLACE FUNCTION isomer.drop_projection(name text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off SET citus.enable_unsafe_triggers = on AS $$
DECLARE
    p isomer.projections;
BEGIN
    SELECT * INTO p FROM isomer.projections WHERE projections.name = drop_projection.name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', name;
    END IF;
    IF p.kind = 'event' THEN
        PERFORM isomer._drop_source_triggers(p.name);
    ELSIF p.kind = 'analytics' THEN
        EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
        EXECUTE format('DROP TABLE IF EXISTS isomer_analytics.%I',
                       p.name || '_' || (SELECT generation FROM isomer.analytics_builds WHERE projection = p.name));
        IF p.strategy = 'incremental' THEN
            PERFORM isomer._drop_source_triggers(p.name);
            PERFORM isomer._engine_drop_state(p.name);
            EXECUTE format('DROP TABLE IF EXISTS %s', isomer._analytics_delta(p.name));
        END IF;
    ELSIF p.kind IN ('document', 'vector', 'cache') THEN
        PERFORM isomer._drop_source_triggers(p.name);
        IF p.strategy = 'incremental' THEN
            PERFORM isomer._engine_drop_state(p.name);
        END IF;
        IF p.kind = 'document' THEN
            -- With DDL propagation off, Citus doesn't clean up after the collection's table: its
            -- shard and metadata stay behind, and the next citus_add_node fails on them.
            PERFORM set_config('citus.enable_ddl_propagation', 'on', true);
            PERFORM documentdb_api.drop_collection(p.mongo_database, p.name);
            PERFORM set_config('citus.enable_ddl_propagation', 'off', true);
        ELSIF p.kind = 'vector' THEN
            EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
            EXECUTE format('DROP TABLE IF EXISTS %s', isomer._vector_store(p.name));
        ELSE
            EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
            EXECUTE format('DROP TABLE IF EXISTS %s', isomer._cache_store(p.name));
        END IF;
    ELSE
        PERFORM set_config('isomer.dropping_projection', p.name, true);
        PERFORM pgtrickle.drop_stream_table(format('%I.%I', p.target_schema, p.name), false);
    END IF;
    DELETE FROM isomer.embed_dead_letter WHERE projection = p.name;
    DELETE FROM isomer.audit_suspects WHERE projection = p.name;
    DELETE FROM isomer.projections WHERE projections.name = p.name;
END $$;

-- Collections dropped by drop_projection since 0.17.0 left their tables' Citus metadata behind
-- (the table itself gone, and on one node its shard too); the next citus_add_node, or any full
-- metadata sync, failed on it. Clean it up the way Citus's drop trigger would have.
DO $$
DECLARE
    r      record;
    v_name record;
BEGIN
    PERFORM set_config('citus.enable_ddl_propagation', 'on', true);
    FOR r IN SELECT p.logicalrelid::oid AS relid FROM pg_dist_partition p
             WHERE NOT EXISTS (SELECT 1 FROM pg_class x WHERE x.oid = p.logicalrelid) LOOP
        -- The dropped table's name, from a shard of it still on this node.
        SELECT n.nspname::text AS schema_name, left(c.relname, length(c.relname) - length('_' || s.shardid))::text AS table_name
        INTO v_name
        FROM pg_dist_shard s JOIN pg_class c ON c.relname LIKE '%\_' || s.shardid AND c.relkind IN ('r', 'p')
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE s.logicalrelid = r.relid LIMIT 1;
        IF FOUND THEN
            PERFORM master_remove_distributed_table_metadata_from_workers(r.relid, v_name.schema_name, v_name.table_name);
            PERFORM citus_drop_all_shards(r.relid, v_name.schema_name, v_name.table_name, drop_shards_metadata_only := false);
            PERFORM master_remove_partition_metadata(r.relid, v_name.schema_name, v_name.table_name);
        ELSE
            -- Its shards are gone too (they were on other nodes): only the metadata is left.
            PERFORM citus_drop_all_shards(r.relid, 'pg_catalog', 'dropped_' || r.relid, drop_shards_metadata_only := true);
            PERFORM master_remove_partition_metadata(r.relid, 'pg_catalog', 'dropped_' || r.relid);
        END IF;
        PERFORM master_unmark_object_distributed('pg_class'::regclass, r.relid, 0);
    END LOOP;
END $$;

-- ── The auditor and trigger DDL take turns ──────────────────────────────────
--
-- Creating or dropping a projection's capture triggers locks every shard of its tables; the
-- auditor's reference queries lock shards too, across nodes and in another order (a repartition
-- join reads them in stages). On a cluster the two deadlocked, and Citus's deadlock detector
-- cancelled the DDL (drop_projection, alter_projection). Trigger DDL now waits for a running audit
-- before it locks anything, and an audit that finds trigger DDL under way skips its round.

ALTER FUNCTION isomer.audit(integer) RENAME TO _audit_projections;
ALTER FUNCTION isomer._create_source_triggers(text) RENAME TO _create_source_triggers_now;
ALTER FUNCTION isomer._drop_source_triggers(text) RENAME TO _drop_source_triggers_now;

CREATE FUNCTION isomer.audit(p_sample integer DEFAULT 50)
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NOT pg_try_advisory_xact_lock_shared(hashtext('isomer.audit')) THEN
        RETURN 0;
    END IF;
    RETURN isomer._audit_projections(p_sample);
END $$;

CREATE FUNCTION isomer._create_source_triggers(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('isomer.audit'));
    PERFORM isomer._create_source_triggers_now(p_projection);
END $$;

CREATE FUNCTION isomer._drop_source_triggers(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('isomer.audit'));
    PERFORM isomer._drop_source_triggers_now(p_projection);
END $$;

REVOKE EXECUTE ON FUNCTION isomer.audit(integer), isomer._audit_projections(integer),
                           isomer._create_source_triggers(text), isomer._create_source_triggers_now(text),
                           isomer._drop_source_triggers(text), isomer._drop_source_triggers_now(text)
    FROM PUBLIC;
