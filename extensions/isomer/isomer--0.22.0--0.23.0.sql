-- IsomerDB 0.23.0: subqueries in analytics projections, and in the WHERE of a query that
-- aggregates (PRD FR-P6).
--
--   * A block that aggregates may have subqueries in its WHERE and its output, not only inside
--     aggregates kept as written: SELECT tier, count(*) FROM customers c WHERE EXISTS (SELECT 1
--     FROM orders o WHERE o.customer_id = c.id) GROUP BY tier. The condition is not a filter of
--     the fact; the columns it reads stay in the fact's partial aggregates (grouped by them as
--     well), and it is applied to those when deriving.
--   * Analytics projections with subqueries are maintained incrementally like document and cache
--     ones. auto keeps them incremental unless some block keeps an aggregate as written.
--   * The analytics base table and its audit read the tables through truth_query, which Citus can
--     plan for correlated subqueries between sharded tables that are not co-located.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.23.0'" to load this file. \quit

-- What the engine needs to know about one block (see 0.22.0). A block that aggregates may now
-- have subqueries in its WHERE and output too: such a condition is not a filter of the fact, so
-- the columns it reads (the correlation, for a correlated subquery) are kept in the fact's partial
-- aggregates and it is applied to them when deriving. Every fact row of a partial row agrees on
-- those columns, so the condition passes or fails the partial row as a whole.
CREATE OR REPLACE FUNCTION isomer._engine_block_shape(a jsonb, p_key text, p_kind text, p_role text)
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

-- What the engine needs to know about a query (see 0.22.0): queries with subqueries now in
-- analytics projections too.
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

-- Every aggregate of a shape: the query's, or every block's for a query with subqueries.
CREATE FUNCTION isomer._engine_shape_aggs(p_shape jsonb)
RETURNS SETOF jsonb LANGUAGE sql IMMUTABLE AS $$
    SELECT g FROM jsonb_array_elements(CASE WHEN p_shape ? 'blocks' THEN p_shape -> 'blocks' ELSE jsonb_build_array(p_shape) END) b,
                  jsonb_array_elements(coalesce(b -> 'aggs', '[]')) g
$$;

-- An analytics projection with subqueries: auto keeps it incremental unless some block keeps an
-- aggregate as written (see 0.21.0).
CREATE OR REPLACE FUNCTION isomer.create_analytics_projection(
    name          text,
    query         text,
    refresh       interval DEFAULT '5 minutes',
    target_schema text DEFAULT 'public',
    strategy      text DEFAULT 'auto')
-- The query resolves with the caller's search path, then is stored schema-qualified.
RETURNS text LANGUAGE plpgsql SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_probe    text := format('pg_temp.isomer_create_%s', pg_backend_pid());
    v_first    text;
    v_result   text;
    v_keys     text[];
    v_reason   text;
    v_shape    jsonb;
    v_strategy text := 'recompute';
    v_why      text;
    v_cols     jsonb;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_am WHERE amname = 'columnar') THEN
        RAISE EXCEPTION 'isomer: analytics projections need columnar storage'
            USING HINT = 'CREATE EXTENSION citus_columnar; (new clusters have it).';
    END IF;
    IF strategy NOT IN ('auto', 'rebuild', 'incremental') THEN
        RAISE EXCEPTION 'isomer: strategy must be ''auto'', ''rebuild'' or ''incremental'', not %', strategy;
    END IF;
    EXECUTE format('CREATE OR REPLACE TEMP VIEW %s AS %s', v_probe, query);
    query := isomer._viewdef(v_probe::regclass);
    EXECUTE format('DROP VIEW %s', v_probe);
    SELECT column_name INTO v_first FROM isomer._query_columns(query) LIMIT 1;

    SELECT k.keys, k.reason INTO v_keys, v_reason FROM isomer._analytics_keys(query) k;
    IF v_reason IS NULL THEN
        v_shape := isomer._engine_shape(query, coalesce(v_keys[1], v_first), 'analytics');
        IF NOT (v_shape ->> 'ok')::boolean THEN
            v_reason := v_shape ->> 'reason';
        END IF;
    END IF;
    IF create_analytics_projection.strategy = 'incremental' AND v_reason IS NOT NULL THEN
        RAISE EXCEPTION 'isomer: analytics projection % cannot be maintained incrementally: %', name, v_reason
            USING HINT = 'Use strategy => ''rebuild''.';
    END IF;
    IF create_analytics_projection.strategy = 'incremental' THEN
        v_strategy := 'incremental';
        v_why := 'chosen by create_analytics_projection(strategy => ''incremental'')';
    ELSIF create_analytics_projection.strategy = 'auto' AND v_reason IS NULL THEN
        IF EXISTS (SELECT 1 FROM isomer._engine_shape_aggs(v_shape) g WHERE g ->> 'cls' = 'keep') THEN
            v_why := format('rebuilt as a columnar table when its sources change, at most every %s: maintained incrementally it would keep a copy of every row its %s read',
                            refresh, (SELECT string_agg(DISTINCT g ->> 'fn', ', ') FROM isomer._engine_shape_aggs(v_shape) g WHERE g ->> 'cls' = 'keep'));
        ELSE
            v_strategy := 'incremental';
            v_why := format('each change is folded into partial aggregates and only the rows it reaches are rewritten, merged into a new columnar table at most every %s', refresh);
        END IF;
    ELSE
        v_why := format('rebuilt as a columnar table when its sources change, at most every %s', refresh)
              || CASE WHEN create_analytics_projection.strategy = 'auto' THEN format(' (incremental maintenance does not apply: %s)', v_reason) ELSE '' END;
    END IF;

    INSERT INTO isomer.projections (name, kind, query, key_column, key_type, freshness, lag_budget, target_schema, strategy, strategy_reason, definition)
    VALUES (name, 'analytics', query, coalesce(v_keys[1], v_first), 'text', 'bounded',
            CASE WHEN v_strategy = 'incremental' THEN interval '2 seconds' ELSE refresh END, target_schema, v_strategy, v_why,
            jsonb_build_object('refresh', refresh, 'strategy', create_analytics_projection.strategy));
    INSERT INTO isomer.projection_sources (projection, source_table, key_column)
    SELECT name, isomer._qualified(t), NULL FROM isomer._query_tables(query) t;
    INSERT INTO isomer.projection_stats (projection) VALUES (name) ON CONFLICT DO NOTHING;
    INSERT INTO isomer.analytics_builds (projection, key_columns, refresh)
    VALUES (name, CASE WHEN v_strategy = 'incremental' THEN v_keys END, CASE WHEN v_strategy = 'incremental' THEN refresh END);

    IF v_strategy = 'incremental' THEN
        PERFORM isomer._engine_compile(name);
        SELECT jsonb_agg(column_name ORDER BY n) INTO v_cols FROM isomer._query_columns(query) WITH ORDINALITY q(column_name, column_type, type_oid, n);
        UPDATE isomer.engine_plans e
        SET plan = e.plan || jsonb_build_object(
            'key_columns', to_jsonb(v_keys),
            'key_types', (SELECT coalesce(jsonb_agg(c.column_type ORDER BY k.n), '[]') FROM unnest(v_keys) WITH ORDINALITY k(name, n)
                          JOIN isomer._query_columns(query) c ON c.column_name = k.name),
            'key_expr', format('jsonb_build_array(%s)::text', (SELECT coalesce(string_agg(format('q.%I', k), ', '), '') FROM unnest(v_keys) k)),
            'columns', v_cols)
        WHERE e.projection = name;
        EXECUTE format('CREATE TABLE %s AS SELECT NULL::text AS __k, false AS __deleted, q.* FROM (%s) q WITH NO DATA',
                       isomer._analytics_delta(name), query);
        EXECUTE format('ALTER TABLE %1$s ALTER COLUMN __k SET NOT NULL, ALTER COLUMN __deleted SET NOT NULL, ADD PRIMARY KEY (__k)',
                       isomer._analytics_delta(name));
        -- The capture triggers lock every shard against writes until this transaction commits,
        -- so the state, the base table and the deltas that follow meet without a gap.
        PERFORM isomer._create_source_triggers(name);
        v_result := isomer._analytics_rebuild(name, true);
    ELSE
        v_result := isomer.refresh_analytics(name, force => true);
    END IF;
    RETURN format('analytics projection %s: %s; read it as %I.%I', name, v_result, target_schema, name);
END $$;

-- The base table and the audit read what the tables produce through truth_query where the
-- projection has one (correlated subqueries over sharded tables, see 0.22.0).
CREATE OR REPLACE FUNCTION isomer._analytics_rebuild(p_projection text, p_state boolean DEFAULT true)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p       isomer.projections;
    b       isomer.analytics_builds;
    plan    jsonb;
    s       record;
    v_base  text;
    v_start timestamptz := clock_timestamp();
    v_rows  bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    SELECT * INTO STRICT b FROM isomer.analytics_builds WHERE projection = p.name FOR UPDATE;
    IF p_state THEN
        FOR s IN SELECT source_table FROM isomer.projection_sources WHERE projection = p.name LOOP
            EXECUTE format('LOCK TABLE %s IN SHARE MODE', s.source_table);
        END LOOP;
        IF isomer._multi_node() THEN
            PERFORM run_command_on_workers(format('DELETE FROM isomer.node_deltas WHERE projection = %L', p.name));
        END IF;
        DELETE FROM isomer.deltas WHERE projection = p.name;
        PERFORM isomer._engine_build(p.name);
    END IF;
    DELETE FROM isomer.pending WHERE projection = p.name;
    plan := (SELECT e.plan FROM isomer.engine_plans e WHERE e.projection = p.name);
    v_base := isomer._analytics_base(p.name, b.generation + 1);
    EXECUTE format('CREATE TABLE %s USING columnar AS SELECT %s AS __k, q.* FROM (%s) q', v_base, plan ->> 'key_expr', coalesce(p.truth_query, p.query));
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    EXECUTE format('DELETE FROM %s', isomer._analytics_delta(p.name));
    PERFORM isomer._analytics_view(p, v_base);
    IF b.generation > 0 THEN
        EXECUTE format('DROP TABLE IF EXISTS %s', isomer._analytics_base(p.name, b.generation));
    END IF;
    UPDATE isomer.analytics_builds
    SET generation = b.generation + 1, built_at = clock_timestamp(), build_ms = extract(epoch FROM clock_timestamp() - v_start) * 1000,
        rows = v_rows, bytes = pg_total_relation_size(v_base::regclass), merged_rows = 0, last_error = NULL
    WHERE projection = p.name;
    RETURN format('%s rows, %s columnar, maintained incrementally', v_rows, pg_size_pretty(pg_total_relation_size(v_base::regclass)));
END $$;

CREATE OR REPLACE FUNCTION isomer.analytics_differences(p_projection text)
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
    b isomer.analytics_builds;
    v bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection AND kind = 'analytics';
    SELECT * INTO STRICT b FROM isomer.analytics_builds WHERE projection = p.name;
    IF p.strategy = 'incremental' THEN
        -- Checked in one statement, so the tables, the rows and the queues are seen at one moment:
        -- with nothing queued, every committed change is in the rows. Changes still on a worker
        -- aren't in that moment; they are checked before and after.
        IF isomer._analytics_on_workers(p.name) > 0 THEN
            RETURN NULL;
        END IF;
        EXECUTE format(
            $q$SELECT CASE WHEN EXISTS (SELECT 1 FROM isomer.deltas WHERE projection = %4$L)
                             OR EXISTS (SELECT 1 FROM isomer.pending WHERE projection = %4$L)
                             OR EXISTS (SELECT 1 FROM isomer.key_log WHERE projection = %4$L) THEN NULL
                        ELSE (WITH a AS (SELECT to_jsonb(q) AS r FROM (%1$s) q), b AS (SELECT to_jsonb(t) AS r FROM %2$I.%3$I t)
                              SELECT (SELECT count(*) FROM (SELECT r FROM a EXCEPT ALL SELECT r FROM b) x)
                                   + (SELECT count(*) FROM (SELECT r FROM b EXCEPT ALL SELECT r FROM a) y)) END$q$,
            coalesce(p.truth_query, p.query), p.target_schema, p.name, p.name) INTO v;
        IF isomer._analytics_on_workers(p.name) > 0 THEN
            RETURN NULL;
        END IF;
        RETURN v;
    END IF;
    IF b.fingerprint IS DISTINCT FROM isomer._sources_fingerprint(p.name) THEN
        RETURN NULL;
    END IF;
    EXECUTE format(
        $q$WITH a AS (SELECT to_jsonb(q) AS r FROM (%1$s) q), b AS (SELECT to_jsonb(t) AS r FROM %2$I.%3$I t)
           SELECT (SELECT count(*) FROM (SELECT r FROM a EXCEPT ALL SELECT r FROM b) x)
                + (SELECT count(*) FROM (SELECT r FROM b EXCEPT ALL SELECT r FROM a) y)$q$,
        p.query, p.target_schema, p.name) INTO v;
    RETURN v;
END $$;
