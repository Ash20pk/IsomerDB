-- IsomerDB 0.21.0: incremental maintenance for more queries (PRD FR-P6).
--
--   * Nested documents: an aggregate that is not decomposable into sums, counts or a multiset
--     (jsonb_agg, json_agg, array_agg, string_agg, ordered aggregates, percentiles) is kept as
--     written. The aggregated table's primary key and every column the aggregate reads become
--     grouped columns of its partial aggregates, so they hold one row per row of the table and
--     the aggregate runs, as written, over the same rows the tables give. Such an aggregate may
--     read the joined tables too (an order's lines with each product's name).
--   * count, sum and avg (DISTINCT c) over a column of the aggregated table keep c's values per
--     group; over a joined table's column, they are kept as written. min, max, bool_and and
--     bool_or ignore DISTINCT.
--   * GROUP BY may group by expressions (not volatile ones).
--   * freshness => 'sync' with strategy => 'incremental': each statement's deltas are folded, and
--     the keys they reach applied, before it returns, in the writing transaction. Writers to the
--     projection's tables take turns from their first write to commit. Its tables must be plain
--     tables here (not sharded, not replicated to workers, not attached).

--   * Analytics projections maintained incrementally (create_analytics_projection(strategy =>
--     'incremental'), or 'auto', which picks it when the query fits and keeps no copy of every
--     row): the engine folds each change into its state and rewrites only the rows it reaches,
--     into a heap of changed rows beside the columnar table; readers see the columnar table with
--     those rows replaced, and at most every refresh interval the two are merged into a new
--     columnar table, without running the query again. strategy => 'rebuild' is the 0.20 way.
--   * dump_projections writes analytics and event projections back with their own functions
--     (event projections now record their rules).

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.21.0'" to load this file. \quit

-- ── Shape: aggregates kept as written, DISTINCT, grouping expressions ───────

-- What the engine needs to know about a query, or why it cannot maintain it (see 0.17.0). New:
--   * an aggregate that is not decomposable into sums, counts or a multiset (jsonb_agg,
--     string_agg, array_agg, ordered aggregates, percentiles, anything else) is kept as written
--     ("keep"): the fact's primary key and every column the aggregate reads are kept columns, so
--     its partial aggregates are one row per fact row and the aggregate runs over the same rows as
--     over the tables. Such an aggregate may also read the joined tables.
--   * count, sum and avg (DISTINCT c) over a column of the fact keep c among the kept columns
--     ("distinct"): each partial row is one value of c in its group, present while any row holds
--     it, and the aggregate runs over those values. min, max, bool_and and bool_or ignore DISTINCT.
--   * GROUP BY may group by expressions (not volatile ones); the key must be one of the grouped
--     outputs.
CREATE OR REPLACE FUNCTION isomer._engine_shape(p_query text, p_key text, p_kind text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a          jsonb;
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
    v_numeric  text[] := ARRAY['smallint', 'integer', 'bigint', 'numeric', 'real', 'double precision'];
BEGIN
    IF p_kind NOT IN ('document', 'cache', 'analytics') THEN
        RETURN jsonb_build_object('ok', false, 'reason', format('%s projections are not maintained incrementally', p_kind));
    END IF;
    BEGIN
        a := isomer._engine_analyze(p_query);
    EXCEPTION WHEN OTHERS THEN
        RETURN jsonb_build_object('ok', false, 'reason', 'the query could not be analysed: ' || SQLERRM);
    END;

    SELECT string_agg(k, ', ') INTO v_reason
    FROM jsonb_each(a -> 'flags') f(k, v)
    WHERE v = 'true' AND k NOT IN ('having', 'order');
    IF v_reason IS NOT NULL THEN
        RETURN jsonb_build_object('ok', false, 'reason', format('it uses %s', replace(replace(replace(replace(replace(v_reason,
            'sublinks', 'subqueries'), 'setops', 'UNION/INTERSECT/EXCEPT'), 'srf', 'set-returning functions'), 'cte', 'WITH'), 'window', 'window functions')));
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

    -- Aggregates: decomposed into sums, counts or a multiset where they can be, kept as written
    -- otherwise.
    IF jsonb_array_length(a -> 'aggs') = 0 THEN
        RETURN jsonb_build_object('ok', false, 'reason', 'it has no aggregate: re-deriving a key reads only that key''s rows, which is already the cheapest way');
    END IF;
    FOR v_x IN SELECT * FROM jsonb_array_elements(a -> 'aggs') LOOP
        v_simple := coalesce(v_x ->> 'schema' = 'pg_catalog' AND v_x ->> 'kind' = 'n'
            AND v_x ->> 'fn' IN ('count', 'sum', 'avg', 'min', 'max', 'bool_and', 'bool_or', 'every')
            AND NOT (v_x ->> 'ordered')::boolean AND (v_x ->> 'nargs')::int <= 1
            AND NOT (v_x ->> 'fn' <> 'count' AND (SELECT t.typtype = 'c' OR t.oid = 'record'::regtype FROM pg_type t WHERE t.oid = (v_x ->> 'argtype')::regtype))
            AND NOT (v_x ->> 'fn' IN ('sum', 'avg') AND NOT (v_x ->> 'argtype' = ANY (v_numeric)))
            AND NOT (v_x ->> 'fn' IN ('min', 'max') AND NOT isomer._has_equality(v_x ->> 'argtype')), false);
        v_cls := CASE
            WHEN NOT v_simple THEN 'keep'
            WHEN (v_x ->> 'distinct')::boolean AND v_x ->> 'fn' IN ('count', 'sum', 'avg') THEN
                -- Over a plain column of the one table the aggregate reads: keep its values.
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
        -- DISTINCT over another table's column is kept as written once the fact is known.
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

    -- Grouping and the key.
    IF (a ->> 'group_volatile')::boolean THEN
        RETURN jsonb_build_object('ok', false, 'reason', 'it groups by a volatile expression');
    END IF;
    SELECT t -> 'col' INTO v_key FROM jsonb_array_elements(a -> 'target') t WHERE t ->> 'name' = p_key;
    IF jsonb_array_length(a -> 'group') > 0
       AND NOT coalesce((SELECT (t ->> 'grouped')::boolean FROM jsonb_array_elements(a -> 'target') t WHERE t ->> 'name' = p_key), false) THEN
        RETURN jsonb_build_object('ok', false, 'reason', format('the key %s is not one of the grouping columns', p_key));
    END IF;

    -- The fact: the table the decomposed aggregates read; or one of those the kept aggregates
    -- read; or (count(*) only) the one the joins lead away from.
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

    -- The fact's columns kept in its partial aggregates: everything read outside aggregates and
    -- outside its own filters (grouping, output, join conditions, conditions shared with others),
    -- and what the aggregates above need.
    v_pkeys := ARRAY(
        SELECT DISTINCT col FROM (
            SELECT c ->> 'col' AS col FROM (
                SELECT value AS c FROM jsonb_array_elements(a -> 'nonagg')
                UNION ALL
                SELECT col FROM jsonb_array_elements(a -> 'conjuncts') cj, jsonb_array_elements(cj -> 'cols') col
                WHERE cj -> 'rtis' <> jsonb_build_array(v_fact)) s
            WHERE jsonb_typeof(c) = 'object' AND (c ->> 'rti')::int = v_fact
            UNION ALL
            SELECT unnest(v_extra)) k
        ORDER BY 1);

    -- Relations, the columns captured from each, and the columns kept for each joined table.
    FOR v_rte IN SELECT * FROM jsonb_array_elements(a -> 'rtes') LOOP
        v_relid := (v_rte ->> 'relid')::oid;
        v_cols := ARRAY(SELECT DISTINCT c ->> 'col' FROM jsonb_array_elements(a -> 'columns') c
                        WHERE (c ->> 'rti')::int = (v_rte ->> 'rti')::int ORDER BY 1);
        v_rtes := v_rtes || jsonb_build_object('rti', (v_rte ->> 'rti')::int, 'relid', v_relid,
                                               'table', isomer._qualified(v_relid::regclass),
                                               'role', CASE WHEN (v_rte ->> 'rti')::int = v_fact THEN 'fact' ELSE 'dim' END);
        IF (v_rte ->> 'rti')::int = v_fact THEN
            v_cols := ARRAY(SELECT DISTINCT e FROM unnest(v_cols || v_pkeys) e ORDER BY 1);
        END IF;
        v_capture := jsonb_set(v_capture, ARRAY[v_relid::text],
            to_jsonb(ARRAY(SELECT DISTINCT e FROM unnest(ARRAY(SELECT jsonb_array_elements_text(coalesce(v_capture -> v_relid::text, '[]')))
                                                  || v_cols) e ORDER BY 1)));
        IF (v_rte ->> 'rti')::int <> v_fact THEN
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
        'nullable_fact', v_nullable, 'pkeys', to_jsonb(v_pkeys), 'aggs', v_aggs, 'rtes', v_rtes,
        'capture', v_capture, 'dims', v_dims, 'group', a -> 'group', 'key', v_key,
        'edges', (SELECT coalesce(jsonb_agg(c -> 'equi'), '[]') FROM jsonb_array_elements(a -> 'conjuncts') c
                  WHERE jsonb_typeof(c -> 'equi') = 'array'),
        'joins', a -> 'joins',
        'max_rti', (SELECT max(n) FROM (SELECT (e ->> 'rti')::int AS n FROM jsonb_array_elements(a -> 'rtes') e
                                      UNION ALL SELECT jsonb_array_length(a -> 'rtes')) s));
END $$;

-- Compile (see 0.17.0); aggregates kept as written have no partial aggregate and a NULL template.
CREATE OR REPLACE FUNCTION isomer._engine_compile(p_projection text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p          isomer.projections;
    s          jsonb;
    v_prefix   text;
    v_fact     integer;
    v_factrel  oid;
    v_pkeys    text[];
    x          jsonb;
    r          record;
    v_rel      oid;
    v_cols     text[];
    v_pk       text[];
    v_sources  jsonb := '{}';
    v_p        text;
    v_pv       text;
    v_fd       text;
    v_defs     text;
    v_extra    text;
    v_tmpls    text[] := '{}';
    v_rels     oid[];
    v_k        oid[];
    v_derive   text;
    v_fstage   text;
    v_fbase    text;
    v_keys     jsonb := '[]';
    v_minmax   jsonb := '[]';
    v_i        integer;
    v_fpv      text;
    v_truth    text;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    s := isomer._engine_shape(p.query, p.key_column, p.kind);
    IF NOT (s ->> 'ok')::boolean THEN
        RAISE EXCEPTION 'isomer: projection % cannot be maintained incrementally: %', p.name, s ->> 'reason';
    END IF;
    PERFORM isomer._engine_drop_state(p.name);
    v_prefix := isomer._engine_prefix(p.name);
    v_fact := (s ->> 'fact')::int;
    v_factrel := (s ->> 'fact_relid')::oid;
    v_pkeys := ARRAY(SELECT jsonb_array_elements_text(s -> 'pkeys'));

    -- Every source: a staging table for its changed rows; for joined tables, the kept columns.
    FOR r IN SELECT DISTINCT (e ->> 'relid')::oid AS relid, e ->> 'table' AS tbl FROM jsonb_array_elements(s -> 'rtes') e LOOP
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
            -- The columns joins and grouping look up by.
            FOR v_i IN SELECT DISTINCT n FROM generate_series(1, cardinality(v_cols)) n LOOP
                IF v_pk IS NULL OR NOT (v_cols[v_i] = ANY (v_pk)) OR cardinality(v_pk) > 1 THEN
                    EXECUTE format('CREATE INDEX ON %s (%I)', x ->> 'state', v_cols[v_i]);
                END IF;
            END LOOP;
            EXECUTE format('CREATE VIEW %s AS SELECT %s FROM %s s WHERE s.__w > 0',
                           x ->> 'view', isomer._aligned(r.relid, v_cols, 's'), x ->> 'state');
        END IF;
        v_sources := v_sources || jsonb_build_object(r.relid::text, x);
    END LOOP;

    -- The fact: partial aggregates per group of its kept columns.
    v_p := format('isomer_state.%I', v_prefix || '_p');
    v_pv := format('isomer_state.%I', v_prefix || '_pv');
    v_fd := format('isomer_state.%I', v_prefix || '_fd');
    v_defs := (SELECT string_agg(format('%I %s', c, isomer._coltype(v_factrel, c)), ', ') FROM unnest(v_pkeys) c);
    v_extra := '__n';
    FOR x IN SELECT * FROM jsonb_array_elements(s -> 'aggs') LOOP
        v_i := (x ->> 'i')::int;
        IF x ->> 'cls' = 'keep' THEN
            -- Kept as written: no partial aggregate, the derivation runs the aggregate itself.
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
            v_minmax := v_minmax || jsonb_build_object('i', v_i, 'cls', x ->> 'cls', 'table', format('isomer_state.%I', v_prefix || '_m' || v_i));
            EXECUTE format('CREATE TABLE isomer_state.%I (__h text NOT NULL, value %s NOT NULL, mult bigint NOT NULL, PRIMARY KEY (__h, value))',
                           v_prefix || '_m' || v_i, x ->> 'stype');
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
            -- Each partial row holds one value of the column; it counts while a row holds it.
            WHEN 'distinct' THEN format('(%1$s(DISTINCT %2$I) FILTER (WHERE __c%3$s > 0))::%4$s', x ->> 'fn', x ->> 'col', v_i, x ->> 'restype') END;
    END LOOP;
    EXECUTE format('CREATE TABLE %s (__h text PRIMARY KEY, %s, __n bigint NOT NULL)', v_p, v_defs);
    FOR v_i IN 1 .. coalesce(cardinality(v_pkeys), 0) LOOP
        EXECUTE format('CREATE INDEX ON %s (%I)', v_p, v_pkeys[v_i]);
    END LOOP;
    EXECUTE format('CREATE VIEW %s AS SELECT %s, %s FROM %s p WHERE p.__n > 0',
                   v_pv, isomer._aligned(v_factrel, v_pkeys, 'p'),
                   (SELECT string_agg('p.' || e, ', ') FROM unnest(string_to_array(v_extra, ', ')) e), v_p);

    -- The statements, generated from the parse tree with relations swapped for state.
    v_rels := array_fill(0::oid, ARRAY[(s ->> 'max_rti')::int]);
    FOR x IN SELECT * FROM jsonb_array_elements(s -> 'rtes') LOOP
        v_rels[(x ->> 'rti')::int] := CASE WHEN x ->> 'role' = 'fact' THEN v_pv::regclass::oid
                                           ELSE (v_sources -> (x ->> 'relid') ->> 'view')::regclass::oid END;
    END LOOP;
    v_derive := isomer._engine_sql(p.query, 'derive', v_fact, NULL, v_rels, v_tmpls, NULL);
    v_k := array_fill(0::oid, ARRAY[(s ->> 'max_rti')::int]);
    v_k[v_fact] := (v_sources -> v_factrel::text ->> 'staging')::regclass::oid;
    v_fstage := isomer._engine_sql(p.query, 'fact', v_fact, NULL, v_k, v_tmpls, v_pkeys);
    v_fbase := isomer._engine_sql(p.query, 'fact', v_fact, NULL, NULL, v_tmpls, v_pkeys);
    FOR x IN SELECT * FROM jsonb_array_elements(s -> 'rtes') LOOP
        v_k := v_rels;
        v_k[(x ->> 'rti')::int] := (v_sources -> (x ->> 'relid') ->> 'staging')::regclass::oid;
        v_keys := v_keys || jsonb_build_object('rti', (x ->> 'rti')::int, 'relid', (x ->> 'relid')::oid,
            'sql', isomer._engine_sql(p.query, 'keys', v_fact, (x ->> 'rti')::int, v_k, NULL, NULL));
    END LOOP;
    EXECUTE format('CREATE UNLOGGED TABLE %s AS SELECT * FROM (%s) d WITH NO DATA', v_fd, v_fstage);

    x := s || jsonb_build_object('p_table', v_p, 'minmax', v_minmax);
    -- What the tables produce, for the auditor, when the cluster can't run the query as written
    -- (an outer join between tables that are not co-located, say): the same derivation over the
    -- fact's partial aggregates computed afresh from the table, joined with the tables themselves.
    IF NOT isomer._runnable(s) THEN
        v_fpv := format('isomer_state.%I', v_prefix || '_fpv');
        -- OFFSET 0 keeps Citus from pushing the aggregate into the join it couldn't run: the
        -- partial aggregates are computed first, as an intermediate result.
        EXECUTE format('CREATE VIEW %s AS SELECT %s, %s FROM (%s OFFSET 0) p',
                       v_fpv, isomer._aligned(v_factrel, v_pkeys, 'p'),
                       (SELECT string_agg('p.' || e, ', ') FROM unnest(string_to_array(v_extra, ', ')) e),
                       isomer._engine_p_select(x, '(' || v_fbase || ')', true));
        v_k := array_fill(0::oid, ARRAY[(s ->> 'max_rti')::int]);
        v_k[v_fact] := v_fpv::regclass::oid;
        v_truth := isomer._engine_sql(p.query, 'derive', v_fact, NULL, v_k, v_tmpls, NULL);
    END IF;

    x := s || jsonb_build_object(
        'prefix', v_prefix, 'p_table', v_p, 'p_view', v_pv, 'fd_table', v_fd, 'minmax', v_minmax,
        'sources', v_sources, 'derive_sql', v_derive, 'fact_stage_sql', v_fstage,
        'fact_base_sql', v_fbase, 'key_sqls', v_keys, 'templates', to_jsonb(v_tmpls));
    INSERT INTO isomer.engine_plans (projection, plan) VALUES (p.name, x)
    ON CONFLICT (projection) DO UPDATE SET plan = EXCLUDED.plan, compiled_at = clock_timestamp();
    UPDATE isomer.projections SET derive_query = v_derive, truth_query = v_truth WHERE name = p.name;
    RETURN x;
END $$;

CREATE OR REPLACE FUNCTION isomer._engine_p_select(plan jsonb, p_src text, p_fresh boolean DEFAULT false)
RETURNS text LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    v_keys text := (SELECT string_agg(format('d.%I', c), ', ') FROM jsonb_array_elements_text(plan -> 'pkeys') c);
    v_sel  text := 'sum(d.__w)::bigint AS __n';
    x      jsonb;
    v_i    integer;
BEGIN
    FOR x IN SELECT * FROM jsonb_array_elements(plan -> 'aggs') WHERE value ->> 'cls' <> 'keep' LOOP
        v_i := (x ->> 'i')::int;
        v_sel := v_sel || format(', coalesce(sum(d.__w) FILTER (WHERE d.__f%1$s AND d.__a%1$s IS NOT NULL), 0)::bigint AS __c%1$s', v_i);
        IF x ->> 'cls' IN ('sum', 'avg') THEN
            v_sel := v_sel || format(', coalesce(sum(d.__w::%2$s * d.__a%1$s::%2$s) FILTER (WHERE d.__f%1$s AND d.__a%1$s IS NOT NULL), 0)::%2$s AS __s%1$s', v_i, x ->> 'stype');
        ELSIF x ->> 'cls' IN ('bool_and', 'bool_or') THEN
            v_sel := v_sel || format(', coalesce(sum(d.__w) FILTER (WHERE d.__f%1$s AND d.__a%1$s IS TRUE), 0)::bigint AS __t%1$s', v_i);
        ELSIF x ->> 'cls' IN ('min', 'max') AND p_fresh THEN
            v_sel := v_sel || format(', %2$s(d.__a%1$s) FILTER (WHERE d.__f%1$s) AS __m%1$s', v_i, x ->> 'cls');
        END IF;
    END LOOP;
    RETURN format('SELECT md5(jsonb_build_array(%1$s)::text) AS __h%2$s, %3$s FROM %4$s d %5$s HAVING count(*) > 0',
                  coalesce(v_keys, ''), coalesce(', ' || v_keys, ''), v_sel, p_src,
                  CASE WHEN v_keys IS NULL THEN '' ELSE 'GROUP BY ' || v_keys END);
END $$;

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
    EXECUTE format('DELETE FROM %s WHERE __n = 0', plan ->> 'p_table');

    FOR x IN SELECT * FROM jsonb_array_elements(plan -> 'minmax') LOOP
        v_i := (x ->> 'i')::int;
        EXECUTE format(
            'INSERT INTO %1$s AS m (__h, value, mult) SELECT %2$s, d.__a%3$s, sum(d.__w) FROM %4$s d '
            'WHERE d.__f%3$s AND d.__a%3$s IS NOT NULL GROUP BY %5$s d.__a%3$s HAVING sum(d.__w) <> 0 '
            'ON CONFLICT (__h, value) DO UPDATE SET mult = m.mult + EXCLUDED.mult',
            x ->> 'table', v_hash, v_i, p_src, coalesce(v_keys || ', ', ''));
        EXECUTE format('DELETE FROM %s WHERE mult = 0', x ->> 'table');
        EXECUTE format(
            'UPDATE %1$s t SET __m%2$s = (SELECT %3$s(m.value) FROM %4$s m WHERE m.__h = t.__h AND m.mult > 0) '
            'WHERE t.__h IN (SELECT %5$s FROM %6$s d)',
            plan ->> 'p_table', v_i, x ->> 'cls', x ->> 'table', v_hash, p_src);
    END LOOP;
END $$;

-- ── Sync incremental projections ────────────────────────────────────────────

-- Why an incremental projection can't be sync over these tables, or NULL. Sync folds the
-- deltas and applies the keys they reach in the writing transaction, one writer at a time, so
-- every table must be an ordinary table here: not attached from another database (IsomerDB isn't
-- its primary), and not a Citus distributed or reference table. A statement across a sharded
-- table's shards runs their triggers over parallel connections of one distributed transaction,
-- which can't take turns on one state (each would wait for the others to commit), and on a
-- cluster the shards' deltas reach the coordinator only after they commit.
CREATE FUNCTION isomer._sync_incremental_blocker(p_tables regclass[])
RETURNS text LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    t     regclass;
    v_att text;
    v_m   "char";
BEGIN
    FOREACH t IN ARRAY p_tables LOOP
        v_att := isomer._attachment_of(t);
        IF v_att IS NOT NULL THEN
            RETURN format('%s is attached from another database (attachment %s); sync needs IsomerDB to be the primary. Cut over first (isomer.cutover).', t, v_att);
        END IF;
        v_m := (isomer._distribution(t)).method;
        -- A table Citus manages on the coordinator alone (one shard, here) is written here.
        IF v_m = 'n' AND NOT EXISTS (SELECT 1 FROM pg_dist_shard sh JOIN pg_dist_placement pl USING (shardid)
                                     WHERE sh.logicalrelid = t AND pl.groupid <> 0) THEN
            v_m := NULL;
        END IF;
        IF v_m IS NOT NULL THEN
            RETURN format('%s is a %s table: a statement across its shards runs their triggers over parallel connections that can''t take turns on one state, and on a cluster its changes reach the state only after they commit. Use freshness => ''bounded'' (read-your-writes via isomer.wait_for).',
                          t, CASE v_m WHEN 'n' THEN 'reference' ELSE 'sharded' END);
        END IF;
    END LOOP;
    RETURN NULL;
END $$;

-- Fold every delta of a sync incremental projection and apply the keys they reach, in the
-- writing transaction. Folds of one projection are serialised (isomer._engine_fold's lock, held
-- to the end of the transaction), so writers to its tables take turns from their first write to
-- commit; each sees the state as the writers before it left it.
CREATE FUNCTION isomer._sync_fold(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_keys text[];
BEGIN
    LOOP
        EXIT WHEN isomer._engine_fold(p_projection) = 0;
    END LOOP;
    WITH taken AS (DELETE FROM isomer.pending WHERE projection = p_projection RETURNING key)
    SELECT array_agg(key) INTO v_keys FROM taken;
    PERFORM isomer._apply(p_projection, v_keys);
    UPDATE isomer.projection_stats SET last_applied_at = clock_timestamp() WHERE projection = p_projection;
END $$;

-- After each statement on a source of a sync incremental projection (the row triggers have
-- recorded its deltas by then).
--   arguments: projection
CREATE FUNCTION isomer._on_source_statement()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM isomer.projections WHERE name = TG_ARGV[0]) THEN
        PERFORM isomer._sync_fold(TG_ARGV[0]);
    END IF;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION isomer._on_source_delta()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    v_projection text := TG_ARGV[0];
    v_source     text := TG_ARGV[1];
    v_columns    text[] := nullif(TG_ARGV[3], '')::text[];
    v_events     boolean := TG_ARGV[4] = 'e';
    v_token      bigint := isomer.current_token();
    v_old        jsonb;
    v_new        jsonb;
BEGIN
    PERFORM set_config('isomer.last_write_token', v_token::text, false);
    IF TG_ARGV[2] = 'r' AND NOT isomer._first_placement(TG_TABLE_NAME) THEN
        RETURN NULL;
    END IF;
    IF TG_OP <> 'TRUNCATE' THEN
        IF TG_OP <> 'INSERT' THEN v_old := to_jsonb(OLD); END IF;
        IF TG_OP <> 'DELETE' THEN v_new := to_jsonb(NEW); END IF;
        IF TG_OP = 'UPDATE' AND v_columns IS NOT NULL AND isomer._pick(v_old, v_columns) = isomer._pick(v_new, v_columns) THEN
            RETURN NULL;
        END IF;
    ELSIF v_events THEN
        RETURN NULL;   -- a truncate is not a row change: no events
    END IF;
    IF EXISTS (SELECT 1 FROM isomer.projections WHERE name = v_projection) THEN
        IF v_events THEN
            INSERT INTO isomer.deltas (projection, source, weight, image, token)
            VALUES (v_projection, v_source, 1, jsonb_build_object('old', v_old, 'new', v_new), v_token);
        ELSE
            INSERT INTO isomer.deltas (projection, source, weight, image, token)
            SELECT v_projection, v_source, d.w, d.image, v_token
            FROM (VALUES (-1, v_old), (1, v_new), (0, NULL)) d(w, image)
            WHERE (TG_OP = 'TRUNCATE') = (d.w = 0) AND (d.w = 0 OR d.image IS NOT NULL);
        END IF;
    ELSE
        IF v_events THEN
            INSERT INTO isomer.node_deltas (projection, source, weight, image, token)
            VALUES (v_projection, v_source, 1, jsonb_build_object('old', v_old, 'new', v_new), v_token);
        ELSE
            INSERT INTO isomer.node_deltas (projection, source, weight, image, token)
            SELECT v_projection, v_source, d.w, d.image, v_token
            FROM (VALUES (-1, v_old), (1, v_new), (0, NULL)) d(w, image)
            WHERE (TG_OP = 'TRUNCATE') = (d.w = 0) AND (d.w = 0 OR d.image IS NOT NULL);
        END IF;
    END IF;
    -- A sync incremental projection rebuilds now, in the truncating transaction.
    IF TG_OP = 'TRUNCATE' AND TG_ARGV[5] = 's' AND EXISTS (SELECT 1 FROM isomer.projections WHERE name = v_projection) THEN
        PERFORM isomer._sync_fold(v_projection);
    END IF;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION isomer._create_source_triggers(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_unsafe_triggers = on SET citus.enable_ddl_propagation = on AS $$
DECLARE
    p         isomer.projections;
    s         record;
    v_trigger text := format('isomer_%s', p_projection);
    v_plan    jsonb;
    v_args    text;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    IF p.kind = 'analytics' AND p.strategy <> 'incremental' THEN
        RETURN;   -- noticed from table statistics, no triggers
    END IF;
    SELECT e.plan INTO v_plan FROM isomer.engine_plans e WHERE e.projection = p.name;
    FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger, s.source_table);
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger || '_truncate', s.source_table);
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger || '_sync', s.source_table);
        IF p.kind = 'event' OR p.strategy = 'incremental' THEN
            v_args := format('%L, %L, %L, %L, %L, %L', p.name, s.source_table,
                             CASE WHEN (isomer._distribution(s.source_table::regclass)).method = 'n' THEN 'r' ELSE '' END,
                             CASE WHEN p.kind = 'event' THEN '' ELSE
                                 ARRAY(SELECT jsonb_array_elements_text(v_plan -> 'capture' -> (s.source_table::regclass::oid)::text))::text END,
                             CASE WHEN p.kind = 'event' THEN 'e' ELSE '' END,
                             CASE WHEN p.freshness = 'sync' THEN 's' ELSE '' END);
            EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW '
                           'EXECUTE FUNCTION isomer._on_source_delta(%s)', v_trigger, s.source_table, v_args);
            EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT '
                           'EXECUTE FUNCTION isomer._on_source_delta(%s)', v_trigger || '_truncate', s.source_table, v_args);
            -- Sync: each statement's deltas are folded, and the keys they reach applied, before
            -- the statement returns.
            IF p.strategy = 'incremental' AND p.freshness = 'sync' THEN
                EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH STATEMENT '
                               'EXECUTE FUNCTION isomer._on_source_statement(%L)', v_trigger || '_sync', s.source_table, p.name);
                EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %I', s.source_table, v_trigger || '_sync');
            END IF;
        ELSE
            EXECUTE format(
                'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW '
                'EXECUTE FUNCTION isomer._on_source_change(%L, %L, %L)',
                v_trigger, s.source_table, p.name, coalesce(s.key_column, ''), p.freshness);
            EXECUTE format(
                'CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT '
                'EXECUTE FUNCTION isomer._on_source_change(%L, %L, %L)',
                v_trigger || '_truncate', s.source_table, p.name, coalesce(s.key_column, ''), p.freshness);
        END IF;
        -- Fire for changes applied by logical replication too (attach mode).
        EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %I', s.source_table, v_trigger);
        EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %I', s.source_table, v_trigger || '_truncate');
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION isomer._drop_source_triggers(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_unsafe_triggers = on SET citus.enable_ddl_propagation = on AS $$
DECLARE
    s record;
BEGIN
    FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p_projection LOOP
        IF to_regclass(s.source_table) IS NOT NULL THEN
            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', 'isomer_' || p_projection, s.source_table);
            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', 'isomer_' || p_projection || '_truncate', s.source_table);
            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', 'isomer_' || p_projection || '_sync', s.source_table);
        END IF;
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION isomer.create_projection(
    name          text,
    query         text,
    kind          text,
    key           text,
    freshness     text     DEFAULT 'bounded',
    lag           interval DEFAULT '2 seconds',
    sources       jsonb    DEFAULT NULL,
    database      text     DEFAULT 'isomer',
    index_columns text[]   DEFAULT NULL,
    target_schema text     DEFAULT 'public',
    model         text     DEFAULT NULL,
    strategy      text     DEFAULT 'auto')
-- No SET search_path here: the query and source names resolve with the caller's search path,
-- then get stored schema-qualified. Probe views are local, so Citus must not propagate them.
RETURNS text LANGUAGE plpgsql SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_probe     text := format('pg_temp.isomer_create_%s', pg_backend_pid());
    v_cols      record;
    v_key_type  text;
    v_has_doc   boolean := false;
    v_has_input boolean := false;
    v_has_value boolean := false;
    v_tables    regclass[];
    v_t         regclass;
    v_src       record;
    v_index     text[];
    v_reason    text;
    v_strategy  text := 'recompute';
    v_why       text;
    v_shape     jsonb;
    v_result    text;
    v_diff      bigint;
BEGIN
    IF kind NOT IN ('document', 'search', 'vector', 'cache') THEN
        RAISE EXCEPTION 'isomer: kind must be ''document'', ''search'', ''vector'' or ''cache'', not %', kind;
    END IF;
    IF freshness NOT IN ('sync', 'bounded') THEN
        RAISE EXCEPTION 'isomer: freshness must be ''sync'' or ''bounded'', not %', freshness;
    END IF;
    IF strategy NOT IN ('auto', 'recompute', 'incremental') THEN
        RAISE EXCEPTION 'isomer: strategy must be ''auto'', ''recompute'' or ''incremental'', not %', strategy;
    END IF;
    IF kind = 'vector' THEN
        IF freshness = 'sync' THEN
            RAISE EXCEPTION 'isomer: vector projection % cannot be sync: an embedding call cannot run inside the writing transaction', name
                USING HINT = 'Use freshness => ''bounded'' with a lag budget, e.g. lag => ''30 seconds''.';
        END IF;
        IF model IS NULL THEN
            RAISE EXCEPTION 'isomer: a vector projection needs model => one of %',
                (SELECT string_agg(m.name, ', ') FROM isomer.embedding_models m);
        END IF;
        PERFORM 1 FROM isomer.embedding_models m WHERE m.name = create_projection.model;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'isomer: no embedding model named %', model
                USING HINT = 'Register one with isomer.add_embedding_model().';
        END IF;
    END IF;

    EXECUTE format('CREATE OR REPLACE TEMP VIEW %s AS %s', v_probe, query);
    query := isomer._viewdef(v_probe::regclass);
    EXECUTE format('DROP VIEW %s', v_probe);

    FOR v_cols IN SELECT * FROM isomer._query_columns(query) LOOP
        IF v_cols.column_name = key THEN
            v_key_type := v_cols.column_type;
        END IF;
        IF v_cols.column_name = 'doc' AND v_cols.type_oid IN ('jsonb'::regtype, 'json'::regtype) THEN
            v_has_doc := true;
        END IF;
        IF v_cols.column_name = 'input' AND v_cols.type_oid IN ('text'::regtype, 'varchar'::regtype, 'bpchar'::regtype) THEN
            v_has_input := true;
        END IF;
        IF v_cols.column_name = 'value' THEN
            v_has_value := true;
        END IF;
        IF kind = 'search' AND v_cols.column_name <> key
           AND v_cols.type_oid IN ('text'::regtype, 'varchar'::regtype, 'bpchar'::regtype) THEN
            v_index := v_index || v_cols.column_name;
        END IF;
    END LOOP;
    IF v_key_type IS NULL THEN
        RAISE EXCEPTION 'isomer: the query has no column named % (the projection key)', key;
    END IF;
    IF kind = 'document' AND NOT v_has_doc THEN
        RAISE EXCEPTION 'isomer: a document projection''s query must return a jsonb column named doc'
            USING HINT = 'e.g. SELECT c.id, jsonb_build_object(''name'', c.name, ...) AS doc FROM customers c';
    END IF;
    IF kind = 'vector' AND NOT v_has_input THEN
        RAISE EXCEPTION 'isomer: a vector projection''s query must return a text column named input (the text to embed)'
            USING HINT = 'e.g. SELECT p.sku, p.title || '' '' || p.description AS input FROM products p';
    END IF;
    IF kind = 'cache' AND NOT v_has_value THEN
        RAISE EXCEPTION 'isomer: a cache projection''s query must return a column named value'
            USING HINT = 'e.g. SELECT c.id, count(o.*) AS value FROM customers c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.id';
    END IF;

    v_tables := ARRAY(SELECT isomer._query_tables(query));
    IF cardinality(v_tables) = 0 THEN
        RAISE EXCEPTION 'isomer: the query reads no tables';
    END IF;
    IF kind = 'search' THEN
        FOREACH v_t IN ARRAY v_tables LOOP
            IF freshness = 'sync' AND isomer._attachment_of(v_t) IS NOT NULL THEN
                RAISE EXCEPTION 'isomer: projection % cannot be sync: % is attached from another database (attachment %)',
                    name, v_t, isomer._attachment_of(v_t)
                    USING HINT = 'Use freshness => ''bounded'', or cut over first (isomer.cutover). Sync projections need IsomerDB to be the primary for their tables.';
            END IF;
            IF (isomer._distribution(v_t)).method IS NOT NULL THEN
                RAISE EXCEPTION 'isomer: search projections over Citus distributed tables are not supported yet (%)', v_t;
            END IF;
        END LOOP;
    END IF;

    -- How it is maintained (FR-S7).
    IF kind IN ('document', 'cache') THEN
        IF create_projection.strategy = 'incremental' THEN
            IF freshness = 'sync' THEN
                v_reason := isomer._sync_incremental_blocker(v_tables);
                IF v_reason IS NOT NULL THEN
                    RAISE EXCEPTION 'isomer: projection % cannot be sync and incremental: %', name, v_reason;
                END IF;
            END IF;
            v_shape := isomer._engine_shape(query, key, kind);
            IF NOT (v_shape ->> 'ok')::boolean THEN
                RAISE EXCEPTION 'isomer: projection % cannot be maintained incrementally: %', name, v_shape ->> 'reason'
                    USING HINT = 'Use strategy => ''recompute'' with sources.';
            END IF;
            v_strategy := 'incremental';
            v_why := 'chosen by create_projection(strategy => ''incremental'')';
        ELSIF create_projection.strategy = 'auto' THEN
            SELECT c.strategy, c.reason INTO v_strategy, v_why
            FROM isomer.choose_strategy(query, key, kind, freshness, sources) c;
        ELSE
            v_why := 'chosen by create_projection(strategy => ''recompute'')';
        END IF;
    END IF;

    INSERT INTO isomer.projections (name, kind, query, key_column, key_type, freshness, lag_budget,
                                    mongo_database, target_schema, index_columns, model, strategy, strategy_reason)
    VALUES (name, kind, query, key, v_key_type, freshness, lag,
            CASE WHEN kind = 'document' THEN database END,
            CASE WHEN kind IN ('search', 'vector', 'cache') THEN target_schema END,
            CASE WHEN kind = 'search' THEN key || coalesce(index_columns, v_index) END,
            CASE WHEN kind = 'vector' THEN model END,
            v_strategy, v_why);

    IF kind IN ('document', 'vector', 'cache') THEN
        IF v_strategy = 'incremental' THEN
            -- Every table read is a source; mappings, if given, are kept for reference only.
            INSERT INTO isomer.projection_sources (projection, source_table, key_column)
            SELECT name, isomer._qualified(t), sources ->> isomer._qualified(t) FROM unnest(v_tables) t;
        ELSE
            IF sources IS NULL THEN
                RAISE EXCEPTION 'isomer: a % projection needs sources: which column of each table holds the projection key', kind
                    USING HINT = format('sources => ''{%s}''::jsonb, with null for lookup tables',
                        (SELECT string_agg(format('"%s": "<column>"', t), ', ') FROM unnest(v_tables) t));
            END IF;
            FOR v_src IN SELECT k, v FROM jsonb_each_text(sources) AS e(k, v) LOOP
                INSERT INTO isomer.projection_sources (projection, source_table, key_column)
                VALUES (name, isomer._qualified(v_src.k::regclass), v_src.v);
                IF v_src.v IS NOT NULL AND NOT EXISTS (
                    SELECT 1 FROM pg_attribute WHERE attrelid = v_src.k::regclass AND attname = v_src.v AND NOT attisdropped) THEN
                    RAISE EXCEPTION 'isomer: % has no column %', v_src.k, v_src.v;
                END IF;
            END LOOP;
            FOREACH v_t IN ARRAY v_tables LOOP
                IF NOT EXISTS (SELECT 1 FROM isomer.projection_sources s
                               WHERE s.projection = name AND s.source_table::regclass = v_t) THEN
                    RAISE EXCEPTION 'isomer: the query reads % but sources does not map it', v_t
                        USING HINT = format('Add "%s": "<column holding the key>", or "%s": null if it is a lookup table.', v_t, v_t);
                END IF;
            END LOOP;
        END IF;
        v_reason := CASE WHEN v_strategy = 'incremental' AND freshness = 'sync'
                         THEN isomer._sync_incremental_blocker(v_tables)
                         ELSE isomer._check_sources(name, freshness) END;
        IF v_reason IS NOT NULL THEN
            RAISE EXCEPTION 'isomer: projection % cannot be %: %', name, freshness, v_reason;
        END IF;
    END IF;

    v_result := isomer._install(name);
    IF v_strategy = 'incremental' THEN
        -- The derivation from state must give what the query gives from the tables.
        v_diff := isomer._engine_differences(name);
        IF v_diff > 0 THEN
            RAISE EXCEPTION 'isomer: projection %: maintained incrementally, % row(s) differ from the query', name, v_diff
                USING HINT = 'Please report this. Create it with strategy => ''recompute'' meanwhile.';
        END IF;
        v_result := v_result || ', maintained incrementally';
    END IF;
    RETURN v_result;
END $$;

CREATE OR REPLACE FUNCTION isomer.alter_projection(name text, freshness text DEFAULT NULL, lag interval DEFAULT NULL,
                                        strategy text DEFAULT NULL, sources jsonb DEFAULT NULL)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        isomer.projections;
    v_reason text;
    v_shape  jsonb;
    v_src    record;
BEGIN
    -- NO KEY UPDATE: the auditor's foreign-key checks (KEY SHARE on this row) must not wait on us,
    -- or, holding a lock on a source table we need for the triggers, deadlock with us.
    SELECT * INTO p FROM isomer.projections WHERE projections.name = alter_projection.name FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', name;
    END IF;
    IF strategy IS NOT NULL AND strategy <> p.strategy THEN
        IF strategy NOT IN ('recompute', 'incremental') THEN
            RAISE EXCEPTION 'isomer: strategy must be ''recompute'' or ''incremental'', not %', strategy;
        END IF;
        IF strategy = 'incremental' THEN
            IF coalesce(freshness, p.freshness) = 'sync' THEN
                v_reason := isomer._sync_incremental_blocker(ARRAY(SELECT ps.source_table::regclass FROM isomer.projection_sources ps WHERE ps.projection = p.name));
                IF v_reason IS NOT NULL THEN
                    RAISE EXCEPTION 'isomer: projection % is sync and can''t be incremental: %', p.name, v_reason;
                END IF;
            END IF;
            v_shape := isomer._engine_shape(p.query, p.key_column, p.kind);
            IF NOT (v_shape ->> 'ok')::boolean THEN
                RAISE EXCEPTION 'isomer: projection % cannot be maintained incrementally: %', p.name, v_shape ->> 'reason';
            END IF;
        ELSE
            IF sources IS NOT NULL THEN
                FOR v_src IN SELECT k, v FROM jsonb_each_text(sources) AS e(k, v) LOOP
                    IF v_src.v IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM pg_attribute WHERE attrelid = v_src.k::regclass AND attname = v_src.v AND NOT attisdropped) THEN
                        RAISE EXCEPTION 'isomer: % has no column %', v_src.k, v_src.v;
                    END IF;
                    UPDATE isomer.projection_sources ps SET key_column = v_src.v
                    WHERE ps.projection = p.name AND ps.source_table::regclass = v_src.k::regclass;
                END LOOP;
            END IF;
            -- A sharded table mapped to nothing would re-derive every key from inside every shard's
            -- trigger on every write.
            SELECT string_agg(ps.source_table, ', ') INTO v_reason FROM isomer.projection_sources ps
            WHERE ps.projection = p.name AND ps.key_column IS NULL
              AND (isomer._distribution(ps.source_table::regclass)).method = 'h';
            IF v_reason IS NOT NULL THEN
                RAISE EXCEPTION 'isomer: % cannot be re-derived by key: % % not mapped to the key', p.name, v_reason,
                    CASE WHEN v_reason LIKE '%,%' THEN 'are' ELSE 'is' END
                    USING HINT = 'Pass sources => ''{"table": "column holding the key"}''.';
            END IF;
            v_reason := NULL;
        END IF;
        PERFORM isomer._drop_source_triggers(p.name);
        IF p.strategy = 'incremental' THEN
            PERFORM isomer._engine_drop_state(p.name);
        END IF;
        UPDATE isomer.projections
        SET strategy = alter_projection.strategy, derive_query = NULL,
            strategy_reason = format('set by alter_projection(strategy => %L)', alter_projection.strategy)
        WHERE projections.name = p.name;
        DELETE FROM isomer.deltas WHERE projection = p.name;
        IF strategy = 'incremental' THEN
            PERFORM isomer._engine_compile(p.name);
            PERFORM isomer._create_source_triggers(p.name);
            PERFORM isomer._engine_build(p.name);
        ELSE
            PERFORM isomer._create_source_triggers(p.name);
        END IF;
        PERFORM isomer._rebuild_all(p.name);
        DELETE FROM isomer.pending WHERE projection = p.name;
        SELECT * INTO p FROM isomer.projections WHERE projections.name = alter_projection.name;
    END IF;
    IF freshness IS NOT NULL AND freshness <> p.freshness THEN
        IF p.kind NOT IN ('document', 'cache') THEN
            RAISE EXCEPTION 'isomer: only document and cache projections can change freshness (this is a % projection)', p.kind;
        END IF;
        v_reason := CASE WHEN freshness = 'sync' AND p.strategy = 'incremental'
                         THEN isomer._sync_incremental_blocker(ARRAY(SELECT ps.source_table::regclass FROM isomer.projection_sources ps WHERE ps.projection = p.name))
                         ELSE isomer._check_sources(p.name, freshness) END;
        IF v_reason IS NOT NULL THEN
            RAISE EXCEPTION 'isomer: projection % cannot be %: %', p.name, freshness, v_reason;
        END IF;
        UPDATE isomer.projections SET freshness = alter_projection.freshness WHERE projections.name = p.name;
        PERFORM isomer._create_source_triggers(p.name);
        -- Anything still queued is applied now, so the projection is current as it changes mode.
        IF p.strategy = 'incremental' THEN
            PERFORM isomer._sync_fold(p.name);
        END IF;
        PERFORM isomer._apply(p.name, ARRAY(SELECT key FROM isomer.pending WHERE projection = p.name));
        DELETE FROM isomer.pending WHERE projection = p.name;
    END IF;
    IF lag IS NOT NULL THEN
        UPDATE isomer.projections SET lag_budget = lag WHERE projections.name = p.name;
    END IF;
    SELECT * INTO p FROM isomer.projections WHERE projections.name = alter_projection.name;
    RETURN format('%s: freshness %s, lag budget %s, strategy %s', p.name, p.freshness, p.lag_budget, p.strategy);
END $$;

CREATE OR REPLACE FUNCTION isomer.choose_strategy(query text, key text, kind text DEFAULT 'document',
                                       freshness text DEFAULT 'bounded', sources jsonb DEFAULT NULL)
RETURNS TABLE (strategy text, reason text)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    s         jsonb;
    v_lookups text;
    v_cost    double precision;
    v_kind    text;
    v_min     double precision := coalesce(isomer._setting('strategy.incremental_min_cost')::double precision, 200);
BEGIN
    IF kind NOT IN ('document', 'cache') THEN
        RETURN QUERY SELECT 'recompute', format('%s projections are maintained by key', kind);
        RETURN;
    END IF;
    s := isomer._engine_shape(query, key, kind);
    IF NOT (s ->> 'ok')::boolean THEN
        RETURN QUERY SELECT 'recompute', 'incremental maintenance does not apply: ' || (s ->> 'reason');
        RETURN;
    END IF;
    IF freshness = 'sync' THEN
        v_lookups := isomer._sync_incremental_blocker(ARRAY(SELECT isomer._query_tables(query)));
        IF v_lookups IS NOT NULL THEN
            RETURN QUERY SELECT 'recompute', 'sync projections here are re-derived by key inside the writing transaction: ' || v_lookups;
            RETURN;
        END IF;
        v_lookups := NULL;
    END IF;
    SELECT k.cost, k.kind INTO v_cost, v_kind FROM isomer._key_cost(query, key) k;
    IF v_kind = 'impossible' THEN
        RETURN QUERY SELECT 'incremental', 'the cluster cannot run this query as written (it joins tables that are not co-located in a way Citus can''t plan), so it can''t be re-derived by key; folded incrementally it reads one table at a time';
        RETURN;
    END IF;
    IF sources IS NULL THEN
        RETURN QUERY SELECT 'incremental', 'no key mappings were given, and incremental maintenance needs none: each change is folded into the projection''s state';
        RETURN;
    END IF;
    SELECT string_agg(k, ', ') INTO v_lookups FROM jsonb_each(sources) e(k, v) WHERE jsonb_typeof(v) = 'null';
    IF v_lookups IS NOT NULL THEN
        RETURN QUERY SELECT 'incremental', format('%s reach the key only through a join (lookup tables): re-deriving by key, any change to them would re-derive every key; folded incrementally, a change re-derives only the keys it reaches', v_lookups);
        RETURN;
    END IF;
    IF v_kind = 'repartition' THEN
        RETURN QUERY SELECT 'incremental', 're-deriving one key reshuffles rows between nodes (a repartition join, or whole tables pulled to the coordinator), so its cost grows with the tables; folded incrementally a change reads only its own rows';
    ELSIF v_cost >= v_min THEN
        RETURN QUERY SELECT 'incremental', format('re-deriving one key is estimated at %s (planner cost units, %s; the threshold is %s): folding each change into kept partial aggregates reads only the changed rows', round(v_cost::numeric), CASE v_kind WHEN 'fan-out' THEN 'it asks every shard' ELSE v_kind END, v_min);
    ELSE
        RETURN QUERY SELECT 'recompute', format('re-deriving one key is estimated at %s (planner cost units, %s), under the threshold of %s: key recompute is cheap and keeps no extra state', round(v_cost::numeric), v_kind, v_min);
    END IF;
END $$;

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
BEGIN
    checks := '[]';
    IF plan IS NULL THEN
        rules := '[]';
        assumptions := '[]';
        checks := jsonb_build_array(jsonb_build_object('check', 'compiled', 'passed', false,
            'detail', 'no plan: run isomer.alter_projection(' || quote_literal(p.name) || ', strategy => ''recompute'') then back'));
        RETURN;
    END IF;
    v_fact := (SELECT r ->> 'table' FROM jsonb_array_elements(plan -> 'rtes') r WHERE r ->> 'role' = 'fact');
    rules := jsonb_build_array(
        'capture: each change to a source row is recorded in the writing transaction, on the node holding the row, as row images weighted -1 (old) and +1 (new); a reference table''s change is recorded once, by its first placement',
        'delivery: deltas recorded on a worker reach the coordinator exactly once (taken in a prepared transaction there, committed only after the coordinator has recorded the pull)',
        format('linearity: the conditions on %s alone are applied to each delta row (a filter of a change is the change of the filter)', v_fact),
        format('aggregation below the joins: %s is pre-aggregated by (%s); valid because every join leading away from it lands on a unique key, so each of its rows joins at most one row of the rest',
               v_fact, coalesce((SELECT string_agg(c, ', ') FROM jsonb_array_elements_text(plan -> 'pkeys') c), 'nothing: one group')),
        'integration: count and sum are kept as running sums of the weighted deltas (the change of a sum is the sum of the changes); min and max as integrated multisets of values; avg and bool_and/bool_or from those',
        'distinct: count, sum and avg (DISTINCT c) keep c among the grouped columns, so each partial row is one value of c, present while any row holds it',
        'kept as written: an aggregate that is not decomposable (jsonb_agg, string_agg, ordered aggregates) runs as written over partial rows that are one per row of the fact (its primary key is grouped), so over the same rows as the tables give',
        'joins over integrated state: a key can change only if a delta of one input reaches it through the state of the others, before or after the fold (the change of A join B is dA join B + A join dB + dA join dB), so exactly those keys are re-derived, from the state');
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

    EXECUTE format('SELECT count(*) FROM %s WHERE __n < 0', plan ->> 'p_table') INTO v_n;
    checks := checks || jsonb_build_object('check', 'state', 'passed', v_n = 0,
        'detail', CASE WHEN v_n = 0 THEN 'no group holds a negative count' ELSE format('%s group(s) hold a negative count', v_n) END);

    SELECT clock_timestamp() - min(enqueued_at) INTO v_age FROM isomer.deltas WHERE projection = p.name;
    checks := checks || jsonb_build_object('check', 'deltas flowing', 'passed', v_age IS NULL OR v_age < greatest(p.lag_budget * 10, interval '30 seconds'),
        'detail', CASE WHEN v_age IS NULL THEN 'nothing waiting' ELSE format('oldest waiting %s', date_trunc('second', v_age)) END);
END $$;

-- ── Analytics projections maintained incrementally ─────────────────────────
--
-- An analytics projection whose query the engine can maintain keeps the engine's state, like a
-- document projection, and its rows in two tables: a columnar table (the base) and a heap of the
-- rows written since (the changes), each row tagged with its key, the jsonb array of its grouped
-- output columns. Each key the deltas reach is re-derived from the state into the changes (or
-- marked deleted there); readers see the base with those rows replaced. At most every refresh
-- interval, the two are merged into a new columnar table, without running the query again.

ALTER TABLE isomer.analytics_builds
    ADD COLUMN key_columns  text[],     -- incremental: the grouped output columns that identify a row
    ADD COLUMN refresh      interval,   -- incremental: how often changes are merged into the base
    ADD COLUMN merged_rows  bigint;     -- incremental: changed rows merged at the last merge

-- Recorded definitions, for projections created by something other than create_projection, so
-- dump_projections can write them back.
ALTER TABLE isomer.projections ADD COLUMN definition jsonb;

-- The output columns that identify a row of an analytics query (its grouped outputs; none for a
-- single aggregate row), or why its rows can't be told apart.
CREATE FUNCTION isomer._analytics_keys(p_query text, OUT keys text[], OUT reason text)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a jsonb;
BEGIN
    BEGIN
        a := isomer._engine_analyze(p_query);
    EXCEPTION WHEN OTHERS THEN
        reason := 'the query could not be analysed: ' || SQLERRM;
        RETURN;
    END;
    keys := ARRAY(SELECT t ->> 'name' FROM jsonb_array_elements(a -> 'target') WITH ORDINALITY e(t, n)
                  WHERE (t ->> 'grouped')::boolean ORDER BY n);
    IF cardinality(keys) < jsonb_array_length(a -> 'group') THEN
        reason := 'it groups by something it doesn''t return, so its rows can''t be told apart by their columns';
        keys := NULL;
    ELSIF (SELECT count(DISTINCT k) FROM unnest(keys) k) < cardinality(keys) THEN
        reason := 'two of its grouped columns have the same name';
        keys := NULL;
    END IF;
END $$;

-- The projection's columns as a select list (kept in the plan of an incremental one).
CREATE FUNCTION isomer._analytics_columns(p isomer.projections)
RETURNS text LANGUAGE sql SET search_path = pg_catalog AS $$
    SELECT coalesce(
        (SELECT string_agg(format('%I', c), ', ') FROM isomer.engine_plans e, jsonb_array_elements_text(e.plan -> 'columns') c
         WHERE e.projection = p.name),
        (SELECT string_agg(format('%I', column_name), ', ') FROM isomer._query_columns(p.query)))
$$;

CREATE FUNCTION isomer._analytics_delta(p_projection text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT format('isomer_analytics.%I', p_projection || '_changes')
$$;

CREATE FUNCTION isomer._analytics_base(p_projection text, p_generation integer)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT format('isomer_analytics.%I', p_projection || '_' || p_generation)
$$;

-- Point the projection's view at a base table (and, incremental, its changes).
CREATE FUNCTION isomer._analytics_view(p isomer.projections, p_base text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_cols text := isomer._analytics_columns(p);
BEGIN
    IF p.strategy = 'incremental' THEN
        EXECUTE format('CREATE OR REPLACE VIEW %I.%I AS '
                       'SELECT %s FROM %s b WHERE NOT EXISTS (SELECT 1 FROM %s d WHERE d.__k = b.__k) '
                       'UNION ALL SELECT %s FROM %s d WHERE NOT d.__deleted',
                       p.target_schema, p.name, v_cols, p_base, isomer._analytics_delta(p.name), v_cols, isomer._analytics_delta(p.name));
    ELSE
        EXECUTE format('CREATE OR REPLACE VIEW %I.%I AS SELECT * FROM %s', p.target_schema, p.name, p_base);
    END IF;
END $$;

-- Build an incremental analytics projection from its tables: the engine's state, a base table
-- from the query, no changes. Run with the sources locked (or in the transaction that creates the
-- capture triggers), so the state, the base and the deltas that follow meet without a gap.
--   p_state => false: the state was just rebuilt (after a TRUNCATE); build the rows only.
CREATE FUNCTION isomer._analytics_rebuild(p_projection text, p_state boolean DEFAULT true)
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
    EXECUTE format('CREATE TABLE %s USING columnar AS SELECT %s AS __k, q.* FROM (%s) q', v_base, plan ->> 'key_expr', p.query);
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

-- Merge the changed rows into a new columnar table. Changes being applied wait (the changes table
-- is locked against writes); readers don't.
CREATE FUNCTION isomer._analytics_merge(p_projection text)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p       isomer.projections;
    b       isomer.analytics_builds;
    v_delta text := isomer._analytics_delta(p_projection);
    v_old   text;
    v_new   text;
    v_cols  text;
    v_n     bigint;
    v_rows  bigint;
    v_start timestamptz := clock_timestamp();
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    SELECT * INTO STRICT b FROM isomer.analytics_builds WHERE projection = p.name FOR UPDATE;
    EXECUTE format('LOCK TABLE %s IN EXCLUSIVE MODE', v_delta);
    EXECUTE format('SELECT count(*) FROM %s', v_delta) INTO v_n;
    IF v_n = 0 THEN
        UPDATE isomer.analytics_builds SET built_at = clock_timestamp() WHERE projection = p.name;
        RETURN 'unchanged';
    END IF;
    v_cols := isomer._analytics_columns(p);
    v_old := isomer._analytics_base(p.name, b.generation);
    v_new := isomer._analytics_base(p.name, b.generation + 1);
    EXECUTE format('CREATE TABLE %1$s USING columnar AS '
                   'SELECT __k, %2$s FROM %3$s b WHERE NOT EXISTS (SELECT 1 FROM %4$s d WHERE d.__k = b.__k) '
                   'UNION ALL SELECT __k, %2$s FROM %4$s d WHERE NOT d.__deleted',
                   v_new, v_cols, v_old, v_delta);
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    PERFORM isomer._analytics_view(p, v_new);
    EXECUTE format('DELETE FROM %s', v_delta);
    EXECUTE format('DROP TABLE IF EXISTS %s', v_old);
    UPDATE isomer.analytics_builds
    SET generation = b.generation + 1, built_at = clock_timestamp(), build_ms = extract(epoch FROM clock_timestamp() - v_start) * 1000,
        rows = v_rows, bytes = pg_total_relation_size(v_new::regclass), merged_rows = v_n, last_error = NULL
    WHERE projection = p.name;
    RETURN format('%s changed rows merged: %s rows, %s columnar', v_n, v_rows, pg_size_pretty(pg_total_relation_size(v_new::regclass)));
END $$;

-- Write the rows for these keys, re-derived from the state, to the changes; a key the query no
-- longer returns is marked deleted there.
CREATE FUNCTION isomer._analytics_apply(p_projection text, p_keys text[])
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p         isomer.projections;
    plan      jsonb;
    v_delta   text := isomer._analytics_delta(p_projection);
    v_expr    text;
    v_push    text;
    v_written bigint;
    v_deleted bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    plan := (SELECT e.plan FROM isomer.engine_plans e WHERE e.projection = p.name);
    v_expr := plan ->> 'key_expr';
    -- The keys' columns, as conditions the planner pushes below the derivation's grouping (it
    -- would otherwise derive every row to compare whole keys).
    SELECT string_agg(format(
        '(q.%1$I = ANY (ARRAY(SELECT (k::jsonb ->> %2$s)::%3$s FROM unnest($1) k WHERE k::jsonb -> %2$s <> ''null''::jsonb)) '
        'OR (q.%1$I IS NULL AND EXISTS (SELECT 1 FROM unnest($1) k WHERE k::jsonb -> %2$s = ''null''::jsonb)))',
        c.name, c.n - 1, plan -> 'key_types' ->> (c.n - 1)::int), ' AND ' ORDER BY c.n)
    INTO v_push
    FROM jsonb_array_elements_text(plan -> 'key_columns') WITH ORDINALITY c(name, n);
    EXECUTE format('DELETE FROM %s WHERE __k = ANY ($1)', v_delta) USING p_keys;
    EXECUTE format('INSERT INTO %1$s SELECT %2$s, false, q.* FROM (%3$s) q WHERE %4$s AND %2$s = ANY ($1)',
                   v_delta, v_expr, p.derive_query, coalesce(v_push, 'true')) USING p_keys;
    GET DIAGNOSTICS v_written = ROW_COUNT;
    EXECUTE format('INSERT INTO %1$s (__k, __deleted) SELECT k, true FROM unnest($1) k '
                   'WHERE NOT EXISTS (SELECT 1 FROM %1$s d WHERE d.__k = k) ON CONFLICT (__k) DO NOTHING', v_delta) USING p_keys;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    UPDATE isomer.projection_stats
    SET documents_written = documents_written + v_written, documents_deleted = documents_deleted + v_deleted
    WHERE projection = p.name;
END $$;

DROP FUNCTION isomer.create_analytics_projection(text, text, interval, text);

-- strategy: 'rebuild' runs the query in full into a new columnar table when the sources change
-- (at most once per refresh); 'incremental' folds each change into the engine's state and
-- rewrites only the rows it reaches, merged into a new columnar table at most once per refresh;
-- 'auto' (the default) picks incremental when the query fits and its state is small (no
-- aggregate kept as written, which would keep a copy of every row it reads).
CREATE FUNCTION isomer.create_analytics_projection(
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
        IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_shape -> 'aggs') g WHERE g ->> 'cls' = 'keep') THEN
            v_why := format('rebuilt as a columnar table when its sources change, at most every %s: maintained incrementally it would keep a copy of every row its %s read',
                            refresh, (SELECT string_agg(DISTINCT g ->> 'fn', ', ') FROM jsonb_array_elements(v_shape -> 'aggs') g WHERE g ->> 'cls' = 'keep'));
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

-- The keys a key query reaches, as text: the key column, or (analytics) an expression over q.
CREATE FUNCTION isomer._engine_keys_of(p_sql text, p_key text, p_expr text)
RETURNS text[] LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v text[];
BEGIN
    IF p_expr IS NULL THEN
        RETURN isomer._engine_keys_of(p_sql, p_key);
    END IF;
    EXECUTE format('SELECT array_agg(DISTINCT %s) FROM (%s) q', p_expr, p_sql) INTO v;
    RETURN coalesce(v, '{}');
END $$;

CREATE OR REPLACE FUNCTION isomer._engine_touched(plan jsonb, p_key text, p_relids oid[])
RETURNS text[] LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    k      jsonb;
    v_keys text[] := '{}';
BEGIN
    FOR k IN SELECT * FROM jsonb_array_elements(plan -> 'key_sqls') LOOP
        CONTINUE WHEN NOT ((k ->> 'relid')::oid = ANY (p_relids));
        v_keys := v_keys || ARRAY(SELECT DISTINCT q FROM unnest(isomer._engine_keys_of(k ->> 'sql', p_key, plan ->> 'key_expr')) q);
    END LOOP;
    RETURN v_keys;
END $$;

CREATE OR REPLACE FUNCTION isomer._apply(p_projection text, p_keys text[])
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p          isomer.projections;
    v_query    text;
    v_updates  jsonb;
    v_docs     jsonb;
    v_found    text[];
    v_missing  jsonb;
    v_ok       boolean;
    v_result   text;
    v_written  integer;
    v_deleted  integer;
BEGIN
    IF p_keys IS NULL OR cardinality(p_keys) = 0 THEN
        RETURN;
    END IF;
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    v_query := coalesce(p.derive_query, p.query);
    IF p.kind = 'analytics' THEN
        PERFORM isomer._analytics_apply(p.name, p_keys);
        RETURN;
    END IF;

    IF p.kind = 'cache' THEN
        EXECUTE format(
            $q$WITH src AS (SELECT q.%1$I::text AS key, to_jsonb(q.value) AS value FROM (%2$s) q WHERE q.%1$I = ANY ($1::%3$s[])),
                    up AS (INSERT INTO %4$s AS c (key, value, updated_at) SELECT key, value, clock_timestamp() FROM src
                           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                           WHERE c.value IS DISTINCT FROM EXCLUDED.value RETURNING 1),
                    del AS (DELETE FROM %4$s c WHERE c.key = ANY ($1) AND c.key NOT IN (SELECT key FROM src) RETURNING 1)
               SELECT (SELECT count(*) FROM up)::int, (SELECT count(*) FROM del)::int,
                      (SELECT jsonb_object_agg(key, value) FROM src WHERE $2)$q$,
            p.key_column, v_query, p.key_type, isomer._cache_store(p.name))
        INTO v_written, v_deleted, v_docs USING p_keys, p.history IS NOT NULL;
        UPDATE isomer.projection_stats SET documents_written = documents_written + v_written,
                                           documents_deleted = documents_deleted + v_deleted
        WHERE projection = p.name;
        PERFORM isomer._log_changes(p, p_keys, v_docs);
        RETURN;
    END IF;

    -- In a cluster the collection's shard may live on another node, written in a two-phase
    -- commit. The coordinator commits first and releases the per-key lock while the worker's
    -- COMMIT PREPARED is still on its way, and DocumentDB's update, meeting a row whose newer
    -- version is not yet committed, can match nothing and lose the write. Locking the documents
    -- first waits for any such commit, so the update below reads the latest version.
    IF isomer._multi_node() THEN
        EXECUTE format(
            $q$SELECT 1 FROM %s c
               WHERE c.shard_key_value = %s
                 AND c.object_id OPERATOR(documentdb_core.=) ANY (ARRAY(
                     SELECT jsonb_build_object('', to_jsonb(k::%s))::text::documentdb_core.bson FROM unnest($1::text[]) k))
               FOR UPDATE$q$,
            isomer._collection_table(p.mongo_database, p.name),
            (SELECT collection_id FROM documentdb_api_catalog.collections
             WHERE database_name = p.mongo_database AND collection_name = p.name),
            p.key_type)
        USING p_keys;
    END IF;

    EXECUTE format(
        $q$SELECT coalesce(jsonb_agg(jsonb_build_object(
                     'q', jsonb_build_object('_id', to_jsonb(q.%1$I)),
                     'u', q.doc::jsonb || jsonb_build_object('_id', to_jsonb(q.%1$I)),
                     'upsert', true)), '[]'::jsonb),
                  coalesce(array_agg(q.%1$I::text), '{}'),
                  CASE WHEN $2 THEN jsonb_object_agg(q.%1$I::text, q.doc::jsonb || jsonb_build_object('_id', to_jsonb(q.%1$I))) END
           FROM (%2$s) q
           WHERE q.%1$I = ANY ($1::%3$s[])$q$,
        p.key_column, v_query, p.key_type)
    INTO v_updates, v_found, v_docs
    USING p_keys, p.history IS NOT NULL;

    IF jsonb_array_length(v_updates) > 0 THEN
        SELECT r.p_success, r.p_result::text INTO v_ok, v_result
        FROM documentdb_api.update(
            p.mongo_database,
            jsonb_build_object('update', p.name, 'updates', v_updates, 'ordered', false)::text::documentdb_core.bson) r;
        IF NOT v_ok THEN
            RAISE EXCEPTION 'isomer: writing documents for projection % failed: %', p.name, v_result;
        END IF;
    END IF;

    EXECUTE format(
        $q$SELECT jsonb_agg(to_jsonb(k::%s)) FROM unnest($1::text[]) k WHERE k <> ALL ($2)$q$,
        p.key_type)
    INTO v_missing
    USING p_keys, v_found;

    IF v_missing IS NOT NULL THEN
        SELECT r.p_success, r.p_result::text INTO v_ok, v_result
        FROM documentdb_api.delete(
            p.mongo_database,
            jsonb_build_object('delete', p.name, 'deletes', jsonb_build_array(jsonb_build_object(
                'q', jsonb_build_object('_id', jsonb_build_object('$in', v_missing)),
                'limit', 0)))::text::documentdb_core.bson) r;
        IF NOT v_ok THEN
            RAISE EXCEPTION 'isomer: deleting documents for projection % failed: %', p.name, v_result;
        END IF;
    END IF;

    INSERT INTO isomer.projection_stats AS s (projection, documents_written, documents_deleted)
    VALUES (p.name, cardinality(v_found), coalesce(jsonb_array_length(v_missing), 0))
    ON CONFLICT (projection) DO UPDATE
        SET documents_written = s.documents_written + EXCLUDED.documents_written,
            documents_deleted = s.documents_deleted + EXCLUDED.documents_deleted;

    -- Keys are canonicalised through the key type, as the documents' _id are.
    IF p.history IS NOT NULL THEN
        EXECUTE format('SELECT array_agg(k::%s::text) FROM unnest($1::text[]) k', p.key_type) INTO p_keys USING p_keys;
        PERFORM isomer._log_changes(p, p_keys, v_docs);
    END IF;
END $$;

CREATE OR REPLACE FUNCTION isomer.drain(p_batch integer DEFAULT 500)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        record;
    v_keys   text[];
    v_phases text[];
    v_oldest timestamptz;
    v_token  bigint;
    v_n      integer;
    v_total  integer := 0;
    v_lag_ms double precision;
    v_until  timestamptz;
BEGIN
    -- Queued keys reach pending for every projection, paused ones too: their work waits there.
    FOR p IN SELECT DISTINCT projection AS name FROM isomer.key_log LOOP
        BEGIN
            PERFORM isomer._merge_key_log(p.name);
        EXCEPTION WHEN OTHERS THEN
            PERFORM isomer._log('error', p.name, format('merging queued keys failed: %s', SQLERRM));
        END;
    END LOOP;
    -- Event projections: captured changes become events.
    FOR p IN SELECT name, lag_budget FROM isomer.projections WHERE kind = 'event' AND status = 'active' LOOP
        v_until := clock_timestamp() + greatest(p.lag_budget / 2, interval '200 milliseconds');
        BEGIN
            LOOP
                v_n := isomer._event_fold(p.name);
                v_total := v_total + v_n;
                EXIT WHEN v_n = 0 OR clock_timestamp() > v_until;
            END LOOP;
        EXCEPTION WHEN OTHERS THEN
            PERFORM isomer._log('error', p.name, format('turning changes into events failed: %s', SQLERRM));
        END;
    END LOOP;
    FOR p IN SELECT name, kind, lag_budget, strategy FROM isomer.projections
             WHERE ((kind IN ('document', 'vector', 'cache') AND freshness = 'bounded') OR (kind = 'analytics' AND strategy = 'incremental'))
               AND status = 'active'
    LOOP
        IF p.strategy = 'incremental' THEN
            v_until := clock_timestamp() + greatest(p.lag_budget / 2, interval '200 milliseconds');
            BEGIN
                LOOP
                    EXIT WHEN isomer._engine_fold(p.name) = 0 OR clock_timestamp() > v_until;
                END LOOP;
            EXCEPTION WHEN OTHERS THEN
                PERFORM isomer._log('error', p.name, format('folding changes failed: %s', SQLERRM));
            END;
        END IF;
        LOOP
            BEGIN
                WITH batch AS (
                    DELETE FROM isomer.pending
                    WHERE (projection, key) IN (
                        SELECT projection, key FROM isomer.pending
                        WHERE projection = p.name
                        ORDER BY enqueued_at LIMIT p_batch
                        FOR UPDATE SKIP LOCKED)
                    RETURNING key, enqueued_at, phase, token)
                SELECT array_agg(key), array_agg(DISTINCT phase), min(enqueued_at), max(token), count(*)
                INTO v_keys, v_phases, v_oldest, v_token, v_n FROM batch;

                EXIT WHEN v_n = 0;
                IF p.kind IN ('document', 'cache', 'analytics') THEN
                    PERFORM isomer._apply(p.name, v_keys);
                    v_lag_ms := extract(epoch FROM clock_timestamp() - v_oldest) * 1000;
                    INSERT INTO isomer.projection_stats AS s
                        (projection, last_applied_at, last_batch_size, last_lag_ms, max_lag_ms, budget_breaches)
                    VALUES (p.name, clock_timestamp(), v_n, v_lag_ms, v_lag_ms,
                            (v_lag_ms > extract(epoch FROM p.lag_budget) * 1000)::int)
                    ON CONFLICT (projection) DO UPDATE
                        SET last_applied_at = EXCLUDED.last_applied_at,
                            last_batch_size = EXCLUDED.last_batch_size,
                            last_lag_ms     = EXCLUDED.last_lag_ms,
                            max_lag_ms      = greatest(s.max_lag_ms, EXCLUDED.max_lag_ms),
                            budget_breaches = s.budget_breaches + EXCLUDED.budget_breaches;
                    IF v_lag_ms > extract(epoch FROM p.lag_budget) * 1000 THEN
                        PERFORM isomer._log('warning', p.name,
                            format('lag %s ms exceeded budget %s', round(v_lag_ms::numeric), p.lag_budget));
                    END IF;
                ELSE
                    PERFORM isomer._enqueue_embeddings(p.name, v_keys,
                        CASE WHEN 'backfill' = ANY (v_phases) THEN 'backfill' ELSE 'steady' END);
                    UPDATE isomer.embed_jobs SET (enqueued_at, token) = (least(enqueued_at, v_oldest), greatest(embed_jobs.token, v_token))
                    WHERE projection = p.name AND key = ANY (v_keys);
                END IF;
                v_total := v_total + v_n;
            EXCEPTION WHEN OTHERS THEN
                PERFORM isomer._log('error', p.name, format('drain failed: %s', SQLERRM));
                EXIT;
            END;
        END LOOP;
        IF p.kind = 'vector' THEN
            PERFORM isomer._maybe_swap_model(p.name);
        END IF;
    END LOOP;
    RETURN v_total;
END $$;

CREATE OR REPLACE FUNCTION isomer.refresh_analytics(p_projection text, force boolean DEFAULT false)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p       isomer.projections;
    b       isomer.analytics_builds;
    v_fp    text;
    v_table text;
    v_start timestamptz := clock_timestamp();
    v_rows  bigint;
    v_bytes bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    -- Incremental: merge the changed rows (or, forced, build everything again from the tables).
    IF p.strategy = 'incremental' THEN
        RETURN CASE WHEN force THEN isomer._analytics_rebuild(p.name, true) ELSE isomer._analytics_merge(p.name) END;
    END IF;
    SELECT * INTO STRICT b FROM isomer.analytics_builds WHERE projection = p.name FOR UPDATE;
    v_fp := isomer._sources_fingerprint(p.name);
    IF NOT force AND b.fingerprint = v_fp THEN
        RETURN 'unchanged';
    END IF;
    v_table := format('isomer_analytics.%I', p.name || '_' || (b.generation + 1));
    EXECUTE format('CREATE TABLE %s USING columnar AS %s', v_table, p.query);
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
    EXECUTE format('CREATE VIEW %I.%I AS SELECT * FROM %s', p.target_schema, p.name, v_table);
    IF b.generation > 0 THEN
        EXECUTE format('DROP TABLE IF EXISTS isomer_analytics.%I', p.name || '_' || b.generation);
    END IF;
    v_bytes := pg_total_relation_size(v_table::regclass);
    UPDATE isomer.analytics_builds
    SET generation = b.generation + 1, fingerprint = v_fp, built_at = clock_timestamp(),
        build_ms = extract(epoch FROM clock_timestamp() - v_start) * 1000, rows = v_rows, bytes = v_bytes, last_error = NULL
    WHERE projection = p.name;
    UPDATE isomer.projection_stats
    SET last_applied_at = clock_timestamp(), documents_written = documents_written + v_rows, last_batch_size = v_rows
    WHERE projection = p.name;
    RETURN format('%s rows, %s columnar', v_rows, pg_size_pretty(v_bytes));
END $$;

CREATE OR REPLACE FUNCTION isomer.refresh_analytics_due()
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p   record;
    v_n integer := 0;
BEGIN
    FOR p IN SELECT pr.name FROM isomer.projections pr JOIN isomer.analytics_builds b ON b.projection = pr.name
             WHERE pr.kind = 'analytics' AND pr.status = 'active'
               AND (b.built_at IS NULL OR b.built_at < clock_timestamp() - coalesce(b.refresh, pr.lag_budget)) LOOP
        BEGIN
            IF isomer.refresh_analytics(p.name) <> 'unchanged' THEN
                v_n := v_n + 1;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            UPDATE isomer.analytics_builds SET last_error = SQLERRM WHERE projection = p.name;
            PERFORM isomer._log('error', p.name, format('analytics refresh failed: %s', SQLERRM));
        END;
    END LOOP;
    RETURN v_n;
END $$;

-- Deltas of a projection still waiting on the workers.
CREATE FUNCTION isomer._analytics_on_workers(p_projection text)
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NOT isomer._multi_node() THEN
        RETURN 0;
    END IF;
    RETURN (SELECT coalesce(sum(result::bigint), 0) FROM run_command_on_workers(
        format('SELECT count(*) FROM isomer.node_deltas WHERE projection = %L', p_projection)) WHERE success);
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
            p.query, p.target_schema, p.name, p.name) INTO v;
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
            PERFORM documentdb_api.drop_collection(p.mongo_database, p.name);
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

CREATE OR REPLACE FUNCTION isomer.certificate(p_projection text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        isomer.projections;
    v_checks jsonb := '[]';
    v_rules  jsonb;
    v_assume jsonb;
    v_ok     boolean;
    v_detail text;
    v_reason text;
    s        record;
    v_tables regclass[];
    st       record;
    v_n      bigint;
    v_dist   boolean;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;

    IF p.kind = 'event' THEN
        v_rules := jsonb_build_array(
            'capture: each change to a source row is recorded with its old and new images, in the writing transaction, on the node holding the row',
            'delivery: changes recorded on a worker reach the coordinator exactly once (a prepared transaction on the worker, committed after the coordinator records the pull)',
            'derivation: each change is turned into the events whose conditions hold for its old and new rows, and appended to the feed under the feed lock, so ids follow commit order');
        v_assume := jsonb_build_array(
            'the capture trigger is on every placement of the source, ENABLE ALWAYS',
            'changes to one row are on one node, so its events keep their order; events of different rows written on different nodes are ordered as they are pulled',
            format('events are kept for %s', p.history));
        FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
            v_dist := (isomer._distribution(s.source_table::regclass)).method IN ('h', 'n');
            v_ok := CASE WHEN v_dist THEN isomer._triggers_on_all_placements(s.source_table::regclass, p.name)
                         ELSE EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = s.source_table::regclass AND tgname = 'isomer_' || p.name AND tgenabled = 'A') END;
            v_checks := v_checks || jsonb_build_object('check', 'capture on ' || s.source_table, 'passed', v_ok,
                'detail', CASE WHEN v_ok THEN 'present, ENABLE ALWAYS' ELSE 'missing or disabled: run isomer._create_source_triggers(' || quote_literal(p.name) || ')' END);
        END LOOP;
    ELSIF p.kind = 'analytics' AND p.strategy <> 'incremental' THEN
        v_rules := jsonb_build_array(
            'recompute: the query is run in full into a new columnar table, which replaces the old one in one transaction',
            format('when: the sources'' statistics (rows inserted, updated, deleted, and their files) have changed since the last build, checked every 30 seconds, at most once per %s', p.lag_budget));
        v_assume := jsonb_build_array('table statistics count every change (they do for SQL writes, replication and TRUNCATE alike)');
        SELECT * INTO st FROM isomer.analytics_builds WHERE projection = p.name;
        v_checks := v_checks || jsonb_build_object('check', 'last build', 'passed', st.last_error IS NULL AND st.built_at IS NOT NULL,
            'detail', coalesce('failed: ' || st.last_error, format('%s rows, built %s', st.rows, st.built_at)));
        v_n := isomer.analytics_differences(p.name);
        v_checks := v_checks || jsonb_build_object('check', 'equals the query', 'passed', coalesce(v_n = 0, true),
            'detail', CASE WHEN v_n IS NULL THEN 'the sources changed since the last build; checked again after the next'
                           WHEN v_n = 0 THEN 'the columnar table holds exactly what the query returns'
                           ELSE format('%s row(s) differ', v_n) END);
    ELSIF p.strategy = 'incremental' THEN
        SELECT c.rules, c.assumptions, c.checks INTO v_rules, v_assume, v_checks FROM isomer._engine_certificate(p) c;
        IF p.kind = 'analytics' THEN
            v_rules := v_rules || to_jsonb(format('rows: each key the deltas reach is re-derived from the state into a table of changed rows (or marked deleted there); readers see the columnar table with those rows replaced; at most every %s the two are merged into a new columnar table, without running the query again',
                                                  (SELECT refresh FROM isomer.analytics_builds WHERE projection = p.name)));
            v_n := isomer.analytics_differences(p.name);
            v_checks := v_checks || jsonb_build_object('check', 'equals the query', 'passed', coalesce(v_n = 0, true),
                'detail', CASE WHEN v_n IS NULL THEN 'changes are in flight; checked again once they are applied'
                               WHEN v_n = 0 THEN 'the columnar table with its changed rows holds exactly what the query returns'
                               ELSE format('%s row(s) differ', v_n) END);
        END IF;
    ELSIF p.kind IN ('document', 'cache', 'vector') THEN
        v_rules := jsonb_build_array(
            'key-recompute: a write to a source row marks the projection keys it maps to (old and new values of the mapped column)',
            CASE WHEN p.kind = 'vector'
                 THEN 'for each marked key the text is re-derived with the projection''s own query; an embedding is requested only if md5(text) changed, and written only if the job''s hash still matches'
                 ELSE 'for each marked key the value is re-derived with the projection''s own query and written; keys the query no longer produces are deleted' END,
            'so the stored value for a key equals recomputation from the tables at the time it was applied');
        v_assume := jsonb_build_array(
            'every relation the query reads is a mapped source (completeness)',
            'each mapped column holds the projection key for that row, or the table is a lookup table (null mapping: any change re-derives every key)',
            CASE WHEN p.freshness = 'sync' THEN 'sync: the triggers run in the writing transaction, on the shard holding every row the key depends on (co-location), under a per-key lock'
                 ELSE format('bounded: queued keys (including those queued on worker nodes) are applied within the lag budget (%s); read-your-writes via isomer.wait_for', p.lag_budget) END);

        v_tables := ARRAY(SELECT isomer._query_tables(p.query));
        SELECT bool_and(EXISTS (SELECT 1 FROM isomer.projection_sources ps WHERE ps.projection = p.name AND ps.source_table::regclass = t)),
               string_agg(t::text, ', ') FILTER (WHERE NOT EXISTS (SELECT 1 FROM isomer.projection_sources ps WHERE ps.projection = p.name AND ps.source_table::regclass = t))
        INTO v_ok, v_detail FROM unnest(v_tables) t;
        v_checks := v_checks || jsonb_build_object('check', 'completeness', 'passed', coalesce(v_ok, false),
            'detail', CASE WHEN v_ok THEN format('%s relation(s) read, all mapped', cardinality(v_tables)) ELSE 'unmapped: ' || v_detail END);

        FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
            v_dist := (isomer._distribution(s.source_table::regclass)).method = 'h';
            IF v_dist THEN
                v_ok := isomer._triggers_on_all_placements(s.source_table::regclass, p.name);
            ELSE
                v_ok := EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = s.source_table::regclass
                                AND tgname = 'isomer_' || p.name AND tgenabled = 'A');
            END IF;
            v_checks := v_checks || jsonb_build_object('check', 'capture on ' || s.source_table, 'passed', v_ok,
                'detail', CASE WHEN v_ok THEN 'row and truncate triggers present, ENABLE ALWAYS' || CASE WHEN v_dist THEN ', on every shard placement on every node' ELSE '' END
                               ELSE 'missing or disabled: run isomer._create_source_triggers(' || quote_literal(p.name) || ')' END);
        END LOOP;

        IF p.freshness = 'sync' THEN
            v_reason := isomer._check_sources(p.name, 'sync');
            v_checks := v_checks || jsonb_build_object('check', 'sync preconditions', 'passed', v_reason IS NULL,
                'detail', coalesce(v_reason, 'IsomerDB is primary for every source and distributed sources are co-located on the key'));
        END IF;
    ELSE
        SELECT * INTO st FROM pgtrickle.pgt_stream_tables WHERE pgt_schema = p.target_schema AND pgt_name = p.name;
        v_rules := jsonb_build_array(
            format('pg_trickle %s maintenance: %s', coalesce(st.effective_refresh_mode, st.refresh_mode),
                   CASE WHEN coalesce(st.effective_refresh_mode, st.refresh_mode) = 'DIFFERENTIAL'
                        THEN 'changes are captured per source row and the query''s delta is applied (DBSP-style incremental rules)'
                        WHEN coalesce(st.effective_refresh_mode, st.refresh_mode) = 'IMMEDIATE'
                        THEN 'the delta is applied inside the writing transaction'
                        ELSE 'the query is recomputed in full each refresh' END),
            'BM25 index maintained by pg_search on the stream table');
        v_assume := jsonb_build_array('change capture fires for every write to every source, including replicated ones (CDC triggers ENABLE ALWAYS, row mode)');
        SELECT bool_and(t.tgenabled = 'A'), count(*) INTO v_ok, v_n
        FROM pgtrickle.pgt_dependencies d JOIN pg_trigger t ON t.tgrelid = d.source_relid AND t.tgname LIKE 'pg_trickle_cdc_%'
        WHERE d.pgt_id = st.pgt_id;
        v_checks := v_checks || jsonb_build_object('check', 'capture on sources', 'passed', coalesce(v_ok, false),
            'detail', CASE WHEN v_ok THEN 'pg_trickle CDC triggers present and ENABLE ALWAYS' ELSE 'CDC triggers missing or not ENABLE ALWAYS' END);
        v_checks := v_checks || jsonb_build_object('check', 'stream table healthy', 'passed', coalesce(st.status = 'ACTIVE', false),
            'detail', format('status %s', coalesce(st.status, 'missing')));
    END IF;

    SELECT * INTO st FROM isomer.audit_stats WHERE projection = p.name;
    RETURN jsonb_build_object(
        'projection', p.name, 'kind', p.kind, 'freshness', p.freshness, 'strategy', p.strategy,
        'strategy_reason', p.strategy_reason,
        'valid', NOT EXISTS (SELECT 1 FROM jsonb_array_elements(v_checks) c WHERE NOT (c ->> 'passed')::boolean),
        'rules', v_rules, 'assumptions', v_assume, 'checks', v_checks,
        'auditor', jsonb_build_object('checks', coalesce(st.checks, 0), 'drift', coalesce(st.drift, 0), 'since', st.since),
        'issued_at', now());
END $$;

CREATE OR REPLACE FUNCTION isomer._full_refresh(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    IF p.kind IN ('document', 'cache') THEN
        PERFORM isomer._rebuild_all(p.name);
    ELSIF p.kind = 'analytics' AND p.strategy = 'incremental' THEN
        PERFORM isomer._analytics_rebuild(p.name, false);   -- the state was just rebuilt
    ELSIF p.kind = 'vector' THEN
        EXECUTE format('DELETE FROM %s WHERE key NOT IN (SELECT isomer._all_keys(%L))',
                       isomer._vector_store(p.name), p.name);
        DELETE FROM isomer.embed_jobs j WHERE j.projection = p.name
            AND j.key NOT IN (SELECT isomer._all_keys(p.name));
        INSERT INTO isomer.pending (projection, key, token)
        SELECT p.name, k, isomer.current_token() FROM isomer._all_keys(p.name) k ON CONFLICT DO NOTHING;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION isomer.create_event_projection(
    name      text,
    source    regclass,
    events    jsonb,
    key       text DEFAULT NULL,
    retention interval DEFAULT '7 days')
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_src    text := isomer._qualified(source);
    v_pk     text[] := isomer._primary_key(source);
    v_key    text;
    v_union  text;
    v_sql    text;
    v_probe  text;
BEGIN
    IF jsonb_typeof(events) <> 'object' OR events = '{}' THEN
        RAISE EXCEPTION 'isomer: events must be an object of event types, e.g. {"order_paid": {"when": "new.status = ''paid''"}}';
    END IF;
    v_key := coalesce(key, CASE WHEN cardinality(v_pk) = 1 THEN format('coalesce(new.%1$I, old.%1$I)', v_pk[1]) END);
    IF v_key IS NULL THEN
        RAISE EXCEPTION 'isomer: % has no single-column primary key; give key => an expression over old and new', source;
    END IF;
    SELECT string_agg(format('SELECT %L::text AS type, (%s)::jsonb AS payload WHERE %s',
                             e.type,
                             -- A missing row reads as a row of nulls, which IS NULL.
                             coalesce(e.def ->> 'payload', 'to_jsonb(CASE WHEN new IS NULL THEN old ELSE new END)'),
                             coalesce(e.def ->> 'when', 'true')), ' UNION ALL ' ORDER BY e.type)
    INTO v_union FROM jsonb_each(events) e(type, def);
    -- One statement turns a batch of captured changes (old and new images) into events.
    v_sql := format(
        $q$SELECT d.id, d.token, ev.type, (%1$s)::text AS key, ev.payload
           FROM unnest($1::isomer.deltas[]) d
           CROSS JOIN LATERAL jsonb_populate_record(NULL::%2$s, CASE WHEN jsonb_typeof(d.image -> 'old') = 'object' THEN d.image -> 'old' END) AS old
           CROSS JOIN LATERAL jsonb_populate_record(NULL::%2$s, CASE WHEN jsonb_typeof(d.image -> 'new') = 'object' THEN d.image -> 'new' END) AS new
           CROSS JOIN LATERAL (%3$s) ev
           ORDER BY d.id, ev.type$q$,
        v_key, v_src, v_union);
    -- Check it compiles now, not at the first write.
    v_probe := format('SELECT * FROM (%s) s LIMIT 0', replace(v_sql, '$1::isomer.deltas[]', 'ARRAY[]::isomer.deltas[]'));
    BEGIN
        EXECUTE v_probe;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'isomer: the event rules for % do not compile: %', name, SQLERRM
            USING HINT = 'Each "when" is a condition and each "payload" a jsonb expression, over old and new.';
    END;

    INSERT INTO isomer.projections (name, kind, query, key_column, key_type, freshness, lag_budget, history, history_since, strategy, strategy_reason, definition)
    VALUES (name, 'event', v_sql, 'key', 'text', 'bounded', interval '2 seconds', retention, clock_timestamp(),
            'incremental', 'events are derived from each captured change',
            jsonb_build_object('source', v_src, 'events', events, 'key', key));
    INSERT INTO isomer.projection_sources (projection, source_table, key_column) VALUES (name, v_src, NULL);
    INSERT INTO isomer.projection_stats (projection) VALUES (name) ON CONFLICT DO NOTHING;
    PERFORM isomer._create_source_triggers(name);
    RETURN format('event projection %s: %s event type(s) from %s, kept %s; read with isomer.changes_since(%L, cursor)',
                  name, (SELECT count(*) FROM jsonb_object_keys(events)), v_src, retention, name);
END $$;

-- Projections as the statements that create them again: analytics and event projections through
-- their own functions (an event projection created before 0.21.0 kept only its compiled form, so
-- it is listed as a comment).
CREATE OR REPLACE FUNCTION isomer.dump_projections()
RETURNS SETOF text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT CASE p.kind
        WHEN 'analytics' THEN
            format('SELECT isomer.create_analytics_projection(name => %L, query => %L, refresh => %L, target_schema => %L, strategy => %L);',
                   p.name, p.query, coalesce((p.definition ->> 'refresh')::interval, p.lag_budget), coalesce(p.target_schema, 'public'),
                   CASE p.strategy WHEN 'incremental' THEN 'incremental' ELSE 'rebuild' END)
        WHEN 'event' THEN
            CASE WHEN p.definition IS NULL
                 THEN format('-- event projection %s was created before 0.21.0 and its rules were not recorded: recreate it with isomer.create_event_projection', p.name)
                 ELSE format('SELECT isomer.create_event_projection(name => %L, source => %L, events => %L, key => %L, retention => %L);',
                             p.name, p.definition ->> 'source', p.definition -> 'events', p.definition ->> 'key', p.history) END
        ELSE
            format('SELECT isomer.create_projection(name => %L, query => %L, kind => %L, key => %L, '
                   'freshness => %L, lag => %L, sources => %L, database => %L, index_columns => %L, target_schema => %L, model => %L, strategy => %L);',
                   p.name, p.query, p.kind, p.key_column, p.freshness, p.lag_budget,
                   (SELECT jsonb_object_agg(s.source_table, s.key_column) FROM isomer.projection_sources s WHERE s.projection = p.name),
                   coalesce(p.mongo_database, 'isomer'), p.index_columns, coalesce(p.target_schema, 'public'), p.model, p.strategy) END
    FROM isomer.projections p ORDER BY p.created_at
$$;

REVOKE EXECUTE ON FUNCTION isomer.create_analytics_projection(text, text, interval, text, text),
                           isomer._analytics_keys(text), isomer._analytics_columns(isomer.projections),
                           isomer._analytics_view(isomer.projections, text), isomer._analytics_rebuild(text, boolean),
                           isomer._analytics_merge(text), isomer._analytics_apply(text, text[]), isomer._analytics_on_workers(text),
                           isomer._engine_keys_of(text, text, text), isomer._sync_incremental_blocker(regclass[]),
                           isomer._sync_fold(text), isomer._on_source_statement()
    FROM PUBLIC;
