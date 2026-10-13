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
    IF p.kind = 'analytics' THEN
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
