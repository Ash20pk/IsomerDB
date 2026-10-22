-- IsomerDB 0.24.0: the state of a query with subqueries distributed across the cluster (PRD FR-P6).
--
--   * state => 'distributed' for a projection with subqueries. A cluster runs a correlated
--     subquery shard by shard when the outer rows and the rows it reads are co-located, sharded by
--     the two sides of its correlation. So the query's relation is sharded by the column its
--     subqueries are correlated with (c.id); each subquery correlated with that column by equality
--     keeps its state, partial aggregates or a copy of its table, sharded by its own side
--     (o.customer_id), co-located; the tables read any other way (a lookup of the customer's tier)
--     are reference tables on every node, and a subquery correlated with nothing outside it keeps
--     its partial aggregates sharded by group. A key is derived by the query filtered by the key.
--   * Not distributed, with the reason given: a subquery that aggregates and is correlated through
--     another column (an order's lines inside a customer's document: the lines are correlated
--     with the order, not with the customer the orders are sharded by), a subquery reading a row
--     two levels up, a table read through different columns by different subqueries.
--   * Staging tables for changed rows go where their table's state goes, since the keys a change
--     reaches are found by running the subqueries over them.
--   * alter_projection refuses to change an analytics projection's strategy or state: it rebuilt
--     one as documents, and failed half way (since 0.21.0). Its strategy is chosen when it is
--     created, and its state stays on the coordinator.
--   * drop_projection waits for a fold of the projection under way: the two deadlocked, now and
--     then, over the projection's deltas.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.24.0'" to load this file. \quit

-- What the engine needs to know about a query (see 0.23.0); each block of a query with subqueries
-- now also says how it is correlated with its parent's row (by equality, from _engine_blocks),
-- whether it reads a row more than one level up, and whether it reads an outer row at all. The
-- shape says which kind of projection it is for.
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
        RETURN isomer._engine_block_shape(a, p_key, p_kind, 'query') || jsonb_build_object('kind', p_kind);
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
                                                                           'type', b ->> 'type', 'place', b ->> 'place',
                                                                           'correlation', coalesce(b -> 'correlation', '[]'),
                                                                           'deep_outer', coalesce(b -> 'deep_outer', 'false'),
                                                                           'outer', b -> 'analysis' -> 'outer'));
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
    RETURN (v_shapes -> 0) || jsonb_build_object('nested', true, 'blocks', v_shapes, 'capture', v_cap, 'dims', v_dims, 'kind', p_kind);
END $$;

-- ── Where each part of a nested state goes ──────────────────────────────────
--
-- A cluster runs a correlated subquery shard by shard only when the outer rows and the rows the
-- subquery reads are co-located, sharded by the two sides of its correlation; a reference table
-- can't be the outer side at all. So the query's own relation is sharded by the column its
-- subqueries are correlated with (c.id), each subquery correlated with that column by equality
-- is sharded by its own side (o.customer_id), and so on down; every other relation is a reference
-- table. Returns {"blocks": [per block: {"rti", "col"} sharded, {"hash": true} an uncorrelated
-- block's partial aggregates, or null], "rels": {relid: column, or null for a reference table}},
-- or {"reason": why it can't be placed}.
CREATE FUNCTION isomer._engine_placement(s jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    v_n      integer := jsonb_array_length(s -> 'blocks');
    v_pl     jsonb;
    v_anchor jsonb;
    v_par    jsonb;
    v_req    jsonb := '{}';
    v_rels   jsonb := '{}';
    v_want   text;
    v_cols   text[];
    v_type   text;
    b        jsonb;
    c        jsonb;
    r        jsonb;
    e        record;
    k        integer;
BEGIN
    -- The column of the query's row its subqueries are correlated with most often.
    SELECT jsonb_build_object('rti', (x.o ->> 'outer_rti')::int, 'col', x.o ->> 'outer_col') INTO v_anchor
    FROM (SELECT jsonb_build_object('outer_rti', cr ->> 'outer_rti', 'outer_col', cr ->> 'outer_col') AS o
          FROM jsonb_array_elements(s -> 'blocks') bl, jsonb_array_elements(coalesce(bl -> 'correlation', '[]')) cr
          WHERE (bl ->> 'parent')::int = 0) x
    GROUP BY x.o ORDER BY count(*) DESC, x.o::text LIMIT 1;
    IF v_anchor IS NULL THEN
        RETURN jsonb_build_object('reason', 'none of its subqueries is correlated with the query''s row by an equality (o.customer_id = c.id), so there is no column to shard its state by');
    END IF;
    b := s -> 'blocks' -> 0;
    IF b ->> 'fact' IS NOT NULL AND ((b ->> 'fact')::int <> (v_anchor ->> 'rti')::int OR NOT (b -> 'pkeys' ? (v_anchor ->> 'col'))) THEN
        RETURN jsonb_build_object('reason', format('it aggregates %s, and its subqueries are correlated with another table''s column',
                                                   (b ->> 'fact_relid')::oid::regclass));
    END IF;
    v_pl := jsonb_build_array(v_anchor);

    -- Each subquery, after its parent (blocks are numbered parents first).
    FOR k IN 1 .. v_n - 1 LOOP
        b := s -> 'blocks' -> k;
        IF (b ->> 'deep_outer')::boolean THEN
            RETURN jsonb_build_object('reason', 'a subquery reads the row of a query more than one level above it');
        END IF;
        v_par := v_pl -> (b ->> 'parent')::int;
        c := NULL;
        IF jsonb_typeof(v_par) = 'object' AND v_par ? 'rti' THEN
            SELECT x INTO c FROM jsonb_array_elements(b -> 'correlation') x
            WHERE (x ->> 'outer_rti')::int = (v_par ->> 'rti')::int AND x ->> 'outer_col' = v_par ->> 'col' LIMIT 1;
        END IF;
        IF c IS NOT NULL THEN
            IF b ->> 'fact' IS NOT NULL AND ((b ->> 'fact')::int <> (c ->> 'rti')::int OR NOT (b -> 'pkeys' ? (c ->> 'col'))) THEN
                RETURN jsonb_build_object('reason', format('a subquery aggregates %s, not the table it is correlated through',
                                                           (b ->> 'fact_relid')::oid::regclass));
            END IF;
            v_pl := v_pl || jsonb_build_array(jsonb_build_object('rti', (c ->> 'rti')::int, 'col', c ->> 'col'));
        ELSIF b ->> 'fact' IS NOT NULL THEN
            IF coalesce((b ->> 'outer')::boolean, true) THEN
                RETURN jsonb_build_object('reason', format('a subquery that aggregates %s is not correlated with its outer row through the column that row is sharded by',
                                                           (b ->> 'fact_relid')::oid::regclass));
            END IF;
            v_pl := v_pl || '[{"hash": true}]'::jsonb;
        ELSE
            v_pl := v_pl || '[null]'::jsonb;
        END IF;
    END LOOP;

    -- Each table, from what every block that reads it needs: sharded by a column, a reference
    -- table (read by a correlated block other than through its shard column), or anything (read
    -- by a block correlated with nothing outside it, which the cluster runs on its own).
    FOR k IN 0 .. v_n - 1 LOOP
        b := s -> 'blocks' -> k;
        v_par := v_pl -> k;
        FOR r IN SELECT * FROM jsonb_array_elements(b -> 'rtes') LOOP
            v_want := CASE WHEN jsonb_typeof(v_par) = 'object' AND v_par ? 'rti' AND (v_par ->> 'rti')::int = (r ->> 'rti')::int THEN v_par ->> 'col'
                           WHEN k > 0 AND NOT coalesce((b ->> 'outer')::boolean, true) THEN '*any'
                           ELSE '*ref' END;
            v_req := jsonb_set(v_req, ARRAY[r ->> 'relid'], coalesce(v_req -> (r ->> 'relid'), '[]') || to_jsonb(v_want));
        END LOOP;
    END LOOP;
    FOR e IN SELECT key, value FROM jsonb_each(v_req) LOOP
        v_cols := ARRAY(SELECT DISTINCT w FROM jsonb_array_elements_text(e.value) w WHERE w NOT LIKE '*%' ORDER BY 1);
        IF cardinality(v_cols) > 1 THEN
            RETURN jsonb_build_object('reason', format('%s is read by subqueries correlated through different columns (%s)',
                                                       e.key::oid::regclass, array_to_string(v_cols, ', ')));
        END IF;
        IF cardinality(v_cols) = 1 AND e.value ? '*ref' THEN
            RETURN jsonb_build_object('reason', format('%s is sharded by %s for one subquery and read through another column in another',
                                                       e.key::oid::regclass, v_cols[1]));
        END IF;
        v_rels := v_rels || jsonb_build_object(e.key, v_cols[1]);
    END LOOP;

    -- Co-located tables are sharded by columns of one type.
    SELECT string_agg(DISTINCT isomer._coltype(key::oid, value #>> '{}'), ', ') INTO v_type
    FROM jsonb_each(v_rels) WHERE jsonb_typeof(value) = 'string';
    IF v_type LIKE '%,%' THEN
        RETURN jsonb_build_object('reason', format('the columns its subqueries are correlated through have different types (%s), and co-located shards need one', v_type));
    END IF;
    RETURN jsonb_build_object('blocks', v_pl, 'rels', v_rels);
END $$;

-- Why an incremental projection's state can't be distributed, or NULL (see 0.22.0): a query with
-- subqueries can be, when its state can be placed.
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
        RETURN isomer._engine_placement(p_shape) ->> 'reason';
    END IF;
    RETURN NULL;
END $$;

-- Make a table's primary key these columns (a sharded table's unique keys hold its shard column).
CREATE FUNCTION isomer._engine_rekey(p_table text, p_cols text[])
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I, ADD PRIMARY KEY (%s)', p_table,
                   (SELECT conname FROM pg_constraint WHERE conrelid = p_table::regclass AND contype = 'p'),
                   (SELECT string_agg(format('%I', c), ', ') FROM unnest(p_cols) c));
END $$;

-- Distribute a compiled nested state (_engine_compile_nested built it on the coordinator) as
-- _engine_placement says: sharded tables co-located, the rest reference tables. Staging tables go
-- where their relation's state goes, since the keys a change reaches are found by running the
-- subqueries over them. Partial aggregates and multisets sharded by a column gain it in their keys.
CREATE FUNCTION isomer._engine_distribute_nested(p isomer.projections, s jsonb, plan jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_pl    jsonb := isomer._engine_placement(s);
    v_first text;
    v_col   text;
    v_src   jsonb;
    v_pk    text[];
    f       jsonb;
    m       jsonb;
    e       record;
    v_i     integer;
    v_bl    jsonb;
BEGIN
    IF v_pl ? 'reason' THEN
        RAISE EXCEPTION 'isomer: the state of projection % can''t be distributed: %', p.name, v_pl ->> 'reason';
    END IF;
    -- The state is built and read again in this transaction: one connection per node.
    PERFORM set_config('citus.multi_shard_modify_mode', 'sequential', true);
    PERFORM set_config('citus.enable_ddl_propagation', 'on', true);

    FOR e IN SELECT key, value FROM jsonb_each(plan -> 'sources') ORDER BY key LOOP
        v_src := e.value;
        v_col := v_pl -> 'rels' ->> e.key;
        IF v_col IS NULL THEN
            PERFORM create_reference_table(v_src ->> 'staging');
        ELSE
            PERFORM create_distributed_table(v_src ->> 'staging', v_col, colocate_with => coalesce(v_first, 'default'));
            v_first := coalesce(v_first, v_src ->> 'staging');
        END IF;
        IF v_src ? 'state' THEN
            IF v_col IS NULL THEN
                PERFORM create_reference_table(v_src ->> 'state');
            ELSE
                IF jsonb_typeof(v_src -> 'pk') = 'array' THEN
                    v_pk := ARRAY(SELECT jsonb_array_elements_text(v_src -> 'pk'));
                    IF NOT (v_col = ANY (v_pk)) THEN
                        v_pk := v_pk || v_col;
                        PERFORM isomer._engine_rekey(v_src ->> 'state', v_pk);
                    END IF;
                    v_src := v_src || jsonb_build_object('pk', to_jsonb(v_pk));
                ELSE
                    PERFORM isomer._engine_rekey(v_src ->> 'state', ARRAY['__h', v_col]);
                    v_src := v_src || jsonb_build_object('conflict', jsonb_build_array('__h', v_col));
                END IF;
                PERFORM create_distributed_table(v_src ->> 'state', v_col, colocate_with => v_first);
            END IF;
            v_src := v_src || jsonb_build_object('replicated', true, 'shard_col', v_col);
        END IF;
        plan := jsonb_set(plan, ARRAY['sources', e.key], v_src);
    END LOOP;

    FOR v_i IN 0 .. jsonb_array_length(plan -> 'facts') - 1 LOOP
        f := plan -> 'facts' -> v_i;
        v_bl := v_pl -> 'blocks' -> (f ->> 'block')::int;
        IF v_bl ? 'col' THEN
            v_col := v_bl ->> 'col';
            PERFORM isomer._engine_rekey(f ->> 'p_table', ARRAY[v_col, '__h']);
            PERFORM create_distributed_table(f ->> 'p_table', v_col, colocate_with => coalesce(v_first, 'default'));
            v_first := coalesce(v_first, f ->> 'p_table');
            FOR m IN SELECT * FROM jsonb_array_elements(f -> 'minmax') LOOP
                EXECUTE format('ALTER TABLE %s ADD COLUMN %I %s', m ->> 'table', v_col, isomer._coltype((f ->> 'fact_relid')::oid, v_col));
                PERFORM isomer._engine_rekey(m ->> 'table', ARRAY[v_col, '__h', 'value']);
                PERFORM create_distributed_table(m ->> 'table', v_col, colocate_with => v_first);
            END LOOP;
            f := f || jsonb_build_object('shard_col', v_col);
        ELSE
            -- A block correlated with nothing outside it: its partial aggregates by group.
            PERFORM create_distributed_table(f ->> 'p_table', '__h', colocate_with => 'none');
            FOR m IN SELECT * FROM jsonb_array_elements(f -> 'minmax') LOOP
                PERFORM create_distributed_table(m ->> 'table', '__h', colocate_with => f ->> 'p_table');
            END LOOP;
        END IF;
        plan := jsonb_set(plan, ARRAY['facts', v_i::text], f);
    END LOOP;
    PERFORM set_config('citus.enable_ddl_propagation', 'off', true);

    plan := plan || jsonb_build_object(
        'distributed', true, 'placement', v_pl,
        'derive_keyed', isomer._engine_sql_blocks(p.query, 'derive_keyed',
                                                  jsonb_set(plan -> 'block_config', '{0,key}', to_jsonb(p.key_column)), 0, NULL));
    UPDATE isomer.engine_plans ep SET plan = $3, compiled_at = clock_timestamp() WHERE ep.projection = p.name;
    RETURN plan;
END $$;

-- Compile (see 0.22.0). A query with subqueries whose state is distributed is compiled on the
-- coordinator first, then distributed.
CREATE OR REPLACE FUNCTION isomer._engine_compile(p_projection text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p     isomer.projections;
    local isomer.projections;
    s     jsonb;
    x     jsonb;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    s := isomer._engine_shape(p.query, p.key_column, p.kind);
    IF NOT coalesce((s ->> 'nested')::boolean, false) THEN
        x := isomer._engine_compile_single(p_projection);
    ELSIF NOT (s ->> 'ok')::boolean THEN
        RAISE EXCEPTION 'isomer: projection % cannot be maintained incrementally: %', p.name, s ->> 'reason';
    ELSE
        PERFORM isomer._engine_drop_state(p.name);
        IF p.state_placement <> 'distributed' THEN
            x := isomer._engine_compile_nested(p, s);
        ELSE
            local := p;
            local.state_placement := 'coordinator';
            x := isomer._engine_distribute_nested(p, s, isomer._engine_compile_nested(local, s));
        END IF;
    END IF;
    RETURN x;
END $$;

-- ── Folding into state sharded by a column ──────────────────────────────────

-- Fold a fact's rows into its partial aggregates (see 0.22.0). Sharded by a column (shard_col),
-- the partial aggregates and the multisets behind min and max are keyed by it as well.
CREATE OR REPLACE FUNCTION isomer._engine_fold_fact(plan jsonb, p_src text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_keys  text := (SELECT string_agg(format('d.%I', c), ', ') FROM jsonb_array_elements_text(plan -> 'pkeys') c);
    v_hash  text := format('md5(jsonb_build_array(%s)::text)', coalesce((SELECT string_agg(format('d.%I', c), ', ') FROM jsonb_array_elements_text(plan -> 'pkeys') c), ''));
    v_cols  text := '__h' || coalesce((SELECT string_agg(format(', %I', c), '') FROM jsonb_array_elements_text(plan -> 'pkeys') c), '') || ', __n';
    v_set   text := '__n = t.__n + EXCLUDED.__n';
    v_shard text := plan ->> 'shard_col';
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
    EXECUTE format('INSERT INTO %1$s AS t (%2$s) %3$s ON CONFLICT (%5$s__h) DO UPDATE SET %4$s',
                   plan ->> 'p_table', v_cols, isomer._engine_p_select(plan, p_src), v_set, CASE WHEN v_shard IS NOT NULL THEN format('%I, ', v_shard) ELSE '' END);
    -- Only the groups this fold reached can have emptied.
    EXECUTE format('DELETE FROM %1$s t WHERE t.__n = 0 AND t.__h IN (SELECT %2$s FROM %3$s d)', plan ->> 'p_table', v_hash, p_src);

    FOR x IN SELECT * FROM jsonb_array_elements(plan -> 'minmax') LOOP
        v_i := (x ->> 'i')::int;
        EXECUTE format(
            'INSERT INTO %1$s AS m (__h, %6$svalue, mult) SELECT %2$s, %7$sd.__a%3$s, sum(d.__w) FROM %4$s d '
            'WHERE d.__f%3$s AND d.__a%3$s IS NOT NULL GROUP BY %5$s d.__a%3$s HAVING sum(d.__w) <> 0 '
            'ON CONFLICT (%6$s__h, value) DO UPDATE SET mult = m.mult + EXCLUDED.mult',
            x ->> 'table', v_hash, v_i, p_src, coalesce(v_keys || ', ', ''),
            CASE WHEN v_shard IS NOT NULL THEN format('%I, ', v_shard) ELSE '' END, CASE WHEN v_shard IS NOT NULL THEN format('d.%I, ', v_shard) ELSE '' END);
        EXECUTE format('DELETE FROM %1$s m WHERE m.mult = 0 AND m.__h IN (SELECT %2$s FROM %3$s d)', x ->> 'table', v_hash, p_src);
        EXECUTE format(
            'UPDATE %1$s t SET __m%2$s = (SELECT %3$s(m.value) FROM %4$s m WHERE m.__h = t.__h%7$s AND m.mult > 0) '
            'WHERE t.__h IN (SELECT %5$s FROM %6$s d)',
            plan ->> 'p_table', v_i, x ->> 'cls', x ->> 'table', v_hash, p_src,
            CASE WHEN v_shard IS NOT NULL THEN format(' AND m.%1$I = t.%1$I', v_shard) ELSE '' END);
    END LOOP;
END $$;

-- Fold a joined table's changed rows into its copy (see 0.22.0). A copy on every node, or sharded
-- by a column, is written from the netted rows as a parameter, and only the rows they reach are
-- checked for having left. A sharded copy without a primary key is keyed by its hash and its shard
-- column (conflict).
CREATE OR REPLACE FUNCTION isomer._engine_fold_dim(src jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_cols  text := (SELECT string_agg(format('%I', c), ', ') FROM jsonb_array_elements_text(src -> 'cols') c);
    v_xcols text := (SELECT string_agg(format('x.%I', c), ', ') FROM jsonb_array_elements_text(src -> 'cols') c);
    v_pk    text := (SELECT string_agg(format('%I', c), ', ') FROM jsonb_array_elements_text(src -> 'pk') c);
    v_spk   text := (SELECT string_agg(format('s.%I', c), ', ') FROM jsonb_array_elements_text(src -> 'pk') c);
    v_xpk   text := (SELECT string_agg(format('x.%I', c), ', ') FROM jsonb_array_elements_text(src -> 'pk') c);
    v_upd   text := (SELECT string_agg(format('%1$I = EXCLUDED.%1$I', c), ', ') FROM jsonb_array_elements_text(src -> 'cols') c
                     WHERE NOT (src -> 'pk' ? c));
    v_conf  text := coalesce((SELECT string_agg(format('%I', c), ', ') FROM jsonb_array_elements_text(src -> 'conflict') c), '__h');
    v_repl  boolean := coalesce((src ->> 'replicated')::boolean, false);
    v_net   text;
    v_rows  jsonb;
BEGIN
    v_net := format('SELECT %s, sum(__w) AS __w FROM %s GROUP BY %s', v_cols, src ->> 'staging', v_cols);
    IF v_repl THEN
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
                       src ->> 'state', v_net, v_spk, v_xpk)
        USING v_rows;
        -- Only rows of this fold's changes can have left.
        EXECUTE format('DELETE FROM %1$s s WHERE s.__w <= 0 AND (%2$s) IN (SELECT %3$s FROM (%4$s) x)',
                       src ->> 'state', v_spk, v_xpk, CASE WHEN v_repl THEN v_net ELSE 'SELECT * FROM ' || (src ->> 'staging') END)
        USING v_rows;
        EXECUTE format('INSERT INTO %1$s AS s (%2$s, __w) SELECT %3$s, x.__w FROM (%4$s) x WHERE x.__w > 0 '
                       'ON CONFLICT (%5$s) DO UPDATE SET %6$s__w = s.__w + EXCLUDED.__w',
                       src ->> 'state', v_cols, v_xcols, v_net, v_pk, coalesce(v_upd || ', ', ''))
        USING v_rows;
    ELSE
        EXECUTE format('INSERT INTO %1$s AS s (__h, %2$s, __w) SELECT md5(jsonb_build_array(%3$s)::text), %3$s, x.__w FROM (%4$s) x WHERE x.__w <> 0 '
                       'ON CONFLICT (%5$s) DO UPDATE SET __w = s.__w + EXCLUDED.__w',
                       src ->> 'state', v_cols, v_xcols, v_net, v_conf)
        USING v_rows;
        EXECUTE format('DELETE FROM %1$s s WHERE s.__w = 0 AND s.__h IN (SELECT md5(jsonb_build_array(%2$s)::text) FROM (%3$s) x)',
                       src ->> 'state', v_xcols, CASE WHEN v_repl THEN v_net ELSE 'SELECT * FROM ' || (src ->> 'staging') END)
        USING v_rows;
    END IF;
END $$;

-- ── The certificate ─────────────────────────────────────────────────────────

-- A distributed state with subqueries has no single table of partial aggregates: its rule and check
-- name how each part is placed (see 0.22.0 for the rest).
ALTER FUNCTION isomer._engine_certificate(isomer.projections) RENAME TO _engine_certificate_single;

CREATE FUNCTION isomer._engine_certificate(p isomer.projections, OUT rules jsonb, OUT assumptions jsonb, OUT checks jsonb)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    plan    jsonb := (SELECT e.plan FROM isomer.engine_plans e WHERE e.projection = p.name);
    v_shard text;
    v_ref   text;
    v_bad   text;
BEGIN
    SELECT c.rules, c.assumptions, c.checks INTO rules, assumptions, checks FROM isomer._engine_certificate_single(p) c;
    IF NOT (coalesce((plan ->> 'nested')::boolean, false) AND coalesce((plan ->> 'distributed')::boolean, false)) THEN
        RETURN;
    END IF;
    SELECT string_agg(format('%s by %s', e.key::oid::regclass, e.value #>> '{}'), ', ' ORDER BY e.key) FILTER (WHERE jsonb_typeof(e.value) = 'string'),
           string_agg(e.key::oid::regclass::text, ', ' ORDER BY e.key) FILTER (WHERE jsonb_typeof(e.value) <> 'string')
    INTO v_shard, v_ref
    FROM jsonb_each(plan -> 'placement' -> 'rels') e;
    rules := (SELECT coalesce(jsonb_agg(r), '[]') FROM jsonb_array_elements(rules) r WHERE r #>> '{}' NOT LIKE 'distributed state:%')
          || to_jsonb(format('distributed state: each subquery''s state is sharded by the column it is correlated through, co-located with the outer rows it is correlated with (%s), so a correlated subquery runs on the shard holding its outer row; %s; a key is derived by filtering on it on every shard',
                             v_shard, coalesce('kept on every node: ' || v_ref, 'nothing is kept on every node')));
    -- Every table of the state is where the plan says.
    SELECT string_agg(t, ', ') INTO v_bad FROM (
        SELECT f ->> 'p_table' AS t FROM jsonb_array_elements(plan -> 'facts') f
        UNION ALL SELECT x.value ->> 'staging' FROM jsonb_each(plan -> 'sources') x
        UNION ALL SELECT x.value ->> 'state' FROM jsonb_each(plan -> 'sources') x WHERE x.value ? 'state') s
    WHERE NOT EXISTS (SELECT 1 FROM pg_dist_partition d WHERE d.logicalrelid = s.t::regclass);
    checks := (SELECT coalesce(jsonb_agg(c), '[]') FROM jsonb_array_elements(checks) c WHERE c ->> 'check' <> 'state distributed')
           || jsonb_build_array(jsonb_build_object('check', 'state distributed', 'passed', v_bad IS NULL,
                  'detail', coalesce('not distributed: ' || v_bad, format('sharded: %s', v_shard))));
END $$;

-- ── Analytics projections keep their strategy and state ─────────────────────

-- alter_projection (see 0.21.0), refusing to change an analytics projection's strategy or state.
CREATE OR REPLACE FUNCTION isomer.alter_projection(name text, freshness text DEFAULT NULL, lag interval DEFAULT NULL,
                                        strategy text DEFAULT NULL, sources jsonb DEFAULT NULL, state text DEFAULT NULL)
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
    -- An analytics projection is rebuilt as a columnar table, not as documents: its strategy is
    -- chosen when it is created, and its state stays on the coordinator.
    IF p.kind = 'analytics' AND ((strategy IS NOT NULL AND strategy <> p.strategy) OR (state IS NOT NULL AND state <> p.state_placement)) THEN
        RAISE EXCEPTION 'isomer: % is an analytics projection: its strategy is chosen when it is created, and its state stays on the coordinator', p.name
            USING HINT = 'Drop it and create it again with create_analytics_projection(..., strategy => ...).';
    END IF;
    -- A distributed state is dropped, created or built and read again in this transaction:
    -- Citus must use one connection per node from the first statement on.
    IF p.state_placement = 'distributed' OR alter_projection.state = 'distributed' THEN
        PERFORM set_config('citus.multi_shard_modify_mode', 'sequential', true);
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
    IF state IS NOT NULL AND state <> p.state_placement THEN
        IF state NOT IN ('coordinator', 'distributed') THEN
            RAISE EXCEPTION 'isomer: state must be ''coordinator'' or ''distributed'', not %', state;
        END IF;
        IF p.strategy <> 'incremental' THEN
            RAISE EXCEPTION 'isomer: projection % is maintained by key, which keeps no state to distribute', p.name
                USING HINT = 'Switch it to strategy => ''incremental'' first.';
        END IF;
        IF state = 'distributed' THEN
            IF p.freshness = 'sync' THEN
                RAISE EXCEPTION 'isomer: projection % is sync: it folds in the writing transaction, on its tables'' node', p.name;
            END IF;
            v_reason := isomer._distribute_blocker(isomer._engine_shape(p.query, p.key_column, p.kind));
            IF v_reason IS NOT NULL THEN
                RAISE EXCEPTION 'isomer: the state of projection % can''t be distributed: %', p.name, v_reason;
            END IF;
        END IF;
        -- Built again where it now lives. Dropping the capture triggers locks every shard against
        -- writes until commit, so the deltas still queued (here and on the workers) are all in the
        -- tables the new state is built from.
        PERFORM isomer._drop_source_triggers(p.name);
        PERFORM isomer._engine_drop_state(p.name);
        UPDATE isomer.projections SET state_placement = alter_projection.state WHERE projections.name = p.name;
        DELETE FROM isomer.deltas WHERE projection = p.name;
        IF isomer._multi_node() THEN
            PERFORM run_command_on_workers(format('DELETE FROM isomer.node_deltas WHERE projection = %L', p.name));
        END IF;
        PERFORM isomer._engine_compile(p.name);
        PERFORM isomer._create_source_triggers(p.name);
        PERFORM isomer._engine_build(p.name);
        PERFORM isomer._rebuild_all(p.name);
        DELETE FROM isomer.pending WHERE projection = p.name;
    END IF;
    IF lag IS NOT NULL THEN
        UPDATE isomer.projections SET lag_budget = lag WHERE projections.name = p.name;
    END IF;
    SELECT * INTO p FROM isomer.projections WHERE projections.name = alter_projection.name;
    RETURN format('%s: freshness %s, lag budget %s, strategy %s%s', p.name, p.freshness, p.lag_budget, p.strategy,
                  CASE WHEN p.strategy = 'incremental' THEN ', state on the ' || CASE p.state_placement WHEN 'distributed' THEN 'cluster' ELSE 'coordinator' END ELSE '' END);
END $$;

-- ── Dropping a projection while it is folded ────────────────────────────────

-- drop_projection (see 0.22.0), after any fold of the projection under way.
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
    -- Wait for a fold of this projection under way, and keep the next one out: a fold deletes the
    -- projection's deltas and then writes rows that reference the projection, and deleting the
    -- projection cascades to those deltas, so the two deadlocked when they met in between.
    PERFORM pg_advisory_xact_lock(hashtext('isomer.engine'), hashtext(p.name));
    PERFORM pg_advisory_xact_lock(hashtext('isomer.changes'), hashtext(p.name));
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
