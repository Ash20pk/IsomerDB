-- IsomerDB 0.26.0: anchored subqueries, so more queries with subqueries can have their state
-- distributed (PRD FR-P6).
--
--   * A subquery that aggregates and is correlated with its sharded parent through another column
--     than the parent's shard column (an order's lines inside a customer's document: the lines are
--     correlated with the order's id, the orders sharded by customer) is anchored. Its partial
--     aggregates carry the parent's shard value, the customer of each line's order, and are
--     sharded by it, co-located with the orders and customers they belong to; the subquery is told
--     the two are equal, so the cluster runs it on that shard.
--   * The anchors come from a map on the coordinator, kept from the parent table's changes. A
--     parent row that moves (an order to another customer) moves its partial rows with it; rows
--     whose parent doesn't exist wait on the coordinator until it does. A correlation value with
--     several parent rows has its partial rows under each of their shard values.
--   * Not min or max in an anchored subquery, and not one anchored below another: refused with
--     the reason, as before.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.26.0'" to load this file. \quit

-- Where each part of a nested state goes (see 0.24.0). New: a subquery that aggregates and is
-- correlated with its sharded parent through another column than the parent's shard column (an
-- order's lines, the orders sharded by customer) is anchored: {"rti", "col": "__anchor",
-- "anchor": {"col", "outer_col", "parent_block", "parent_rti", "parent_col", "parent_relid"}}. Its
-- partial aggregates carry the parent's shard value (__anchor) and are sharded by it. Not min or
-- max (their multisets would need anchoring too), and not below another anchored subquery.
CREATE OR REPLACE FUNCTION isomer._engine_placement(s jsonb)
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
            -- Correlated with its sharded parent through another column (a line with its order,
            -- the orders sharded by customer): anchored, its state sharded by the parent's shard
            -- value, looked up through the parent's rows.
            IF jsonb_typeof(v_par) = 'object' AND v_par ? 'rti' AND NOT v_par ? 'anchor' THEN
                SELECT x INTO c FROM jsonb_array_elements(b -> 'correlation') x
                WHERE (x ->> 'outer_rti')::int = (v_par ->> 'rti')::int AND (x ->> 'rti')::int = (b ->> 'fact')::int
                  AND b -> 'pkeys' ? (x ->> 'col') LIMIT 1;
            END IF;
            IF c IS NOT NULL THEN
                IF EXISTS (SELECT 1 FROM jsonb_array_elements(b -> 'aggs') g WHERE g ->> 'cls' IN ('min', 'max')) THEN
                    RETURN jsonb_build_object('reason', format('a subquery takes min or max over %s, which is correlated with its outer row through another column than the one that row is sharded by',
                                                               (b ->> 'fact_relid')::oid::regclass));
                END IF;
                v_pl := v_pl || jsonb_build_array(jsonb_build_object('rti', (b ->> 'fact')::int, 'col', '__anchor', 'anchor', jsonb_build_object(
                    'col', c ->> 'col', 'outer_col', c ->> 'outer_col', 'parent_block', (b ->> 'parent')::int,
                    'parent_rti', (v_par ->> 'rti')::int, 'parent_col', v_par ->> 'col',
                    'parent_relid', (SELECT (pr ->> 'relid')::oid FROM jsonb_array_elements(s -> 'blocks' -> (b ->> 'parent')::int -> 'rtes') pr
                                     WHERE (pr ->> 'rti')::int = (v_par ->> 'rti')::int))));
                CONTINUE;
            END IF;
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
    FROM jsonb_each(v_rels) WHERE jsonb_typeof(value) = 'string' AND value #>> '{}' <> '__anchor';
    IF v_type LIKE '%,%' THEN
        RETURN jsonb_build_object('reason', format('the columns its subqueries are correlated through have different types (%s), and co-located shards need one', v_type));
    END IF;
    RETURN jsonb_build_object('blocks', v_pl, 'rels', v_rels);
END $$;

-- ── Anchored state ──────────────────────────────────────────────────────────
--
-- A subquery that aggregates and is correlated with its sharded parent through another column
-- (SELECT sum(l.qty) FROM lines l WHERE l.order_id = o.id, the orders sharded by customer_id)
-- keeps its partial aggregates sharded by the parent's shard value: each partial row carries the
-- customer of its order (__anchor), so it sits on the shard of the orders it is correlated with,
-- and the subquery is told l.__anchor = o.customer_id. The anchor of a correlation value comes
-- from a map on the coordinator (the order's id to its customer), kept from the parent table's own
-- changes. A value with several parent rows (two orders with one id, under two customers: a
-- sharded table can't make id unique on its own) has its partial rows kept once under each, which
-- is what the query means. A value with no parent row keeps its partial rows on the coordinator
-- (the orphans) until one appears. When a parent row moves (an order to another customer), the
-- partial rows of its value move with it, before the fold anchors new rows.

-- Turn a block's partial aggregates, built on the coordinator, into anchored ones: the column, the
-- key, the view, sharded co-located with the parent; the orphans and the map beside them.
CREATE FUNCTION isomer._engine_anchor_fact(plan jsonb, f jsonb, a jsonb, p_colocate text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_parent oid := (a ->> 'parent_relid')::oid;
    v_type   text := isomer._coltype(v_parent, a ->> 'parent_col');
    v_ktype  text := isomer._coltype(v_parent, a ->> 'outer_col');
    v_po     text := format('isomer_state.%I', format('%s_b%spo', plan ->> 'prefix', f ->> 'block'));
    v_map    text := format('isomer_state.%I', format('%s_b%sam', plan ->> 'prefix', f ->> 'block'));
    v_pkeys  text[] := ARRAY(SELECT jsonb_array_elements_text(f -> 'pkeys'));
BEGIN
    PERFORM set_config('citus.enable_ddl_propagation', 'off', true);
    EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING ALL)', v_po, f ->> 'p_table');
    EXECUTE format('CREATE TABLE %s (k %s NOT NULL, __anchor %s NOT NULL, n bigint NOT NULL, PRIMARY KEY (k, __anchor))', v_map, v_ktype, v_type);
    EXECUTE format('ALTER TABLE %s ADD COLUMN __anchor %s', f ->> 'p_table', v_type);
    PERFORM isomer._engine_rekey(f ->> 'p_table', ARRAY['__anchor', '__h']);
    -- The view the derivation reads, with the anchor last (the columns before it unchanged).
    EXECUTE format('CREATE OR REPLACE VIEW %s AS SELECT %s, p.__n%s, p.__anchor FROM %s p WHERE p.__n > 0',
                   f ->> 'p_view', isomer._aligned((f ->> 'fact_relid')::oid, v_pkeys, 'p'),
                   coalesce((SELECT string_agg(format(', p.%I', attname), '' ORDER BY attnum) FROM pg_attribute
                             WHERE attrelid = (f ->> 'p_table')::regclass AND attnum > 0 AND NOT attisdropped
                               AND attname NOT IN ('__h', '__n', '__anchor') AND NOT (attname = ANY (v_pkeys))), ''),
                   f ->> 'p_table');
    PERFORM set_config('citus.enable_ddl_propagation', 'on', true);
    PERFORM create_distributed_table(f ->> 'p_table', '__anchor', colocate_with => p_colocate);
    RETURN f || jsonb_build_object('anchor', a || jsonb_build_object('map', v_map, 'orphans', v_po, 'key_type', v_ktype,
                                                                     'parent_table', plan -> 'sources' -> v_parent::text ->> 'table',
                                                                     'parent_staging', plan -> 'sources' -> v_parent::text ->> 'staging'));
END $$;

-- The columns of an anchored fact's partial aggregates, without the anchor, in table order.
CREATE FUNCTION isomer._engine_anchor_columns(f jsonb)
RETURNS text[] LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT ARRAY(SELECT attname::text FROM pg_attribute WHERE attrelid = (f -> 'anchor' ->> 'orphans')::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum)
$$;

-- Fold an anchored fact's rows (see _engine_fold_fact): grouped here, then written under every
-- anchor of their correlation value, or to the orphans when it has none.
CREATE FUNCTION isomer._engine_fold_fact_anchored(plan jsonb, p_src text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a      jsonb := plan -> 'anchor';
    v_cols text := '__h' || coalesce((SELECT string_agg(format(', %I', c), '') FROM jsonb_array_elements_text(plan -> 'pkeys') c), '') || ', __n';
    v_set  text := '__n = t.__n + EXCLUDED.__n';
    v_dcol text;
    x      jsonb;
    v_i    integer;
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
    v_dcol := (SELECT string_agg('d.' || c, ', ') FROM unnest(string_to_array(v_cols, ', ')) c);
    DROP TABLE IF EXISTS pg_temp.isomer_anchor_d;
    EXECUTE format('CREATE TEMP TABLE isomer_anchor_d AS %s', isomer._engine_p_select(plan, p_src));
    EXECUTE format('INSERT INTO %1$s AS t (__anchor, %2$s) SELECT m.__anchor, %3$s FROM pg_temp.isomer_anchor_d d JOIN %4$s m ON m.k = d.%5$I '
                   'ON CONFLICT (__anchor, __h) DO UPDATE SET %6$s',
                   plan ->> 'p_table', v_cols, v_dcol, a ->> 'map', a ->> 'col', v_set);
    EXECUTE format('INSERT INTO %1$s AS t (%2$s) SELECT %3$s FROM pg_temp.isomer_anchor_d d WHERE NOT EXISTS (SELECT 1 FROM %4$s m WHERE m.k = d.%5$I) '
                   'ON CONFLICT (__h) DO UPDATE SET %6$s',
                   a ->> 'orphans', v_cols, v_dcol, a ->> 'map', a ->> 'col', v_set);
    -- Only the groups this fold reached can have emptied.
    EXECUTE format('DELETE FROM %s t WHERE t.__n = 0 AND t.__h IN (SELECT __h FROM pg_temp.isomer_anchor_d)', plan ->> 'p_table');
    EXECUTE format('DELETE FROM %s t WHERE t.__n = 0 AND t.__h IN (SELECT __h FROM pg_temp.isomer_anchor_d)', a ->> 'orphans');
    DROP TABLE pg_temp.isomer_anchor_d;
END $$;

-- Before a fold: the parents' changed rows update each anchored fact's map, and the partial rows
-- of every correlation value whose anchors changed are written again under its anchors now.
CREATE FUNCTION isomer._engine_anchor_moves(plan jsonb, p_relids oid[])
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    f      jsonb;
    a      jsonb;
    v_vals jsonb;
    v_rows jsonb;
    v_orph jsonb;
    v_in   text;
    v_cols text;
BEGIN
    FOR f IN SELECT fx FROM isomer._engine_facts(plan) fx WHERE fx ? 'anchor' LOOP
        a := f -> 'anchor';
        CONTINUE WHEN NOT ((a ->> 'parent_relid')::oid = ANY (p_relids));
        EXECUTE format('SELECT jsonb_agg(DISTINCT k) FROM (SELECT %1$I AS k FROM %3$s WHERE %1$I IS NOT NULL AND %2$I IS NOT NULL GROUP BY %1$I, %2$I HAVING sum(__w) <> 0) x',
                       a ->> 'outer_col', a ->> 'parent_col', a ->> 'parent_staging') INTO v_vals;
        CONTINUE WHEN v_vals IS NULL;
        EXECUTE format('INSERT INTO %1$s AS m (k, __anchor, n) SELECT %2$I, %3$I, sum(__w) FROM %4$s WHERE %2$I IS NOT NULL AND %3$I IS NOT NULL GROUP BY 1, 2 HAVING sum(__w) <> 0 '
                       'ON CONFLICT (k, __anchor) DO UPDATE SET n = m.n + EXCLUDED.n',
                       a ->> 'map', a ->> 'outer_col', a ->> 'parent_col', a ->> 'parent_staging');
        v_in := format('(SELECT x::%s FROM jsonb_array_elements_text($1) x)', a ->> 'key_type');
        EXECUTE format('DELETE FROM %s WHERE n = 0 AND k IN %s', a ->> 'map', v_in) USING v_vals;

        -- Every partial row of those values (one copy of each: they are the same under every anchor).
        EXECUTE format('SELECT jsonb_agg(to_jsonb(t) - ''__anchor'') FROM (SELECT DISTINCT ON (__h) * FROM %1$s WHERE %2$I IN %3$s ORDER BY __h) t',
                       f ->> 'p_table', a ->> 'col', v_in) USING v_vals INTO v_rows;
        EXECUTE format('SELECT jsonb_agg(to_jsonb(t)) FROM %1$s t WHERE %2$I IN %3$s', a ->> 'orphans', a ->> 'col', v_in) USING v_vals INTO v_orph;
        CONTINUE WHEN v_rows IS NULL AND v_orph IS NULL;
        v_rows := coalesce(v_rows, '[]') || coalesce(v_orph, '[]');
        EXECUTE format('DELETE FROM %s WHERE %I IN %s', f ->> 'p_table', a ->> 'col', v_in) USING v_vals;
        EXECUTE format('DELETE FROM %s WHERE %I IN %s', a ->> 'orphans', a ->> 'col', v_in) USING v_vals;
        v_cols := (SELECT string_agg(format('%I', c), ', ') FROM unnest(isomer._engine_anchor_columns(f)) c);
        EXECUTE format('INSERT INTO %1$s (__anchor, %2$s) SELECT m.__anchor, %3$s FROM jsonb_populate_recordset(NULL::%4$s, $1) r JOIN %5$s m ON m.k = r.%6$I',
                       f ->> 'p_table', v_cols, (SELECT string_agg(format('r.%I', c), ', ') FROM unnest(isomer._engine_anchor_columns(f)) c),
                       a ->> 'orphans', a ->> 'map', a ->> 'col') USING v_rows;
        EXECUTE format('INSERT INTO %1$s SELECT r.* FROM jsonb_populate_recordset(NULL::%1$s, $1) r WHERE NOT EXISTS (SELECT 1 FROM %2$s m WHERE m.k = r.%3$I)',
                       a ->> 'orphans', a ->> 'map', a ->> 'col') USING v_rows;
    END LOOP;
END $$;

-- Before a build: the map from the parent table, and no orphans.
CREATE FUNCTION isomer._engine_anchor_build(plan jsonb, f jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a jsonb := f -> 'anchor';
BEGIN
    EXECUTE format('TRUNCATE %s, %s', a ->> 'map', a ->> 'orphans');
    EXECUTE format('INSERT INTO %1$s (k, __anchor, n) SELECT %2$I, %3$I, count(*) FROM %4$s WHERE %2$I IS NOT NULL AND %3$I IS NOT NULL GROUP BY 1, 2',
                   a ->> 'map', a ->> 'outer_col', a ->> 'parent_col', a ->> 'parent_table');
END $$;

-- Distribute a compiled nested state (see 0.24.0); an anchored block's partial aggregates are sharded by
-- their parent's shard value, and the statements are generated again with the anchor condition.
CREATE OR REPLACE FUNCTION isomer._engine_distribute_nested(p isomer.projections, s jsonb, plan jsonb)
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
    v_cfgs  jsonb := plan -> 'block_config';
    v_kc    jsonb;
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
        -- An anchored table's changed rows carry no shard value: they are on every node.
        IF v_col IS NULL OR v_col = '__anchor' THEN
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
        IF v_bl ? 'anchor' THEN
            f := isomer._engine_anchor_fact(plan, f, v_bl -> 'anchor', v_first);
            v_cfgs := jsonb_set(v_cfgs, ARRAY[f ->> 'block', 'anchor'], jsonb_build_object(
                'col', '__anchor', 'outer_rti', (v_bl -> 'anchor' ->> 'parent_rti')::int, 'outer_col', v_bl -> 'anchor' ->> 'parent_col'));
        ELSIF v_bl ? 'col' THEN
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

    -- An anchored block is told its parent's shard value equals its own: the derivation and the
    -- key queries are generated again with that condition (not where the anchored table's changed
    -- rows drive, read from a table on every node).
    IF v_cfgs IS DISTINCT FROM plan -> 'block_config' THEN
        UPDATE isomer.projections SET derive_query = isomer._engine_sql_blocks(p.query, 'derive', v_cfgs, 0, NULL) WHERE name = p.name;
        FOR v_i IN 0 .. jsonb_array_length(plan -> 'key_sqls') - 1 LOOP
            m := plan -> 'key_sqls' -> v_i;
            v_kc := jsonb_set(v_cfgs, ARRAY[m ->> 'block', 'rels', ((m ->> 'rti')::int - 1)::text],
                              to_jsonb((plan -> 'sources' -> (m ->> 'relid') ->> 'staging')::regclass::oid::bigint));
            IF (v_kc -> (m ->> 'block')::int ->> 'fact')::int = (m ->> 'rti')::int THEN
                v_kc := v_kc #- ARRAY[m ->> 'block', 'anchor'];
            END IF;
            plan := jsonb_set(plan, ARRAY['key_sqls', v_i::text, 'sql'],
                              to_jsonb(isomer._engine_sql_blocks(p.query, 'keys', v_kc, (m ->> 'block')::int, (m ->> 'rti')::int)));
        END LOOP;
    END IF;

    plan := plan || jsonb_build_object(
        'distributed', true, 'placement', v_pl, 'block_config', v_cfgs,
        'derive_keyed', isomer._engine_sql_blocks(p.query, 'derive_keyed',
                                                  jsonb_set(v_cfgs, '{0,key}', to_jsonb(p.key_column)), 0, NULL));
    UPDATE isomer.engine_plans ep SET plan = $3, compiled_at = clock_timestamp() WHERE ep.projection = p.name;
    RETURN plan;
END $$;

-- Fold a fact's rows (see 0.24.0); an anchored fact's own way.
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
    IF plan ? 'anchor' THEN
        PERFORM isomer._engine_fold_fact_anchored(plan, p_src);
        RETURN;
    END IF;
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

-- Fold queued deltas (see 0.22.0), moving anchored state with its parents first.
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

    -- Anchored state follows its parents: moved where a parent moved, before this fold's rows are
    -- anchored by the parents as they now are.
    PERFORM isomer._engine_anchor_moves(plan, v_relids);

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

-- Build the state from the tables (see 0.22.0), the anchors first.
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
    FOR f IN SELECT fx FROM isomer._engine_facts(plan) fx WHERE fx ? 'anchor' LOOP
        PERFORM isomer._engine_anchor_build(plan, f);
    END LOOP;
    FOR f IN SELECT * FROM isomer._engine_facts(plan) LOOP
        PERFORM isomer._engine_fold_fact(f, '(' || (f ->> 'fact_base_sql') || ')');
    END LOOP;
END $$;

-- The certificate (see 0.24.0), naming anchored tables.
CREATE OR REPLACE FUNCTION isomer._engine_certificate(p isomer.projections, OUT rules jsonb, OUT assumptions jsonb, OUT checks jsonb)
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
    SELECT string_agg(format('%s by %s', e.key::oid::regclass,
                             CASE WHEN e.value #>> '{}' = '__anchor' THEN 'the shard value of the row it is correlated with (anchored)' ELSE e.value #>> '{}' END),
                      ', ' ORDER BY e.key) FILTER (WHERE jsonb_typeof(e.value) = 'string'),
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
