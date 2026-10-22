-- IsomerDB 0.27.0: the rest of anchored subqueries, so every query with subqueries the engine
-- maintains can have its state distributed when each subquery is correlated by equality (PRD
-- FR-P6).
--
--   * min and max in an anchored subquery (the largest line of each order inside a customer's
--     document): each multiset of values is anchored like the partial aggregates, carrying the
--     customer of its order, sharded by it, with orphans of its own on the coordinator, and moving
--     with its partial rows.
--   * An anchored subquery inside another (each line's marks inside the order's lines inside a
--     customer's document): its anchors are those of its parent's anchored partial rows, a mark
--     carrying the customer of its line's order. After each fold, the parent's partial rows that
--     the fold reached or moved update the map, and the marks of every value whose anchors changed
--     follow, level by level.
--   * Not a subquery in the WHERE of an anchored one: finding the keys a changed line reaches
--     reads the lines' changed rows, kept on every node, and the cluster can't run a correlated
--     subquery over sharded rows from there. Refused with the reason.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.27.0'" to load this file. \quit

-- Where each part of a nested state goes (see 0.26.0). min and max are anchored now, and a
-- subquery correlated with an anchored parent is anchored below it: {"rti", "col": "__anchor",
-- "anchor": {..., "parent_col": "__anchor", "nested": true}}, its anchors those of the parent's
-- partial rows.
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
            -- value, looked up through the parent's rows; below an anchored parent, through the
            -- parent's anchored partial rows.
            IF jsonb_typeof(v_par) = 'object' AND v_par ? 'rti' THEN
                SELECT x INTO c FROM jsonb_array_elements(b -> 'correlation') x
                WHERE (x ->> 'outer_rti')::int = (v_par ->> 'rti')::int AND (x ->> 'rti')::int = (b ->> 'fact')::int
                  AND b -> 'pkeys' ? (x ->> 'col')
                  AND (NOT v_par ? 'anchor' OR s -> 'blocks' -> (b ->> 'parent')::int -> 'pkeys' ? (x ->> 'outer_col'))
                -- Below an anchored parent, through the column the parent is anchored by if it can
                -- (a mark through its line's order, not its line number, which every order has).
                ORDER BY (x ->> 'outer_col') IS DISTINCT FROM (v_par -> 'anchor' ->> 'col') LIMIT 1;
            END IF;
            IF c IS NOT NULL THEN
                IF v_par ? 'anchor' AND b ->> 'place' NOT IN ('target', 'agg') THEN
                    RETURN jsonb_build_object('reason', format('a subquery over %s is in the %s of a subquery whose state is anchored, and the keys a change to that subquery''s rows reaches can''t be found on the cluster through it',
                                                               (b ->> 'fact_relid')::oid::regclass, upper(b ->> 'place')));
                END IF;
                v_pl := v_pl || jsonb_build_array(jsonb_build_object('rti', (b ->> 'fact')::int, 'col', '__anchor', 'anchor', jsonb_build_object(
                    'col', c ->> 'col', 'outer_col', c ->> 'outer_col', 'parent_block', (b ->> 'parent')::int,
                    'parent_rti', (v_par ->> 'rti')::int, 'parent_col', v_par ->> 'col', 'nested', v_par ? 'anchor',
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
-- (See 0.26.0.) A block's min and max keep a multiset of values per partial row; anchored, each
-- multiset row carries the anchor of its partial row, is sharded by it, and an orphan's multiset
-- waits on the coordinator beside it. A block anchored below an anchored parent (a line's marks,
-- inside the order's lines, inside a customer's document) takes its anchors from the parent's
-- partial rows: a mark is kept under every customer that has a partial row of its line. The
-- partial rows a fold reaches, or moves, are noted per block (isomer_anchor_touched, for this
-- transaction), so after the fold the blocks below can update their maps and move what changed.

-- Turn a block's partial aggregates into anchored ones (see 0.26.0), its multisets too; below an
-- anchored parent, the anchor has the type of the parent's.
CREATE OR REPLACE FUNCTION isomer._engine_anchor_fact(plan jsonb, f jsonb, a jsonb, p_colocate text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_parent oid := (a ->> 'parent_relid')::oid;
    v_pfact  jsonb := (SELECT x FROM jsonb_array_elements(plan -> 'facts') x WHERE (x ->> 'block')::int = (a ->> 'parent_block')::int);
    v_type   text;
    v_ktype  text := isomer._coltype(v_parent, a ->> 'outer_col');
    v_po     text := format('isomer_state.%I', format('%s_b%spo', plan ->> 'prefix', f ->> 'block'));
    v_map    text := format('isomer_state.%I', format('%s_b%sam', plan ->> 'prefix', f ->> 'block'));
    v_pkeys  text[] := ARRAY(SELECT jsonb_array_elements_text(f -> 'pkeys'));
    v_mm     jsonb := '[]';
    v_pom    text;
    m        jsonb;
BEGIN
    v_type := CASE WHEN coalesce((a ->> 'nested')::boolean, false)
                   THEN isomer._coltype((v_pfact ->> 'p_table')::regclass::oid, '__anchor')
                   ELSE isomer._coltype(v_parent, a ->> 'parent_col') END;
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

    -- min and max: each multiset anchored with its partial rows, its orphans beside theirs.
    FOR m IN SELECT * FROM jsonb_array_elements(f -> 'minmax') LOOP
        v_pom := format('isomer_state.%I', format('%s_b%spom%s', plan ->> 'prefix', f ->> 'block', m ->> 'i'));
        PERFORM set_config('citus.enable_ddl_propagation', 'off', true);
        EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING ALL)', v_pom, m ->> 'table');
        EXECUTE format('ALTER TABLE %s ADD COLUMN __anchor %s', m ->> 'table', v_type);
        PERFORM isomer._engine_rekey(m ->> 'table', ARRAY['__anchor', '__h', 'value']);
        PERFORM set_config('citus.enable_ddl_propagation', 'on', true);
        PERFORM create_distributed_table(m ->> 'table', '__anchor', colocate_with => f ->> 'p_table');
        v_mm := v_mm || jsonb_build_array(m || jsonb_build_object('orphans', v_pom));
    END LOOP;

    RETURN f || jsonb_build_object('minmax', v_mm, 'anchor', a || jsonb_build_object(
        'map', v_map, 'orphans', v_po, 'key_type', v_ktype,
        'parent_table', plan -> 'sources' -> v_parent::text ->> 'table',
        'parent_staging', plan -> 'sources' -> v_parent::text ->> 'staging',
        'parent_p_table', v_pfact ->> 'p_table'));
END $$;

-- Note a block's partial rows that a fold reached or moved, for the blocks anchored below it.
CREATE FUNCTION isomer._engine_anchor_touch(p_block integer, p_rows jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF p_rows IS NULL THEN
        RETURN;
    END IF;
    IF to_regclass('pg_temp.isomer_anchor_touched') IS NULL THEN
        CREATE TEMP TABLE isomer_anchor_touched (block integer NOT NULL, r jsonb NOT NULL);
    END IF;
    EXECUTE 'INSERT INTO pg_temp.isomer_anchor_touched SELECT $1, x FROM jsonb_array_elements($2) x' USING p_block, p_rows;
END $$;

CREATE FUNCTION isomer._engine_anchor_untouch()
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF to_regclass('pg_temp.isomer_anchor_touched') IS NOT NULL THEN
        EXECUTE 'TRUNCATE pg_temp.isomer_anchor_touched';
    END IF;
END $$;

-- Fold an anchored fact's rows (see 0.26.0), and its multisets: each value written under every
-- anchor of its correlation value, or to the orphans, and the partial rows' min or max taken again.
CREATE OR REPLACE FUNCTION isomer._engine_fold_fact_anchored(plan jsonb, p_src text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a      jsonb := plan -> 'anchor';
    v_keys text := (SELECT string_agg(format('d.%I', c), ', ') FROM jsonb_array_elements_text(plan -> 'pkeys') c);
    v_cols text := '__h' || coalesce((SELECT string_agg(format(', %I', c), '') FROM jsonb_array_elements_text(plan -> 'pkeys') c), '') || ', __n';
    v_set  text := '__n = t.__n + EXCLUDED.__n';
    v_dcol text;
    v_rows jsonb;
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

    FOR x IN SELECT * FROM jsonb_array_elements(plan -> 'minmax') LOOP
        v_i := (x ->> 'i')::int;
        DROP TABLE IF EXISTS pg_temp.isomer_anchor_m;
        EXECUTE format('CREATE TEMP TABLE isomer_anchor_m AS SELECT md5(jsonb_build_array(%1$s)::text) AS __h, d.%2$I AS k, d.__a%3$s AS value, sum(d.__w)::bigint AS mult '
                       'FROM %4$s d WHERE d.__f%3$s AND d.__a%3$s IS NOT NULL GROUP BY %1$s, d.__a%3$s HAVING sum(d.__w) <> 0',
                       v_keys, a ->> 'col', v_i, p_src);
        EXECUTE format('INSERT INTO %1$s AS t (__anchor, __h, value, mult) SELECT m.__anchor, d.__h, d.value, d.mult FROM pg_temp.isomer_anchor_m d JOIN %2$s m ON m.k = d.k '
                       'ON CONFLICT (__anchor, __h, value) DO UPDATE SET mult = t.mult + EXCLUDED.mult',
                       x ->> 'table', a ->> 'map');
        EXECUTE format('INSERT INTO %1$s AS t (__h, value, mult) SELECT d.__h, d.value, d.mult FROM pg_temp.isomer_anchor_m d WHERE NOT EXISTS (SELECT 1 FROM %2$s m WHERE m.k = d.k) '
                       'ON CONFLICT (__h, value) DO UPDATE SET mult = t.mult + EXCLUDED.mult',
                       x ->> 'orphans', a ->> 'map');
        EXECUTE format('DELETE FROM %s t WHERE t.mult = 0 AND t.__h IN (SELECT __h FROM pg_temp.isomer_anchor_m)', x ->> 'table');
        EXECUTE format('DELETE FROM %s t WHERE t.mult = 0 AND t.__h IN (SELECT __h FROM pg_temp.isomer_anchor_m)', x ->> 'orphans');
        EXECUTE format('UPDATE %1$s t SET __m%2$s = (SELECT %3$s(m.value) FROM %4$s m WHERE m.__h = t.__h AND m.__anchor = t.__anchor AND m.mult > 0) '
                       'WHERE t.__h IN (SELECT __h FROM pg_temp.isomer_anchor_m)',
                       plan ->> 'p_table', v_i, x ->> 'cls', x ->> 'table');
        EXECUTE format('UPDATE %1$s t SET __m%2$s = (SELECT %3$s(m.value) FROM %4$s m WHERE m.__h = t.__h AND m.mult > 0) '
                       'WHERE t.__h IN (SELECT __h FROM pg_temp.isomer_anchor_m)',
                       a ->> 'orphans', v_i, x ->> 'cls', x ->> 'orphans');
        DROP TABLE pg_temp.isomer_anchor_m;
    END LOOP;

    IF coalesce((plan ->> 'anchor_children')::boolean, false) THEN
        EXECUTE 'SELECT jsonb_agg(to_jsonb(d)) FROM pg_temp.isomer_anchor_d d' INTO v_rows;
        PERFORM isomer._engine_anchor_touch((plan ->> 'block')::int, v_rows);
    END IF;
    DROP TABLE pg_temp.isomer_anchor_d;
END $$;

-- Write the partial rows of these correlation values, and their multisets, again under their
-- anchors now (or to the orphans): one copy of each, since they are the same under every anchor.
CREATE FUNCTION isomer._engine_anchor_relocate(f jsonb, p_vals jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a       jsonb := f -> 'anchor';
    v_in    text := format('(SELECT x::%s FROM jsonb_array_elements_text($1) x)', a ->> 'key_type');
    v_hs    text := '(SELECT x ->> ''__h'' FROM jsonb_array_elements($1) x)';
    v_hk    text := format('(SELECT x ->> ''__h'' AS h, (x ->> %L)::%s AS k FROM jsonb_array_elements($2) x)', a ->> 'col', a ->> 'key_type');
    v_rows  jsonb;
    v_orph  jsonb;
    v_mrows jsonb;
    v_cols  text;
    m       jsonb;
BEGIN
    IF p_vals IS NULL THEN
        RETURN;
    END IF;
    EXECUTE format('SELECT jsonb_agg(to_jsonb(t) - ''__anchor'') FROM (SELECT DISTINCT ON (__h) * FROM %1$s WHERE %2$I IN %3$s ORDER BY __h) t',
                   f ->> 'p_table', a ->> 'col', v_in) USING p_vals INTO v_rows;
    EXECUTE format('SELECT jsonb_agg(to_jsonb(t)) FROM %1$s t WHERE %2$I IN %3$s', a ->> 'orphans', a ->> 'col', v_in) USING p_vals INTO v_orph;
    IF v_rows IS NULL AND v_orph IS NULL THEN
        RETURN;
    END IF;
    v_rows := coalesce(v_rows, '[]') || coalesce(v_orph, '[]');
    EXECUTE format('DELETE FROM %s WHERE %I IN %s', f ->> 'p_table', a ->> 'col', v_in) USING p_vals;
    EXECUTE format('DELETE FROM %s WHERE %I IN %s', a ->> 'orphans', a ->> 'col', v_in) USING p_vals;
    v_cols := (SELECT string_agg(format('%I', c), ', ') FROM unnest(isomer._engine_anchor_columns(f)) c);
    EXECUTE format('INSERT INTO %1$s (__anchor, %2$s) SELECT m.__anchor, %3$s FROM jsonb_populate_recordset(NULL::%4$s, $1) r JOIN %5$s m ON m.k = r.%6$I',
                   f ->> 'p_table', v_cols, (SELECT string_agg(format('r.%I', c), ', ') FROM unnest(isomer._engine_anchor_columns(f)) c),
                   a ->> 'orphans', a ->> 'map', a ->> 'col') USING v_rows;
    EXECUTE format('INSERT INTO %1$s SELECT r.* FROM jsonb_populate_recordset(NULL::%1$s, $1) r WHERE NOT EXISTS (SELECT 1 FROM %2$s m WHERE m.k = r.%3$I)',
                   a ->> 'orphans', a ->> 'map', a ->> 'col') USING v_rows;

    -- The multisets of those partial rows, anchored by their partial row's correlation value.
    FOR m IN SELECT * FROM jsonb_array_elements(f -> 'minmax') LOOP
        EXECUTE format('SELECT jsonb_agg(to_jsonb(t)) FROM (SELECT DISTINCT ON (__h, value) __h, value, mult FROM %1$s WHERE __h IN %2$s ORDER BY __h, value) t',
                       m ->> 'table', v_hs) USING v_rows INTO v_mrows;
        EXECUTE format('SELECT coalesce($2, ''[]'') || coalesce(jsonb_agg(to_jsonb(t)), ''[]'') FROM %1$s t WHERE __h IN %2$s',
                       m ->> 'orphans', v_hs) USING v_rows, v_mrows INTO v_mrows;
        CONTINUE WHEN jsonb_array_length(v_mrows) = 0;
        EXECUTE format('DELETE FROM %s WHERE __h IN %s', m ->> 'table', v_hs) USING v_rows;
        EXECUTE format('DELETE FROM %s WHERE __h IN %s', m ->> 'orphans', v_hs) USING v_rows;
        EXECUTE format('INSERT INTO %1$s (__anchor, __h, value, mult) SELECT am.__anchor, r.__h, r.value, r.mult '
                       'FROM jsonb_populate_recordset(NULL::%2$s, $1) r JOIN %3$s hk ON hk.h = r.__h JOIN %4$s am ON am.k = hk.k',
                       m ->> 'table', m ->> 'orphans', v_hk, a ->> 'map') USING v_mrows, v_rows;
        EXECUTE format('INSERT INTO %1$s SELECT r.* FROM jsonb_populate_recordset(NULL::%1$s, $1) r JOIN %2$s hk ON hk.h = r.__h '
                       'WHERE NOT EXISTS (SELECT 1 FROM %3$s am WHERE am.k = hk.k)',
                       m ->> 'orphans', v_hk, a ->> 'map') USING v_mrows, v_rows;
    END LOOP;

    IF coalesce((f ->> 'anchor_children')::boolean, false) THEN
        PERFORM isomer._engine_anchor_touch((f ->> 'block')::int, v_rows);
    END IF;
END $$;

-- Before a fold: the parents' changed rows update each anchored fact's map, and the partial rows
-- of every correlation value whose anchors changed are written again under its anchors now. (A
-- fact anchored below an anchored parent follows after the fold, in _engine_anchor_nested_moves.)
CREATE OR REPLACE FUNCTION isomer._engine_anchor_moves(plan jsonb, p_relids oid[])
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    f      jsonb;
    a      jsonb;
    v_vals jsonb;
BEGIN
    FOR f IN SELECT fx FROM isomer._engine_facts(plan) fx
             WHERE fx ? 'anchor' AND NOT coalesce((fx -> 'anchor' ->> 'nested')::boolean, false)
             ORDER BY (fx ->> 'block')::int LOOP
        a := f -> 'anchor';
        CONTINUE WHEN NOT ((a ->> 'parent_relid')::oid = ANY (p_relids));
        EXECUTE format('SELECT jsonb_agg(DISTINCT k) FROM (SELECT %1$I AS k FROM %3$s WHERE %1$I IS NOT NULL AND %2$I IS NOT NULL GROUP BY %1$I, %2$I HAVING sum(__w) <> 0) x',
                       a ->> 'outer_col', a ->> 'parent_col', a ->> 'parent_staging') INTO v_vals;
        CONTINUE WHEN v_vals IS NULL;
        EXECUTE format('INSERT INTO %1$s AS m (k, __anchor, n) SELECT %2$I, %3$I, sum(__w) FROM %4$s WHERE %2$I IS NOT NULL AND %3$I IS NOT NULL GROUP BY 1, 2 HAVING sum(__w) <> 0 '
                       'ON CONFLICT (k, __anchor) DO UPDATE SET n = m.n + EXCLUDED.n',
                       a ->> 'map', a ->> 'outer_col', a ->> 'parent_col', a ->> 'parent_staging');
        EXECUTE format('DELETE FROM %s WHERE n = 0 AND k IN (SELECT x::%s FROM jsonb_array_elements_text($1) x)', a ->> 'map', a ->> 'key_type') USING v_vals;
        PERFORM isomer._engine_anchor_relocate(f, v_vals);
    END LOOP;
END $$;

-- After a fold: each fact anchored below an anchored parent, top down, takes the anchors of the
-- parent's partial rows the fold reached or moved, and moves the partial rows of every value
-- whose anchors changed.
CREATE FUNCTION isomer._engine_anchor_nested_moves(plan jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    f       jsonb;
    a       jsonb;
    v_vals  jsonb;
    v_now   jsonb;
    v_moved jsonb;
    v_in    text;
BEGIN
    IF to_regclass('pg_temp.isomer_anchor_touched') IS NULL THEN
        RETURN;
    END IF;
    FOR f IN SELECT fx FROM isomer._engine_facts(plan) fx
             WHERE coalesce((fx -> 'anchor' ->> 'nested')::boolean, false) ORDER BY (fx ->> 'block')::int LOOP
        a := f -> 'anchor';
        EXECUTE 'SELECT jsonb_agg(DISTINCT r -> $2) FROM pg_temp.isomer_anchor_touched WHERE block = $1 AND r ->> $2 IS NOT NULL'
            USING (a ->> 'parent_block')::int, a ->> 'outer_col' INTO v_vals;
        CONTINUE WHEN v_vals IS NULL;
        v_in := format('(SELECT x::%s FROM jsonb_array_elements_text($1) x)', a ->> 'key_type');
        -- Those values' anchors now: the anchors of the parent's partial rows holding them.
        EXECUTE format('SELECT coalesce(jsonb_agg(x), ''[]'') FROM (SELECT %1$I AS k, __anchor, count(*) AS n FROM %2$s WHERE __n > 0 AND %1$I IN %3$s GROUP BY 1, 2) x',
                       a ->> 'outer_col', a ->> 'parent_p_table', v_in) USING v_vals INTO v_now;
        EXECUTE format('SELECT jsonb_agg(DISTINCT k) FROM ('
                       '(SELECT k, __anchor FROM jsonb_populate_recordset(NULL::%1$s, $2) EXCEPT SELECT k, __anchor FROM %1$s WHERE k IN %2$s) UNION ALL '
                       '(SELECT k, __anchor FROM %1$s WHERE k IN %2$s EXCEPT SELECT k, __anchor FROM jsonb_populate_recordset(NULL::%1$s, $2))) d',
                       a ->> 'map', v_in) USING v_vals, v_now INTO v_moved;
        CONTINUE WHEN v_moved IS NULL;
        EXECUTE format('DELETE FROM %s WHERE k IN %s', a ->> 'map', v_in) USING v_vals;
        EXECUTE format('INSERT INTO %1$s (k, __anchor, n) SELECT k, __anchor, n FROM jsonb_populate_recordset(NULL::%1$s, $1)', a ->> 'map') USING v_now;
        PERFORM isomer._engine_anchor_relocate(f, v_moved);
    END LOOP;
END $$;

-- Before a fact's build: its map from the parent table, or (below an anchored parent, already
-- built) from the parent's partial rows; no orphans.
CREATE OR REPLACE FUNCTION isomer._engine_anchor_build(plan jsonb, f jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a jsonb := f -> 'anchor';
    m jsonb;
BEGIN
    EXECUTE format('TRUNCATE %s, %s', a ->> 'map', a ->> 'orphans');
    FOR m IN SELECT * FROM jsonb_array_elements(f -> 'minmax') LOOP
        EXECUTE format('TRUNCATE %s', m ->> 'orphans');
    END LOOP;
    IF coalesce((a ->> 'nested')::boolean, false) THEN
        EXECUTE format('INSERT INTO %1$s (k, __anchor, n) SELECT %2$I, __anchor, count(*) FROM %3$s WHERE __n > 0 AND %2$I IS NOT NULL GROUP BY 1, 2',
                       a ->> 'map', a ->> 'outer_col', a ->> 'parent_p_table');
    ELSE
        EXECUTE format('INSERT INTO %1$s (k, __anchor, n) SELECT %2$I, %3$I, count(*) FROM %4$s WHERE %2$I IS NOT NULL AND %3$I IS NOT NULL GROUP BY 1, 2',
                       a ->> 'map', a ->> 'outer_col', a ->> 'parent_col', a ->> 'parent_table');
    END IF;
END $$;

-- Distribute a compiled nested state (see 0.26.0). New: multisets and nested anchors (in
-- _engine_anchor_fact), a parent with anchored blocks below it notes what its folds reach, and a
-- key query driven by an anchored parent's changed rows, kept on every node, leaves out the
-- anchor of the blocks below it (they are only in its output, which finding keys drops).
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
    v_j     integer;
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

    -- Parents before the blocks below them, so a nested anchor finds its parent's partial rows.
    FOR v_i IN SELECT o - 1 FROM jsonb_array_elements(plan -> 'facts') WITH ORDINALITY x(fx, o) ORDER BY (fx ->> 'block')::int LOOP
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

    -- A fact with a block anchored below it notes the partial rows its folds reach.
    FOR v_i IN 0 .. jsonb_array_length(plan -> 'facts') - 1 LOOP
        IF EXISTS (SELECT 1 FROM jsonb_array_elements(plan -> 'facts') c
                   WHERE coalesce((c -> 'anchor' ->> 'nested')::boolean, false)
                     AND (c -> 'anchor' ->> 'parent_block')::int = (plan -> 'facts' -> v_i ->> 'block')::int) THEN
            plan := jsonb_set(plan, ARRAY['facts', v_i::text, 'anchor_children'], 'true');
        END IF;
    END LOOP;

    -- An anchored block is told its parent's shard value equals its own: the derivation and the
    -- key queries are generated again with that condition (not where the anchored table's changed
    -- rows drive, read from a table on every node, nor below them).
    IF v_cfgs IS DISTINCT FROM plan -> 'block_config' THEN
        UPDATE isomer.projections SET derive_query = isomer._engine_sql_blocks(p.query, 'derive', v_cfgs, 0, NULL) WHERE name = p.name;
        FOR v_i IN 0 .. jsonb_array_length(plan -> 'key_sqls') - 1 LOOP
            m := plan -> 'key_sqls' -> v_i;
            v_kc := jsonb_set(v_cfgs, ARRAY[m ->> 'block', 'rels', ((m ->> 'rti')::int - 1)::text],
                              to_jsonb((plan -> 'sources' -> (m ->> 'relid') ->> 'staging')::regclass::oid::bigint));
            FOR v_j IN 1 .. jsonb_array_length(v_kc) - 1 LOOP
                CONTINUE WHEN NOT (v_kc -> v_j ? 'anchor');
                IF (v_j = (m ->> 'block')::int AND (v_kc -> v_j ->> 'fact')::int = (m ->> 'rti')::int)
                   OR ((s -> 'blocks' -> v_j ->> 'parent')::int = (m ->> 'block')::int
                       AND v_kc -> v_j -> 'anchor' ->> 'outer_col' = '__anchor'
                       AND (v_kc -> v_j -> 'anchor' ->> 'outer_rti')::int = (m ->> 'rti')::int) THEN
                    v_kc := v_kc #- ARRAY[v_j::text, 'anchor'];
                END IF;
            END LOOP;
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

-- Fold queued deltas (see 0.26.0); after the facts, the blocks anchored below anchored parents
-- follow their parents' partial rows.
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
    PERFORM isomer._engine_anchor_untouch();
    PERFORM isomer._engine_anchor_moves(plan, v_relids);

    FOR f IN SELECT * FROM isomer._engine_facts(plan) LOOP
        CONTINUE WHEN NOT ((f ->> 'fact_relid')::oid = ANY (v_relids));
        EXECUTE format('TRUNCATE %s', f ->> 'fd_table');
        EXECUTE format('INSERT INTO %s SELECT * FROM (%s) d', f ->> 'fd_table', f ->> 'fact_stage_sql');
        PERFORM isomer._engine_fold_fact(f, f ->> 'fd_table');
    END LOOP;
    -- Below an anchored parent, the anchors are its partial rows, as this fold left them.
    PERFORM isomer._engine_anchor_nested_moves(plan);
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

-- Build the state from the tables (see 0.26.0), parents first, each anchored fact's map just
-- before it is folded.
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
    PERFORM isomer._engine_anchor_untouch();
    FOR f IN SELECT fx FROM isomer._engine_facts(plan) fx ORDER BY (fx ->> 'block')::int LOOP
        IF f ? 'anchor' THEN
            PERFORM isomer._engine_anchor_build(plan, f);
        END IF;
        PERFORM isomer._engine_fold_fact(f, '(' || (f ->> 'fact_base_sql') || ')');
    END LOOP;
    PERFORM isomer._engine_anchor_untouch();
END $$;
