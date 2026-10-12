-- IsomerDB 0.17.0: incremental maintenance for projections that aggregate across shards (PRD
-- FR-P6, FR-S5), and a cost model that picks each projection's maintenance strategy (FR-S7).
--
--   * strategy => 'incremental': each change to a source is captured as a row delta (on the node
--     where the row is written, pulled to the coordinator exactly once). The aggregated table is
--     folded into per-group partial aggregates, the tables it joins into copies of the columns
--     the query reads, and only the keys a delta can reach are re-derived, from that state. A
--     write costs the same whether its group holds ten rows or ten million, and a table the key
--     only reaches through a join no longer re-derives every key. The compiler
--     (isomer._engine_analyze / _engine_sql, in the isomer_router library) works on Postgres's
--     own parse tree; the certificate names the rules it relied on.
--   * strategy => 'auto' (the default for new projections) asks isomer.choose_strategy, which
--     estimates the cost of re-deriving one key and picks incremental maintenance where it is
--     cheaper and supported. isomer.strategy_advice() shows what it would pick for each
--     projection now; isomer.alter_projection(name, strategy => ...) switches.
--   * Projections declared this way need no `sources` mappings.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.17.0'" to load this file. \quit

ALTER TABLE isomer.projections
    ADD COLUMN strategy        text NOT NULL DEFAULT 'recompute' CHECK (strategy IN ('recompute', 'incremental')),
    ADD COLUMN strategy_reason text,
    ADD COLUMN derive_query    text,   -- incremental: how a key's value is derived from the state
    ADD COLUMN truth_query     text;   -- what the tables produce, when the cluster can't run the query as written

-- What the compiler produced for an incremental projection: the state it keeps and the SQL that
-- maintains it. Rebuilt from the projection's query whenever the projection is installed.
CREATE TABLE isomer.engine_plans (
    projection  text PRIMARY KEY REFERENCES isomer.projections (name) ON DELETE CASCADE,
    plan        jsonb NOT NULL,
    compiled_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Row changes captured for incremental projections, waiting to be folded into their state: the
-- row's image, +1 when it arrives and -1 when it leaves. A delta with no image asks for a rebuild
-- (the source was truncated).
CREATE TABLE isomer.deltas (
    id          bigserial PRIMARY KEY,
    projection  text NOT NULL REFERENCES isomer.projections (name) ON DELETE CASCADE,
    source      text NOT NULL,
    weight      smallint NOT NULL,
    image       jsonb,
    token       bigint NOT NULL DEFAULT 0,
    enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX deltas_projection ON isomer.deltas (projection, id);
CREATE INDEX deltas_token ON isomer.deltas (projection, token);

-- The same, on a worker node (which has no catalog), until the coordinator pulls them.
CREATE TABLE isomer.node_deltas (
    id          bigserial PRIMARY KEY,
    projection  text NOT NULL,
    source      text NOT NULL,
    weight      smallint NOT NULL,
    image       jsonb,
    token       bigint NOT NULL DEFAULT 0,
    enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Deltas are counted, not just noted, so each must arrive exactly once. A pull takes a worker's
-- deltas in a prepared transaction there, commits them here together with its transaction id,
-- then commits the worker's. Whoever finds a pull still prepared on a worker finishes it the
-- way this table says: committed here means commit there, absent means roll back.
CREATE TABLE isomer.delta_pulls (
    gid       text PRIMARY KEY,
    nodeid    integer NOT NULL,
    rows      integer NOT NULL,
    pulled_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- A statement that writes several shards runs their triggers over parallel connections of one
-- distributed transaction. When two of them queued the same key with INSERT ... ON CONFLICT, the
-- second waited for the first's uncommitted row, which could not commit until the statement
-- finished: Citus's deadlock detector cancelled it. Capture now only appends (here to key_log, on
-- workers to node_queue, both without a unique key), and the drain, one session, merges what has
-- committed into pending. A write still uncommitted when the drain merges is simply merged by the
-- next drain, so nothing is missed.
CREATE TABLE isomer.key_log (
    id          bigserial PRIMARY KEY,
    projection  text NOT NULL REFERENCES isomer.projections (name) ON DELETE CASCADE,
    key         text NOT NULL,
    token       bigint NOT NULL DEFAULT 0,
    enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX key_log_projection ON isomer.key_log (projection, id);
CREATE INDEX key_log_token ON isomer.key_log (projection, token);
REVOKE ALL ON isomer.key_log FROM PUBLIC;

ALTER TABLE isomer.node_queue DROP CONSTRAINT node_queue_pkey;
ALTER TABLE isomer.node_queue ADD COLUMN id bigserial PRIMARY KEY;

CREATE SCHEMA isomer_state;
COMMENT ON SCHEMA isomer_state IS 'IsomerDB: state kept by incrementally maintained projections';

INSERT INTO isomer.settings VALUES ('strategy.incremental_min_cost', '200') ON CONFLICT (key) DO NOTHING;

CREATE FUNCTION isomer._engine_analyze(query text)
RETURNS jsonb LANGUAGE C STRICT AS 'isomer_router', 'isomer_engine_analyze';

CREATE FUNCTION isomer._engine_sql(query text, mode text, fact integer, driving integer,
                                   rels oid[], templates text[], fact_columns text[])
RETURNS text LANGUAGE C AS 'isomer_router', 'isomer_engine_sql';

-- ── Shape: can this query be maintained incrementally, and how ──────────────

-- Does a type have the equality grouping and conflict detection need?
CREATE FUNCTION isomer._has_equality(p_type text)
RETURNS boolean LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    EXECUTE format('SELECT NULL::%1$s IS NOT DISTINCT FROM NULL::%1$s', p_type);
    RETURN true;
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END $$;

-- Is this set of columns (attribute numbers) a unique key of the table, by a unique index or
-- primary key over a subset of them?
CREATE FUNCTION isomer._is_unique(p_rel oid, p_cols text[])
RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_index i
        WHERE i.indrelid = p_rel AND i.indisunique AND i.indpred IS NULL AND i.indexprs IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM unnest((i.indkey::int2[])[0:i.indnkeyatts - 1]) k
              WHERE NOT EXISTS (SELECT 1 FROM pg_attribute a
                                WHERE a.attrelid = p_rel AND a.attnum = k AND a.attname = ANY (p_cols))))
$$;

CREATE FUNCTION isomer._primary_key(p_rel oid)
RETURNS text[] LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT array_agg(a.attname::text ORDER BY k.n)
    FROM pg_index i
    CROSS JOIN LATERAL unnest((i.indkey::int2[])[0:i.indnkeyatts - 1]) WITH ORDINALITY k(attnum, n)
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
    WHERE i.indrelid = p_rel AND i.indisprimary
$$;

-- What the engine needs to know about a query, or why it cannot maintain it:
--   {"ok": false, "reason": "..."}  or
--   {"ok": true, "fact": rti, "fact_relid": oid, "nullable_fact": bool, "pkeys": [columns],
--    "aggs": [{"i", "cls", "argtype", "restype", "stype"}],
--    "rtes": [{"rti", "relid", "table", "role": "fact"|"dim"}],
--    "capture": {"<relid>": [columns]}, "dims": {"<relid>": [columns]}, "edges": [...],
--    "group": [{"rti", "col"}]}
-- The rule it enforces: one table (the fact) holds every aggregate's input, and every join
-- leading away from it lands on a unique key, so each fact row joins at most one row of the rest
-- and its aggregates can be computed per group before the join.
CREATE FUNCTION isomer._engine_shape(p_query text, p_key text, p_kind text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a          jsonb;
    v_rte      jsonb;
    v_x        jsonb;
    v_fact     integer;
    v_rtis     integer[];
    v_aggrtis  integer[];
    v_reason   text;
    v_seen     integer[];
    v_front    integer[];
    v_node     integer;
    v_other    integer;
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
    v_numeric  text[] := ARRAY['smallint', 'integer', 'bigint', 'numeric', 'real', 'double precision'];
BEGIN
    IF p_kind NOT IN ('document', 'cache') THEN
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

    -- Aggregates: each must be decomposable into sums, counts or a multiset.
    IF jsonb_array_length(a -> 'aggs') = 0 THEN
        RETURN jsonb_build_object('ok', false, 'reason', 'it has no aggregate: re-deriving a key reads only that key''s rows, which is already the cheapest way');
    END IF;
    FOR v_x IN SELECT * FROM jsonb_array_elements(a -> 'aggs') LOOP
        IF v_x ->> 'schema' <> 'pg_catalog' OR v_x ->> 'kind' <> 'n'
           OR v_x ->> 'fn' NOT IN ('count', 'sum', 'avg', 'min', 'max', 'bool_and', 'bool_or', 'every') THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('it uses the aggregate %s (count, sum, avg, min, max, bool_and and bool_or are maintained incrementally)', v_x ->> 'fn'));
        END IF;
        IF (v_x ->> 'distinct')::boolean THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('it uses %s(DISTINCT ...)', v_x ->> 'fn'));
        END IF;
        IF (v_x ->> 'ordered')::boolean THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('it uses %s(... ORDER BY ...)', v_x ->> 'fn'));
        END IF;
        IF (v_x ->> 'nargs')::int > 1 THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('it uses %s with several arguments', v_x ->> 'fn'));
        END IF;
        IF v_x ->> 'fn' <> 'count' AND (SELECT t.typtype = 'c' OR t.oid = 'record'::regtype FROM pg_type t WHERE t.oid = (v_x ->> 'argtype')::regtype) THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('it uses %s over a whole row', v_x ->> 'fn'));
        END IF;
        IF v_x ->> 'fn' IN ('sum', 'avg') AND NOT (v_x ->> 'argtype' = ANY (v_numeric)) THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('it uses %s over %s (numeric types only)', v_x ->> 'fn', v_x ->> 'argtype'));
        END IF;
        IF v_x ->> 'fn' IN ('min', 'max') AND NOT isomer._has_equality(v_x ->> 'argtype') THEN
            RETURN jsonb_build_object('ok', false, 'reason', format('it uses %s over %s, which has no ordering', v_x ->> 'fn', v_x ->> 'argtype'));
        END IF;
        v_cls := CASE
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
        v_aggs := v_aggs || jsonb_build_object('i', (v_x ->> 'i')::int, 'cls', v_cls, 'argtype', v_x ->> 'argtype',
                                               'restype', v_x ->> 'restype', 'stype', v_stype);
        v_aggrtis := v_aggrtis || ARRAY(SELECT jsonb_array_elements_text(v_x -> 'rtis')::int);
    END LOOP;
    v_aggrtis := ARRAY(SELECT DISTINCT unnest(v_aggrtis));
    IF cardinality(v_aggrtis) > 1 THEN
        RETURN jsonb_build_object('ok', false, 'reason', 'its aggregates read more than one table');
    END IF;

    -- Grouping and the key.
    IF (a ->> 'group_expr')::boolean THEN
        RETURN jsonb_build_object('ok', false, 'reason', 'it groups by an expression; group by columns');
    END IF;
    SELECT t -> 'col' INTO v_key FROM jsonb_array_elements(a -> 'target') t WHERE t ->> 'name' = p_key;
    IF jsonb_array_length(a -> 'group') > 0 AND (v_key IS NULL OR jsonb_typeof(v_key) = 'null') THEN
        RETURN jsonb_build_object('ok', false, 'reason', format('the key %s is not one of the grouping columns', p_key));
    END IF;

    -- The fact: the table the aggregates read, or (count(*) only) the one the joins lead away from.
    IF cardinality(v_aggrtis) = 1 THEN
        v_fact := v_aggrtis[1];
        v_reason := isomer._engine_tree(a, v_fact);
    ELSE
        FOREACH v_node IN ARRAY v_rtis LOOP
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

    -- The fact's columns kept in its partial aggregates: everything read outside aggregates and
    -- outside its own filters (grouping, output, join conditions, conditions shared with others).
    v_pkeys := ARRAY(
        SELECT DISTINCT c ->> 'col' FROM (
            SELECT value AS c FROM jsonb_array_elements(a -> 'nonagg')
            UNION ALL
            SELECT col FROM jsonb_array_elements(a -> 'conjuncts') cj, jsonb_array_elements(cj -> 'cols') col
            WHERE cj -> 'rtis' <> jsonb_build_array(v_fact)) s
        WHERE jsonb_typeof(c) = 'object' AND (c ->> 'rti')::int = v_fact
        ORDER BY 1);

    -- Relations, the columns captured from each, and the columns kept for each joined table.
    FOR v_rte IN SELECT * FROM jsonb_array_elements(a -> 'rtes') LOOP
        v_relid := (v_rte ->> 'relid')::oid;
        v_cols := ARRAY(SELECT DISTINCT c ->> 'col' FROM jsonb_array_elements(a -> 'columns') c
                        WHERE (c ->> 'rti')::int = (v_rte ->> 'rti')::int ORDER BY 1);
        v_rtes := v_rtes || jsonb_build_object('rti', (v_rte ->> 'rti')::int, 'relid', v_relid,
                                               'table', isomer._qualified(v_relid::regclass),
                                               'role', CASE WHEN (v_rte ->> 'rti')::int = v_fact THEN 'fact' ELSE 'dim' END);
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
        'nullable_fact', v_nullable, 'pkeys', to_jsonb(v_pkeys), 'aggs', v_aggs, 'rtes', v_rtes,
        'capture', v_capture, 'dims', v_dims, 'group', a -> 'group', 'key', v_key,
        'edges', (SELECT coalesce(jsonb_agg(c -> 'equi'), '[]') FROM jsonb_array_elements(a -> 'conjuncts') c
                  WHERE jsonb_typeof(c -> 'equi') = 'array'),
        'joins', a -> 'joins',
        'max_rti', (SELECT max(n) FROM (SELECT (e ->> 'rti')::int AS n FROM jsonb_array_elements(a -> 'rtes') e
                                      UNION ALL SELECT jsonb_array_length(a -> 'rtes')) s));
END $$;

-- Walk the joins outward from a candidate fact. Every table must be reached through equalities,
-- and each step must land on a unique key of the table it reaches. Returns why not, or NULL.
CREATE FUNCTION isomer._engine_tree(a jsonb, p_root integer)
RETURNS text LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    v_rtis  integer[] := ARRAY(SELECT (r ->> 'rti')::int FROM jsonb_array_elements(a -> 'rtes') r);
    v_seen  integer[] := ARRAY[p_root];
    v_front integer[] := ARRAY[p_root];
    v_node  integer;
    v_next  integer;
    v_cols  text[];
    v_relid oid;
BEGIN
    WHILE cardinality(v_front) > 0 LOOP
        v_node := v_front[1];
        v_front := v_front[2:];
        FOREACH v_next IN ARRAY v_rtis LOOP
            CONTINUE WHEN v_next = ANY (v_seen);
            -- The columns of v_next that equal columns of v_node.
            v_cols := ARRAY(
                SELECT DISTINCT CASE WHEN (e -> 0 ->> 'rti')::int = v_next THEN e -> 0 ->> 'col' ELSE e -> 1 ->> 'col' END
                FROM jsonb_array_elements(a -> 'conjuncts') c, LATERAL (SELECT c -> 'equi' AS e) s
                WHERE jsonb_typeof(c -> 'equi') = 'array'
                  AND ((e -> 0 ->> 'rti')::int, (e -> 1 ->> 'rti')::int) IN ((v_node, v_next), (v_next, v_node)));
            CONTINUE WHEN cardinality(v_cols) = 0;
            v_relid := (SELECT (r ->> 'relid')::oid FROM jsonb_array_elements(a -> 'rtes') r WHERE (r ->> 'rti')::int = v_next);
            IF NOT isomer._is_unique(v_relid, v_cols) THEN
                RETURN format('the join to %s is on %s, which is not a unique key there: a row of %s could join several of its rows, so aggregates cannot be kept per group',
                              v_relid::regclass, array_to_string(v_cols, ', '),
                              (SELECT (r ->> 'relid')::oid::regclass FROM jsonb_array_elements(a -> 'rtes') r WHERE (r ->> 'rti')::int = p_root));
            END IF;
            v_seen := v_seen || v_next;
            v_front := v_front || v_next;
        END LOOP;
    END LOOP;
    IF cardinality(v_seen) < cardinality(v_rtis) THEN
        RETURN format('%s is not joined to the rest by an equality',
                      (SELECT string_agg((r ->> 'relid')::oid::regclass::text, ', ') FROM jsonb_array_elements(a -> 'rtes') r
                       WHERE NOT ((r ->> 'rti')::int = ANY (v_seen))));
    END IF;
    RETURN NULL;
END $$;

-- ── State ───────────────────────────────────────────────────────────────────

CREATE FUNCTION isomer._engine_prefix(p_projection text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT 'e' || left(md5(p_projection), 12)
$$;

-- A relation's columns in attribute order, as a select list (from p_alias for the columns in
-- p_have, NULL of the right type otherwise) or as column definitions. Relations built this way
-- line up with the source table attribute by attribute (dropped columns included), so the
-- compiler can swap one for the other in a parse tree without renumbering anything.
CREATE FUNCTION isomer._aligned(p_rel oid, p_have text[], p_alias text, p_defs boolean DEFAULT false)
RETURNS text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT string_agg(
        CASE WHEN p_defs AND a.attisdropped THEN format('%I integer', '__dropped_' || a.attnum)
             WHEN p_defs THEN format('%I %s', a.attname, format_type(a.atttypid, a.atttypmod))
             WHEN a.attisdropped THEN format('NULL::integer AS %I', '__dropped_' || a.attnum)
             WHEN a.attname = ANY (p_have) THEN format('%s.%I', p_alias, a.attname)
             ELSE format('NULL::%s AS %I', format_type(a.atttypid, a.atttypmod), a.attname) END,
        ', ' ORDER BY a.attnum)
    FROM pg_attribute a WHERE a.attrelid = p_rel AND a.attnum > 0
$$;

CREATE FUNCTION isomer._coltype(p_rel oid, p_col text)
RETURNS text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = p_rel AND attname = p_col AND NOT attisdropped
$$;

CREATE FUNCTION isomer._engine_drop_state(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT c.oid::regclass AS rel, c.relkind FROM pg_class c
             WHERE c.relnamespace = 'isomer_state'::regnamespace
               AND c.relname LIKE isomer._engine_prefix(p_projection) || '\_%' AND c.relkind IN ('v', 'r')
             ORDER BY c.relkind = 'r'
    LOOP
        EXECUTE format('DROP %s IF EXISTS %s CASCADE', CASE r.relkind WHEN 'v' THEN 'VIEW' ELSE 'TABLE' END, r.rel);
    END LOOP;
    DELETE FROM isomer.engine_plans WHERE projection = p_projection;
END $$;

-- Compile a projection for incremental maintenance: create its state and staging relations and
-- generate the statements that maintain them. Returns the plan (also kept in engine_plans).
CREATE FUNCTION isomer._engine_compile(p_projection text)
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
            WHEN 'bool_or' THEN format('(CASE WHEN coalesce(sum(__c%1$s), 0) = 0 THEN NULL ELSE coalesce(sum(__t%1$s), 0) > 0 END)', v_i) END;
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
    v_fstage := isomer._engine_sql(p.query, 'fact', v_fact, NULL, v_k, NULL, v_pkeys);
    v_fbase := isomer._engine_sql(p.query, 'fact', v_fact, NULL, NULL, NULL, v_pkeys);
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

-- The partial aggregates of a set of fact rows (a relation or subquery shaped like the 'fact'
-- statement: kept columns, __a<i>/__f<i> per aggregate, __w), one row per group of the kept
-- columns: __h (the group's hash), the columns, __n and each aggregate's sums. With p_fresh, min
-- and max are computed directly (for a fresh derivation from the tables, not for folding).
CREATE FUNCTION isomer._engine_p_select(plan jsonb, p_src text, p_fresh boolean DEFAULT false)
RETURNS text LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    v_keys text := (SELECT string_agg(format('d.%I', c), ', ') FROM jsonb_array_elements_text(plan -> 'pkeys') c);
    v_sel  text := 'sum(d.__w)::bigint AS __n';
    x      jsonb;
    v_i    integer;
BEGIN
    FOR x IN SELECT * FROM jsonb_array_elements(plan -> 'aggs') LOOP
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

-- Fold a set of fact rows into the partial aggregates (and the multisets behind min and max).
CREATE FUNCTION isomer._engine_fold_fact(plan jsonb, p_src text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_keys  text := (SELECT string_agg(format('d.%I', c), ', ') FROM jsonb_array_elements_text(plan -> 'pkeys') c);
    v_hash  text := format('md5(jsonb_build_array(%s)::text)', coalesce((SELECT string_agg(format('d.%I', c), ', ') FROM jsonb_array_elements_text(plan -> 'pkeys') c), ''));
    v_cols  text := '__h' || coalesce((SELECT string_agg(format(', %I', c), '') FROM jsonb_array_elements_text(plan -> 'pkeys') c), '') || ', __n';
    v_set   text := '__n = t.__n + EXCLUDED.__n';
    x       jsonb;
    v_i     integer;
BEGIN
    FOR x IN SELECT * FROM jsonb_array_elements(plan -> 'aggs') LOOP
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

-- Fold a joined table's changed rows (its staging table) into the columns kept for it. Rows are
-- netted first, so an update (old row out, new row in) is one replacement. With a primary key,
-- a key's row is the newest one inserted and its weight counts how many times it is present.
CREATE FUNCTION isomer._engine_fold_dim(src jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_cols  text := (SELECT string_agg(format('%I', c), ', ') FROM jsonb_array_elements_text(src -> 'cols') c);
    v_xcols text := (SELECT string_agg(format('x.%I', c), ', ') FROM jsonb_array_elements_text(src -> 'cols') c);
    v_pk    text := (SELECT string_agg(format('%I', c), ', ') FROM jsonb_array_elements_text(src -> 'pk') c);
    v_upd   text := (SELECT string_agg(format('%1$I = EXCLUDED.%1$I', c), ', ') FROM jsonb_array_elements_text(src -> 'cols') c
                     WHERE NOT (src -> 'pk' ? c));
    v_net   text;
BEGIN
    v_net := format('SELECT %s, sum(__w) AS __w FROM %s GROUP BY %s', v_cols, src ->> 'staging', v_cols);
    IF jsonb_typeof(src -> 'pk') = 'array' THEN
        EXECUTE format('UPDATE %1$s s SET __w = s.__w + x.__w FROM (%2$s) x WHERE x.__w < 0 AND (%3$s) = (%4$s)',
                       src ->> 'state', v_net,
                       (SELECT string_agg(format('s.%I', c), ', ') FROM jsonb_array_elements_text(src -> 'pk') c),
                       (SELECT string_agg(format('x.%I', c), ', ') FROM jsonb_array_elements_text(src -> 'pk') c));
        EXECUTE format('DELETE FROM %s WHERE __w <= 0', src ->> 'state');
        EXECUTE format('INSERT INTO %1$s AS s (%2$s, __w) SELECT %3$s, x.__w FROM (%4$s) x WHERE x.__w > 0 '
                       'ON CONFLICT (%5$s) DO UPDATE SET %6$s__w = s.__w + EXCLUDED.__w',
                       src ->> 'state', v_cols, v_xcols, v_net, v_pk, coalesce(v_upd || ', ', ''));
    ELSE
        EXECUTE format('INSERT INTO %1$s AS s (__h, %2$s, __w) SELECT md5(jsonb_build_array(%3$s)::text), %3$s, x.__w FROM (%4$s) x WHERE x.__w <> 0 '
                       'ON CONFLICT (__h) DO UPDATE SET __w = s.__w + EXCLUDED.__w',
                       src ->> 'state', v_cols, v_xcols, v_net);
        EXECUTE format('DELETE FROM %s WHERE __w = 0', src ->> 'state');
    END IF;
END $$;

-- Build an incremental projection's state from its tables. Run in the transaction that creates
-- the capture triggers (whose creation locks every shard against writes until it commits), or
-- with the sources locked, so no change falls between the state and the deltas.
CREATE FUNCTION isomer._engine_build(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    plan jsonb := (SELECT e.plan FROM isomer.engine_plans e WHERE e.projection = p_projection);
    src  jsonb;
    x    jsonb;
    v_cols text;
BEGIN
    EXECUTE format('TRUNCATE %s', plan ->> 'p_table');
    FOR x IN SELECT * FROM jsonb_array_elements(plan -> 'minmax') LOOP
        EXECUTE format('TRUNCATE %s', x ->> 'table');
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
    PERFORM isomer._engine_fold_fact(plan, '(' || (plan ->> 'fact_base_sql') || ')');
END $$;

-- Rebuild after a TRUNCATE of a source (which carries no rows): lock the sources, drop every
-- delta still queued for the projection (here and on the workers: the tables already hold
-- them), build the state again and re-derive every key.
CREATE FUNCTION isomer._engine_rebuild(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    s record;
BEGIN
    FOR s IN SELECT source_table FROM isomer.projection_sources WHERE projection = p_projection LOOP
        EXECUTE format('LOCK TABLE %s IN SHARE MODE', s.source_table);
    END LOOP;
    PERFORM run_command_on_workers(format('DELETE FROM isomer.node_deltas WHERE projection = %L', p_projection));
    DELETE FROM isomer.deltas WHERE projection = p_projection;
    PERFORM isomer._engine_build(p_projection);
    PERFORM isomer._full_refresh(p_projection);   -- every key again, and those that disappeared
    PERFORM isomer._log('info', p_projection, 'a source was truncated: state rebuilt');
END $$;

-- The keys the staged rows of one relation reach, through the state as it is now.
CREATE FUNCTION isomer._engine_touched(plan jsonb, p_key text, p_relids oid[])
RETURNS text[] LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    k      jsonb;
    v_keys text[] := '{}';
BEGIN
    FOR k IN SELECT * FROM jsonb_array_elements(plan -> 'key_sqls') LOOP
        CONTINUE WHEN NOT ((k ->> 'relid')::oid = ANY (p_relids));
        v_keys := v_keys || ARRAY(SELECT DISTINCT q FROM unnest(isomer._engine_keys_of(k ->> 'sql', p_key)) q);
    END LOOP;
    RETURN v_keys;
END $$;

CREATE FUNCTION isomer._engine_keys_of(p_sql text, p_key text)
RETURNS text[] LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v text[];
BEGIN
    EXECUTE format('SELECT array_agg(DISTINCT q.%I::text) FROM (%s) q WHERE q.%I IS NOT NULL', p_key, p_sql, p_key) INTO v;
    RETURN coalesce(v, '{}');
END $$;

-- Fold the deltas queued for an incremental projection into its state, and queue the keys they
-- reach (before and after folding) for the drain, carrying the oldest write token. Folds of one
-- projection are serialised. Returns the number of deltas folded.
CREATE FUNCTION isomer._engine_fold(p_projection text, p_batch integer DEFAULT 20000)
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
    v_rel    oid;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    SELECT e.plan INTO plan FROM isomer.engine_plans e WHERE e.projection = p.name;
    IF plan IS NULL THEN
        RETURN 0;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtext('isomer.engine'), hashtext(p.name));

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

    IF (plan ->> 'fact_relid')::oid = ANY (v_relids) THEN
        EXECUTE format('TRUNCATE %s', plan ->> 'fd_table');
        EXECUTE format('INSERT INTO %s SELECT * FROM (%s) d', plan ->> 'fd_table', plan ->> 'fact_stage_sql');
        PERFORM isomer._engine_fold_fact(plan, plan ->> 'fd_table');
    END IF;
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

ALTER TABLE isomer.projection_stats ADD COLUMN deltas_folded bigint NOT NULL DEFAULT 0;

-- ── Capture ─────────────────────────────────────────────────────────────────

-- A reference table's change runs on every node holding a copy; only the copy in the lowest node
-- group records it. Shards are named <table>_<shard id>.
CREATE FUNCTION isomer._first_placement(p_relname text)
RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT coalesce((SELECT min(pl.groupid) FROM pg_dist_placement pl
                     WHERE pl.shardid = substring(p_relname FROM '_(\d+)$')::bigint)
                    = (SELECT groupid FROM pg_dist_local_group), true)
$$;

CREATE FUNCTION isomer._pick(p_row jsonb, p_columns text[])
RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
    SELECT coalesce(pg_catalog.jsonb_object_agg(k, v), '{}') FROM pg_catalog.jsonb_each(p_row) e(k, v) WHERE k = ANY (p_columns)
$$;

-- Capture trigger for incremental projections: records each change as row images, -1 for the
-- old row and +1 for the new, in the writing transaction. On a worker (no catalog) the deltas
-- wait in node_deltas for the coordinator. An update that changes none of the columns the
-- projection reads records nothing.
--   arguments: projection, source table, 'r' for a reference table, the columns read
CREATE FUNCTION isomer._on_source_delta()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    v_projection text := TG_ARGV[0];
    v_source     text := TG_ARGV[1];
    v_columns    text[] := nullif(TG_ARGV[3], '')::text[];
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
    END IF;
    IF EXISTS (SELECT 1 FROM isomer.projections WHERE name = v_projection) THEN
        INSERT INTO isomer.deltas (projection, source, weight, image, token)
        SELECT v_projection, v_source, d.w, d.image, v_token
        FROM (VALUES (-1, v_old), (1, v_new), (0, NULL)) d(w, image)
        WHERE (TG_OP = 'TRUNCATE') = (d.w = 0) AND (d.w = 0 OR d.image IS NOT NULL);
    ELSE
        INSERT INTO isomer.node_deltas (projection, source, weight, image, token)
        SELECT v_projection, v_source, d.w, d.image, v_token
        FROM (VALUES (-1, v_old), (1, v_new), (0, NULL)) d(w, image)
        WHERE (TG_OP = 'TRUNCATE') = (d.w = 0) AND (d.w = 0 OR d.image IS NOT NULL);
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
    SELECT e.plan INTO v_plan FROM isomer.engine_plans e WHERE e.projection = p.name;
    FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger, s.source_table);
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger || '_truncate', s.source_table);
        IF p.strategy = 'incremental' THEN
            v_args := format('%L, %L, %L, %L', p.name, s.source_table,
                             CASE WHEN (isomer._distribution(s.source_table::regclass)).method = 'n' THEN 'r' ELSE '' END,
                             ARRAY(SELECT jsonb_array_elements_text(v_plan -> 'capture' -> (s.source_table::regclass::oid)::text))::text);
            EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW '
                           'EXECUTE FUNCTION isomer._on_source_delta(%s)', v_trigger, s.source_table, v_args);
            EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT '
                           'EXECUTE FUNCTION isomer._on_source_delta(%s)', v_trigger || '_truncate', s.source_table, v_args);
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

-- ── Pulling deltas from workers, exactly once ───────────────────────────────

CREATE PROCEDURE isomer.pull_node_deltas(p_batch integer DEFAULT 10000)
LANGUAGE plpgsql AS $$
DECLARE
    w       record;
    g       text;
    v_conn  text;
    v_gid   text;
    v_rows  jsonb;
    v_n     integer;
    v_ok    boolean;
    v_round integer;
BEGIN
    IF NOT pg_catalog.citus_is_coordinator() THEN
        RETURN;
    END IF;
    -- One puller at a time: another one, seeing this one's prepared pull before it is recorded
    -- here, would roll it back.
    IF NOT pg_catalog.pg_try_advisory_lock(pg_catalog.hashtext('isomer.pull_node_deltas')) THEN
        RETURN;
    END IF;
    FOR w IN SELECT * FROM isomer._workers() LOOP
        v_conn := pg_catalog.concat('isomer_pulld_', w.nodeid);
        BEGIN
            IF NOT (v_conn = ANY (coalesce(public.dblink_get_connections(), '{}'::text[]))) THEN
                PERFORM public.dblink_connect(v_conn, w.conninfo);
            END IF;
            -- Finish what an interrupted pull left prepared on the worker.
            FOR g IN SELECT t.gid FROM public.dblink(v_conn,
                         $q$SELECT gid FROM pg_catalog.pg_prepared_xacts WHERE gid LIKE 'isomer\_pull\_%' AND database = pg_catalog.current_database()$q$)
                         AS t(gid text) LOOP
                IF EXISTS (SELECT 1 FROM isomer.delta_pulls d WHERE d.gid = g) THEN
                    PERFORM public.dblink_exec(v_conn, pg_catalog.format('COMMIT PREPARED %L', g));
                ELSE
                    PERFORM public.dblink_exec(v_conn, pg_catalog.format('ROLLBACK PREPARED %L', g));
                END IF;
            END LOOP;
            DELETE FROM isomer.delta_pulls d WHERE d.nodeid = w.nodeid;
        EXCEPTION WHEN OTHERS THEN
            PERFORM isomer._log('error', pg_catalog.concat('node ', w.nodeid), pg_catalog.format('resolving delta pulls failed: %s', SQLERRM));
            BEGIN
                PERFORM public.dblink_disconnect(v_conn);
            EXCEPTION WHEN OTHERS THEN
                NULL;
            END;
            CONTINUE;
        END;
        COMMIT;

        v_round := 0;
        LOOP
            v_round := v_round + 1;
            v_ok := true;
            v_n := 0;
            v_gid := pg_catalog.format('isomer_pull_%s_%s', w.nodeid, (extract(epoch FROM pg_catalog.clock_timestamp()) * 1000000)::bigint);
            BEGIN
                PERFORM public.dblink_exec(v_conn, 'BEGIN');
                SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(d) ORDER BY d.id), count(*) INTO v_rows, v_n
                FROM public.dblink(v_conn, pg_catalog.format(
                         'DELETE FROM isomer.node_deltas WHERE id IN (SELECT id FROM isomer.node_deltas ORDER BY id LIMIT %s) '
                         'RETURNING id, projection, source, weight, image, token, enqueued_at', p_batch))
                     AS d(id bigint, projection text, source text, weight smallint, image jsonb, token bigint, enqueued_at timestamptz);
                IF v_n = 0 THEN
                    PERFORM public.dblink_exec(v_conn, 'ROLLBACK');
                ELSE
                    PERFORM public.dblink_exec(v_conn, pg_catalog.format('PREPARE TRANSACTION %L', v_gid));
                    -- In the worker's order, so changes to one row keep theirs.
                    INSERT INTO isomer.deltas (projection, source, weight, image, token, enqueued_at)
                    SELECT x.projection, x.source, x.weight, x.image, x.token, x.enqueued_at
                    FROM pg_catalog.jsonb_to_recordset(v_rows)
                         AS x(id bigint, projection text, source text, weight smallint, image jsonb, token bigint, enqueued_at timestamptz)
                    WHERE EXISTS (SELECT 1 FROM isomer.projections p WHERE p.name = x.projection)
                    ORDER BY x.id;
                    INSERT INTO isomer.delta_pulls (gid, nodeid, rows) VALUES (v_gid, w.nodeid, v_n);
                END IF;
            EXCEPTION WHEN OTHERS THEN
                v_ok := false;
                PERFORM isomer._log('error', pg_catalog.concat('node ', w.nodeid), pg_catalog.format('pulling deltas failed: %s', SQLERRM));
                BEGIN
                    PERFORM public.dblink_disconnect(v_conn);   -- a prepared pull survives; the next run rolls it back
                EXCEPTION WHEN OTHERS THEN
                    NULL;
                END;
            END;
            EXIT WHEN NOT v_ok OR v_n = 0;
            COMMIT;                                            -- the deltas are safe here first...
            BEGIN
                PERFORM public.dblink_exec(v_conn, pg_catalog.format('COMMIT PREPARED %L', v_gid));   -- ...then gone from the worker
                DELETE FROM isomer.delta_pulls WHERE gid = v_gid;
            EXCEPTION WHEN OTHERS THEN
                PERFORM isomer._log('warning', pg_catalog.concat('node ', w.nodeid), pg_catalog.format('finishing pull %s failed (it will be finished on the next run): %s', v_gid, SQLERRM));
                v_ok := false;
            END;
            COMMIT;
            EXIT WHEN NOT v_ok OR v_n < p_batch OR v_round >= 10;
        END LOOP;
    END LOOP;
    PERFORM pg_catalog.pg_advisory_unlock(pg_catalog.hashtext('isomer.pull_node_deltas'));
END $$;

CREATE OR REPLACE PROCEDURE isomer.pull_node_queues()
LANGUAGE plpgsql AS $$
DECLARE
    w      record;
    r      record;
    v_conn text;
    v_ok   boolean;
    v_rows jsonb;
    v_keys text[];
BEGIN
    -- Only the coordinator has the catalog; a worker running this would discard what it took.
    IF NOT pg_catalog.citus_is_coordinator() THEN
        RETURN;
    END IF;
    FOR w IN SELECT * FROM isomer._workers() LOOP
        v_conn := pg_catalog.concat('isomer_pull_', w.nodeid);
        v_ok := true;
        BEGIN
            IF NOT (v_conn = ANY (coalesce(public.dblink_get_connections(), '{}'::text[]))) THEN
                PERFORM public.dblink_connect(v_conn, w.conninfo);
            END IF;
            PERFORM public.dblink_exec(v_conn, 'BEGIN');
            -- Lock and take everything queued now; writes arriving meanwhile wait, then queue anew.
            SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('projection', d.projection, 'key', d.key,
                                                                      'token', d.token, 'enqueued_at', d.enqueued_at))
            INTO v_rows
            FROM public.dblink(v_conn, 'DELETE FROM isomer.node_queue RETURNING projection, key, token, enqueued_at')
                 AS d(projection text, key text, token bigint, enqueued_at timestamptz);
            FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(coalesce(v_rows, '[]'::jsonb))
                     AS x(projection text, key text, token bigint, enqueued_at timestamptz) LOOP
                CONTINUE WHEN NOT EXISTS (SELECT 1 FROM isomer.projections p WHERE p.name = r.projection);
                IF r.key = '*' THEN
                    v_keys := ARRAY(SELECT isomer._all_keys(r.projection));
                ELSE
                    v_keys := ARRAY[r.key];
                END IF;
                INSERT INTO isomer.pending AS q (projection, key, token, enqueued_at)
                SELECT r.projection, k, r.token, r.enqueued_at FROM pg_catalog.unnest(v_keys) k
                ON CONFLICT (projection, key) DO UPDATE
                    SET (token, enqueued_at) = (CASE WHEN q.token = 0 THEN EXCLUDED.token ELSE least(q.token, EXCLUDED.token) END,
                                                least(q.enqueued_at, EXCLUDED.enqueued_at));
            END LOOP;
        EXCEPTION WHEN OTHERS THEN
            v_ok := false;
            PERFORM isomer._log('error', pg_catalog.concat('node ', w.nodeid), pg_catalog.format('pulling queued keys failed: %s', SQLERRM));
            BEGIN
                PERFORM public.dblink_disconnect(v_conn);   -- also rolls back the worker's transaction
            EXCEPTION WHEN OTHERS THEN
                NULL;
            END;
        END;
        IF v_ok THEN
            COMMIT;                                        -- the keys are safe here first...
            PERFORM public.dblink_exec(v_conn, 'COMMIT');  -- ...then gone from the worker
        END IF;
    END LOOP;
    CALL isomer.pull_node_deltas();
END $$;

-- ── Strategy (FR-S7) ────────────────────────────────────────────────────────

-- How a query's sharded relations group: relations that are co-located and joined on their
-- distribution columns run together, shard by shard. Returns {rti: group} for every sharded
-- relation (by rtable index), from the engine's shape of the query. Worked out from metadata, not
-- by planning: once Citus has planned a repartition join, the rest of the transaction runs
-- statements over its own connections, which can't see state created in the transaction.
CREATE FUNCTION isomer._shard_groups(s jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    v_hash  jsonb;
    v_group jsonb := '{}';
    v_start text;
    v_front integer[];
    v_node  integer;
    v_edge  jsonb;
    v_other integer;
    v_n     integer := 0;
BEGIN
    SELECT coalesce(jsonb_object_agg(e ->> 'rti', jsonb_build_object('col', d.column_name, 'coloc', d.colocation, 'relid', e ->> 'relid')), '{}')
    INTO v_hash
    FROM jsonb_array_elements(s -> 'rtes') e, isomer._distribution((e ->> 'relid')::oid::regclass) d
    WHERE d.method = 'h';
    LOOP
        SELECT min(k) INTO v_start FROM jsonb_object_keys(v_hash) k WHERE NOT v_group ? k;
        EXIT WHEN v_start IS NULL;
        v_n := v_n + 1;
        v_group := v_group || jsonb_build_object(v_start, v_n);
        v_front := ARRAY[v_start::int];
        WHILE cardinality(v_front) > 0 LOOP
            v_node := v_front[1];
            v_front := v_front[2:];
            FOR v_edge IN SELECT * FROM jsonb_array_elements(s -> 'edges') LOOP
                FOR v_other IN SELECT (v_edge -> i ->> 'rti')::int FROM generate_series(0, 1) i
                               WHERE (v_edge -> (1 - i) ->> 'rti')::int = v_node LOOP
                    CONTINUE WHEN v_group ? v_other::text OR NOT v_hash ? v_other::text;
                    CONTINUE WHEN v_hash -> v_other::text -> 'coloc' <> v_hash -> v_node::text -> 'coloc';
                    CONTINUE WHEN NOT EXISTS (SELECT 1 FROM generate_series(0, 1) i
                                              WHERE (v_edge -> i ->> 'rti')::int = v_other AND v_edge -> i ->> 'col' = v_hash -> v_other::text ->> 'col'
                                                AND v_edge -> (1 - i) ->> 'col' = v_hash -> v_node::text ->> 'col');
                    v_group := v_group || jsonb_build_object(v_other::text, v_n);
                    v_front := v_front || v_other;
                END LOOP;
            END LOOP;
        END LOOP;
    END LOOP;
    RETURN jsonb_build_object('groups', v_group, 'hash', v_hash);
END $$;

-- Can the cluster run the query as written? Citus joins sharded relations that are not in one
-- group (co-located and joined on distribution columns) only by repartitioning, which it does for
-- inner joins alone: an outer join across groups, or with a non-sharded relation on the
-- preserved side and a sharded one on the optional side, it refuses.
CREATE FUNCTION isomer._runnable(s jsonb)
RETURNS boolean LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    g jsonb := isomer._shard_groups(s);
    j jsonb;
    v_left  jsonb;
    v_right jsonb;
BEGIN
    FOR j IN SELECT * FROM jsonb_array_elements(s -> 'joins') WHERE value ->> 'type' = 'left' LOOP
        SELECT coalesce(jsonb_agg(DISTINCT g -> 'groups' -> r), '[]') INTO v_right
        FROM jsonb_array_elements_text(j -> 'right') r WHERE g -> 'groups' ? r;
        CONTINUE WHEN v_right = '[]';
        SELECT coalesce(jsonb_agg(DISTINCT g -> 'groups' -> r), '[]') INTO v_left
        FROM jsonb_array_elements_text(j -> 'left') r WHERE g -> 'groups' ? r;
        IF v_left = '[]' OR jsonb_array_length(v_right) > 1 OR NOT v_left @> v_right OR jsonb_array_length(v_left) > 1 THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN true;
END $$;

-- What re-deriving one key from the tables costs. Worked out from the query's shape and where
-- Citus keeps each table, not by asking the shards (so it is safe inside a transaction holding
-- locks on the tables):
--   kind 'local'       no sharded table: the planner's cost of the query filtered to one key;
--   kind 'router'      the sharded tables are co-located and joined on their distribution columns,
--                      and the key is one of them: one shard;
--   kind 'fan-out'     the sharded tables are co-located, but the key is not their distribution
--                      column: every shard is asked, on every write (a cost per shard);
--   kind 'repartition' sharded tables that are not co-located, or not joined on their
--                      distribution columns: rows are reshuffled between nodes, and the work grows
--                      with the tables, not the key;
--   kind 'impossible'  the cluster can't run the query as written.
CREATE FUNCTION isomer._key_cost(p_query text, p_key text, OUT cost double precision, OUT kind text)
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
        EXECUTE format('EXPLAIN (FORMAT JSON) SELECT * FROM (%s) q %s', p_query,
                       CASE WHEN v_val IS NULL THEN '' ELSE format('WHERE q.%I = %L::%s', p_key, v_val, isomer._coltype(v_rel, v_col)) END)
        INTO v_plan;
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

-- Which strategy fits a projection, and why.
--   recompute   each write marks the keys it maps to, and each key is re-derived from the
--               tables with the projection's own query: no state, cheap for small groups;
--   incremental each write is folded into the projection's own state: a write costs the same
--               however large its group is, and needs no key mappings.
CREATE FUNCTION isomer.choose_strategy(query text, key text, kind text DEFAULT 'document',
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
    IF freshness = 'sync' THEN
        RETURN QUERY SELECT 'recompute', 'sync projections are re-derived by key inside the writing transaction';
        RETURN;
    END IF;
    s := isomer._engine_shape(query, key, kind);
    IF NOT (s ->> 'ok')::boolean THEN
        RETURN QUERY SELECT 'recompute', 'incremental maintenance does not apply: ' || (s ->> 'reason');
        RETURN;
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

-- What choose_strategy would pick for each document and cache projection now.
CREATE FUNCTION isomer.strategy_advice()
RETURNS TABLE (projection text, current_strategy text, advised_strategy text, reason text)
LANGUAGE sql SET search_path = pg_catalog AS $$
    SELECT p.name, p.strategy, c.strategy, c.reason
    FROM isomer.projections p
    CROSS JOIN LATERAL isomer.choose_strategy(p.query, p.key_column, p.kind, p.freshness,
        CASE WHEN p.strategy = 'recompute'
             THEN (SELECT jsonb_object_agg(s.source_table, s.key_column) FROM isomer.projection_sources s WHERE s.projection = p.name) END) c
    WHERE p.kind IN ('document', 'cache')
    ORDER BY p.name
$$;

-- Does the derivation from state give exactly what the query gives from the tables? Returns the
-- number of rows that differ (checked once, when an incremental projection is created).
CREATE FUNCTION isomer._engine_differences(p_projection text)
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
    v bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    EXECUTE format(
        $q$WITH a AS (SELECT to_jsonb(q) AS r FROM (%1$s) q), b AS (SELECT to_jsonb(q) AS r FROM (%2$s) q)
           SELECT (SELECT count(*) FROM (SELECT r FROM a EXCEPT ALL SELECT r FROM b) x)
                + (SELECT count(*) FROM (SELECT r FROM b EXCEPT ALL SELECT r FROM a) y)$q$,
        isomer._truth(p), p.derive_query) INTO v;
    RETURN v;
END $$;

-- ── Creating, installing, applying ──────────────────────────────────────────

DROP FUNCTION isomer.create_projection(text, text, text, text, text, interval, jsonb, text, text[], text, text);

CREATE FUNCTION isomer.create_projection(
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
                RAISE EXCEPTION 'isomer: projection % cannot be both sync and incremental: incremental maintenance folds changes after they commit', name
                    USING HINT = 'Use freshness => ''bounded''.';
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
        v_reason := isomer._check_sources(name, freshness);
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

CREATE OR REPLACE FUNCTION isomer._install(p_projection text)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p         isomer.projections;
    v_target  text;
    v_count   bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;

    IF p.kind IN ('document', 'vector', 'cache') THEN
        IF p.kind = 'document' THEN
            PERFORM documentdb_api.create_collection(p.mongo_database, p.name);
        ELSIF p.kind = 'vector' THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %s (key text NOT NULL, model text NOT NULL, embedding public.vector NOT NULL, '
                'source_hash text NOT NULL, embedded_at timestamptz NOT NULL, PRIMARY KEY (key, model))',
                isomer._vector_store(p.name));
            PERFORM isomer._vector_index(p.name, p.model);
            PERFORM isomer._vector_view(p.name);
        ELSE
            EXECUTE format('CREATE TABLE IF NOT EXISTS %s (key text PRIMARY KEY, value jsonb, updated_at timestamptz NOT NULL)',
                           isomer._cache_store(p.name));
            EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
            EXECUTE format('CREATE VIEW %I.%I AS SELECT key::%s AS %I, value, updated_at FROM %s',
                           p.target_schema, p.name, p.key_type, p.key_column, isomer._cache_store(p.name));
        END IF;
        -- Incremental: the state is built after the capture triggers exist, in this transaction.
        -- Creating them locks every shard against writes until commit, so the state and the
        -- deltas meet without a gap or an overlap.
        IF p.strategy = 'incremental' THEN
            PERFORM isomer._engine_compile(p.name);
        END IF;
        PERFORM isomer._create_source_triggers(p.name);
        IF p.strategy = 'incremental' THEN
            PERFORM isomer._engine_build(p.name);
        END IF;
        INSERT INTO isomer.projection_stats (projection) VALUES (p.name) ON CONFLICT DO NOTHING;
        IF p.kind IN ('document', 'cache') THEN
            v_count := isomer._rebuild_all(p.name);
            IF p.kind = 'cache' THEN
                RETURN format('cache projection %s: %s entries, read it as %I.%I or isomer.cache_get(%L, key) (%s)',
                              p.name, v_count, p.target_schema, p.name, p.name, p.freshness);
            END IF;
            RETURN format('document projection %s: %s documents in collection %s.%s (%s)',
                          p.name, v_count, p.mongo_database, p.name, p.freshness);
        END IF;
        -- Backfill work carries token 0: read-your-writes waits cover writes, not initial builds.
        INSERT INTO isomer.pending (projection, key, phase, token)
        SELECT p.name, k, 'backfill', 0 FROM isomer._all_keys(p.name) k
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS v_count = ROW_COUNT;
        RETURN format('vector projection %s: %s keys queued for embedding with %s; read it as %I.%I',
                      p.name, v_count, p.model, p.target_schema, p.name);
    END IF;

    -- search
    v_target := format('%I.%I', p.target_schema, p.name);
    PERFORM pgtrickle.create_stream_table(
        v_target, p.query,
        schedule     => CASE WHEN p.freshness = 'sync' THEN NULL
                             ELSE greatest(1, ceil(extract(epoch FROM p.lag_budget) / 2))::int || 's' END,
        refresh_mode => CASE WHEN p.freshness = 'sync' THEN 'IMMEDIATE' ELSE 'AUTO' END);
    EXECUTE format('CREATE INDEX %I ON %s USING bm25 (%s)',
                   p.name || '_bm25', v_target,
                   (SELECT string_agg(format('%I', c), ', ') FROM unnest(p.index_columns) c));
    EXECUTE format('SELECT count(*) FROM %s', v_target) INTO v_count;
    RETURN format('search projection %s: %s rows in %s (%s)', p.name, v_count, v_target, p.freshness);
END $$;

-- Keys are derived with the projection's own query, or, for an incremental projection, from its
-- state (derive_query).
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

-- Draining now folds an incremental projection's deltas first; the keys they reach are then
-- applied like any other queued key, in the same pass.
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
    FOR p IN SELECT name, kind, lag_budget, strategy FROM isomer.projections
             WHERE kind IN ('document', 'vector', 'cache') AND freshness = 'bounded' AND status = 'active'
    LOOP
        BEGIN
            PERFORM isomer._merge_key_log(p.name);
        EXCEPTION WHEN OTHERS THEN
            PERFORM isomer._log('error', p.name, format('merging queued keys failed: %s', SQLERRM));
        END;
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
                IF p.kind IN ('document', 'cache') THEN
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

-- ── Read your writes: deltas not yet folded count as queued work ────────────

CREATE OR REPLACE FUNCTION isomer.freshness(p_projection text)
RETURNS bigint LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    p     isomer.projections;
    v_min bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    IF p.freshness = 'sync' THEN
        RETURN isomer.current_token();
    END IF;
    SELECT least((SELECT min(token) FROM isomer.pending WHERE projection = p.name AND token > 0),
                 (SELECT min(token) FROM isomer.embed_jobs WHERE projection = p.name AND token > 0),
                 (SELECT min(token) FROM isomer.deltas WHERE projection = p.name AND token > 0),
                 (SELECT min(token) FROM isomer.key_log WHERE projection = p.name AND token > 0))
    INTO v_min;
    RETURN coalesce(v_min - 1, isomer.current_token());
END $$;

CREATE OR REPLACE FUNCTION isomer._queued_before(p_barrier timestamptz, p_projection text)
RETURNS boolean LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    v_remote boolean;
BEGIN
    -- Fail closed: a worker that doesn't answer counts as still having queued work.
    SELECT coalesce(bool_or(NOT success OR result = 't'), false) INTO v_remote
    FROM run_command_on_workers(format(
        'SELECT EXISTS (SELECT 1 FROM isomer.node_queue WHERE enqueued_at <= %1$L %2$s) '
        '    OR EXISTS (SELECT 1 FROM isomer.node_deltas WHERE enqueued_at <= %1$L %2$s)',
        p_barrier, CASE WHEN p_projection IS NULL THEN '' ELSE format('AND projection = %L', p_projection) END));
    IF v_remote THEN
        RETURN true;
    END IF;
    RETURN EXISTS (SELECT 1 FROM isomer.pending
                   WHERE token > 0 AND enqueued_at <= p_barrier AND (p_projection IS NULL OR projection = p_projection))
        OR EXISTS (SELECT 1 FROM isomer.embed_jobs
                   WHERE token > 0 AND enqueued_at <= p_barrier AND (p_projection IS NULL OR projection = p_projection))
        OR EXISTS (SELECT 1 FROM isomer.deltas
                   WHERE token > 0 AND enqueued_at <= p_barrier AND (p_projection IS NULL OR projection = p_projection))
        OR EXISTS (SELECT 1 FROM isomer.key_log
                   WHERE token > 0 AND enqueued_at <= p_barrier AND (p_projection IS NULL OR projection = p_projection));
END $$;

CREATE OR REPLACE FUNCTION isomer.wait_for(p_token bigint, p_projection text DEFAULT NULL, p_timeout interval DEFAULT '5 seconds')
RETURNS bigint LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    v_until   timestamptz := clock_timestamp() + p_timeout;
    v_multi   boolean := EXISTS (SELECT 1 FROM isomer._workers());
    v_barrier timestamptz;
    v_gids    text[];
    s         record;
BEGIN
    IF p_token IS NULL AND NOT v_multi THEN
        RETURN isomer.current_token();
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'isomer.wait_for needs READ COMMITTED isolation (this transaction is %)',
            current_setting('transaction_isolation');
    END IF;
    IF EXISTS (SELECT 1 FROM isomer.pending
               WHERE xmin = pg_current_xact_id_if_assigned()::text::xid
                 AND token > 0 AND (p_projection IS NULL OR projection = p_projection))
       OR EXISTS (SELECT 1 FROM isomer.deltas
                  WHERE xmin = pg_current_xact_id_if_assigned()::text::xid
                    AND token > 0 AND (p_projection IS NULL OR projection = p_projection))
       OR EXISTS (SELECT 1 FROM isomer.key_log
                  WHERE xmin = pg_current_xact_id_if_assigned()::text::xid
                    AND token > 0 AND (p_projection IS NULL OR projection = p_projection)) THEN
        RAISE EXCEPTION 'isomer.wait_for: the write you are waiting for is not committed yet'
            USING HINT = 'Commit the write first, then call isomer.wait_for(isomer.last_write_token()) in a new transaction.';
    END IF;
    FOR s IN SELECT name, target_schema FROM isomer.projections
             WHERE kind = 'search' AND freshness = 'bounded' AND (p_projection IS NULL OR name = p_projection) LOOP
        PERFORM pgtrickle.refresh_stream_table(format('%I.%I', s.target_schema, s.name));
    END LOOP;
    IF v_multi THEN
        v_barrier := isomer._barrier();
    END IF;
    LOOP
        IF v_multi THEN
            EXIT WHEN NOT isomer._queued_before(v_barrier, p_projection);
        ELSE
            EXIT WHEN NOT EXISTS (SELECT 1 FROM isomer.pending
                                  WHERE token > 0 AND token <= p_token AND (p_projection IS NULL OR projection = p_projection))
                  AND NOT EXISTS (SELECT 1 FROM isomer.embed_jobs
                                  WHERE token > 0 AND token <= p_token AND (p_projection IS NULL OR projection = p_projection))
                  AND NOT EXISTS (SELECT 1 FROM isomer.deltas
                                  WHERE token > 0 AND token <= p_token AND (p_projection IS NULL OR projection = p_projection))
                  AND NOT EXISTS (SELECT 1 FROM isomer.key_log
                                  WHERE token > 0 AND token <= p_token AND (p_projection IS NULL OR projection = p_projection));
        END IF;
        IF clock_timestamp() > v_until THEN
            RAISE EXCEPTION 'isomer: % has not caught up to token % yet (not yet)', coalesce(p_projection, 'a projection'), coalesce(p_token::text, 'now')
                USING ERRCODE = 'IS001',
                      HINT = 'Retry the read, or raise the timeout. isomer.projection_status shows the backlog.';
        END IF;
        PERFORM pg_sleep(0.02);
    END LOOP;
    IF v_multi THEN
        v_gids := isomer._prepared_gids();
        WHILE v_gids IS NOT NULL AND v_gids && isomer._prepared_gids() LOOP
            IF clock_timestamp() > v_until THEN
                RAISE EXCEPTION 'isomer: % has not caught up yet: a distributed commit is still in flight (not yet)', coalesce(p_projection, 'a projection')
                    USING ERRCODE = 'IS001';
            END IF;
            PERFORM pg_sleep(0.01);
        END LOOP;
    END IF;
    RETURN coalesce(p_token, isomer.current_token());
END $$;

-- ── Lifecycle ───────────────────────────────────────────────────────────────

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
    IF p.kind IN ('document', 'vector', 'cache') THEN
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

DROP FUNCTION isomer.alter_projection(text, text, interval);

-- Change a projection's freshness, lag budget or maintenance strategy. Changing strategy
-- rebuilds the projection's maintenance (and, for incremental, its state) in one transaction.
-- Key recompute needs every sharded source mapped to the key; `sources` sets the mappings.
CREATE FUNCTION isomer.alter_projection(name text, freshness text DEFAULT NULL, lag interval DEFAULT NULL,
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
                RAISE EXCEPTION 'isomer: projection % is sync; incremental maintenance is bounded', p.name;
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
        IF freshness = 'sync' AND p.strategy = 'incremental' THEN
            RAISE EXCEPTION 'isomer: projection % is maintained incrementally, which is bounded', p.name
                USING HINT = 'Switch it to strategy => ''recompute'' first.';
        END IF;
        v_reason := isomer._check_sources(p.name, freshness);
        IF v_reason IS NOT NULL THEN
            RAISE EXCEPTION 'isomer: projection % cannot be %: %', p.name, freshness, v_reason;
        END IF;
        UPDATE isomer.projections SET freshness = alter_projection.freshness WHERE projections.name = p.name;
        PERFORM isomer._create_source_triggers(p.name);
        -- Anything still queued is applied now, so the projection is current as it changes mode.
        PERFORM isomer._apply(p.name, ARRAY(SELECT key FROM isomer.pending WHERE projection = p.name));
        DELETE FROM isomer.pending WHERE projection = p.name;
    END IF;
    IF lag IS NOT NULL THEN
        UPDATE isomer.projections SET lag_budget = lag WHERE projections.name = p.name;
    END IF;
    SELECT * INTO p FROM isomer.projections WHERE projections.name = alter_projection.name;
    RETURN format('%s: freshness %s, lag budget %s, strategy %s', p.name, p.freshness, p.lag_budget, p.strategy);
END $$;

CREATE OR REPLACE FUNCTION isomer.dump_projections()
RETURNS SETOF text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT format('SELECT isomer.create_projection(name => %L, query => %L, kind => %L, key => %L, '
                  'freshness => %L, lag => %L, sources => %L, database => %L, index_columns => %L, target_schema => %L, model => %L, strategy => %L);',
                  p.name, p.query, p.kind, p.key_column, p.freshness, p.lag_budget,
                  (SELECT jsonb_object_agg(s.source_table, s.key_column) FROM isomer.projection_sources s WHERE s.projection = p.name),
                  coalesce(p.mongo_database, 'isomer'), p.index_columns, coalesce(p.target_schema, 'public'), p.model, p.strategy)
    FROM isomer.projections p ORDER BY p.created_at
$$;

-- Native syntax: a view whose key mappings can't be inferred can still be declared when it is
-- maintained incrementally, which needs none.
CREATE OR REPLACE FUNCTION isomer._native_register(p_view regclass, p_key text)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_name      text;
    v_schema    text;
    v_am        text;
    v_query     text;
    v_opts      jsonb := isomer._native_options(p_view);
    v_sources   jsonb;
    v_inferred  jsonb;
    v_existing  isomer.projections;
    v_res       text;
    v_lookups   text;
BEGIN
    SELECT c.relname, n.nspname, a.amname INTO v_name, v_schema, v_am
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_am a ON a.oid = c.relam
    WHERE c.oid = p_view;
    IF coalesce(v_opts ->> 'kind', 'document') <> 'document' THEN
        RAISE EXCEPTION 'isomer: native syntax declares document projections; use isomer.create_projection() for % projections', v_opts ->> 'kind';
    END IF;
    v_query := isomer._viewdef(p_view);

    SELECT * INTO v_existing FROM isomer.projections WHERE name = v_name;
    IF FOUND THEN
        IF v_existing.query = v_query AND v_existing.key_column = p_key THEN
            v_res := isomer._install(v_name);       -- a restore: the catalog came back with the dump
            EXECUTE format('REFRESH MATERIALIZED VIEW %s WITH NO DATA', p_view);
            RETURN v_res;
        END IF;
        RAISE EXCEPTION 'isomer: a projection named % already exists with a different definition', v_name;
    END IF;

    v_inferred := isomer._infer_sources(v_query, p_key);
    IF v_inferred IS NULL AND v_opts -> 'sources' IS NULL
       AND (coalesce(v_opts ->> 'strategy', 'auto') = 'recompute' OR v_am = 'isomer_sync'
            OR NOT (isomer._engine_shape(v_query, p_key, 'document') ->> 'ok')::boolean) THEN
        RAISE EXCEPTION 'isomer: could not tell which table the key % of % comes from', p_key, p_view
            USING HINT = 'Select it as alias.column, or give the mappings: COMMENT ON MATERIALIZED VIEW ... IS ''isomer {"sources": {"schema.table": "column"}}''';
    END IF;
    IF v_inferred IS NOT NULL OR v_opts -> 'sources' IS NOT NULL THEN
        v_sources := coalesce(v_inferred, '{}') || coalesce(v_opts -> 'sources', '{}');
    END IF;

    v_res := isomer.create_projection(
        name      => v_name,
        query     => v_query,
        kind      => 'document',
        key       => p_key,
        freshness => CASE v_am WHEN 'isomer_sync' THEN 'sync' ELSE 'bounded' END,
        lag       => coalesce((v_opts ->> 'lag')::interval, '2 seconds'),
        sources   => v_sources,
        database  => coalesce(v_opts ->> 'database', v_schema),
        strategy  => coalesce(v_opts ->> 'strategy', 'auto'));
    UPDATE isomer.projections SET native_view = format('%I.%I', v_schema, v_name) WHERE name = v_name;
    EXECUTE format('REFRESH MATERIALIZED VIEW %s WITH NO DATA', p_view);
    SELECT string_agg(k, ', ') INTO v_lookups FROM jsonb_each(v_sources) e(k, v) WHERE jsonb_typeof(v) = 'null';
    IF v_lookups IS NOT NULL AND (SELECT strategy FROM isomer.projections WHERE name = v_name) = 'recompute' THEN
        RAISE NOTICE 'isomer: % treated as lookup table(s): any change to them re-derives every document. To map them, COMMENT ON MATERIALIZED VIEW % IS ''isomer {"sources": {"<table>": "<column holding the key>"}}''', v_lookups, p_view;
    END IF;
    RETURN v_res;
END $$;

-- Repair drifted keys by re-deriving them. An incremental projection's keys are derived from its
-- state, so that is rebuilt from the tables first (with the sources locked against writes).
CREATE OR REPLACE FUNCTION isomer.repair(p_projection text, p_keys text[] DEFAULT NULL)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_keys text[] := coalesce(p_keys, ARRAY(SELECT DISTINCT key FROM isomer.drift_samples WHERE projection = p_projection));
    s      record;
BEGIN
    IF (SELECT strategy FROM isomer.projections WHERE name = p_projection) = 'incremental' THEN
        PERFORM pg_advisory_xact_lock(hashtext('isomer.engine'), hashtext(p_projection));
        FOR s IN SELECT source_table FROM isomer.projection_sources WHERE projection = p_projection LOOP
            EXECUTE format('LOCK TABLE %s IN SHARE MODE', s.source_table);
        END LOOP;
        PERFORM run_command_on_workers(format('DELETE FROM isomer.node_deltas WHERE projection = %L', p_projection));
        DELETE FROM isomer.deltas WHERE projection = p_projection;
        PERFORM isomer._engine_build(p_projection);
    END IF;
    PERFORM isomer._apply(p_projection, v_keys);
    RETURN cardinality(v_keys);
END $$;

CREATE OR REPLACE VIEW isomer.projection_status AS
SELECT p.name, p.kind, p.freshness, p.lag_budget, p.status,
       (SELECT count(*) FROM isomer.pending q WHERE q.projection = p.name)
         + (SELECT count(DISTINCT q.key) FROM isomer.key_log q WHERE q.projection = p.name) AS pending_keys,
       (SELECT clock_timestamp() - least(min(q.enqueued_at), (SELECT min(l.enqueued_at) FROM isomer.key_log l WHERE l.projection = p.name))
        FROM isomer.pending q WHERE q.projection = p.name) AS oldest_pending,
       s.last_applied_at, round(s.last_lag_ms::numeric, 1) AS last_lag_ms,
       round(s.max_lag_ms::numeric, 1) AS max_lag_ms, s.budget_breaches,
       s.documents_written, s.documents_deleted,
       -- Time since pg_trickle last confirmed the table current (a refresh that found no changes
       -- counts): pgt_status().staleness measures time since data last changed, which grows when idle.
       clock_timestamp() - st.last_refresh_at AS search_staleness, t.consecutive_errors AS search_errors,
       p.model, p.pending_model,
       (SELECT count(*) FROM isomer.embed_jobs j WHERE j.projection = p.name) AS embed_backlog,
       (SELECT clock_timestamp() - min(j.enqueued_at) FROM isomer.embed_jobs j WHERE j.projection = p.name) AS oldest_embed_job,
       (SELECT count(*) FROM isomer.embed_dead_letter d WHERE d.projection = p.name) AS embed_dead_letters,
       (SELECT round(sum(u.cost_usd), 6) FROM isomer.embedding_usage u
         WHERE u.projection = p.name AND u.month = date_trunc('month', now())::date) AS embed_cost_this_month,
       p.monthly_cap_usd,
       p.strategy,
       (SELECT count(*) FROM isomer.deltas d WHERE d.projection = p.name) AS pending_deltas,
       s.deltas_folded
FROM isomer.projections p
LEFT JOIN isomer.projection_stats s ON s.projection = p.name
LEFT JOIN pgtrickle.pgt_status() t ON p.kind = 'search' AND t.name = format('%s.%s', p.target_schema, p.name)
LEFT JOIN pgtrickle.pgt_stream_tables st ON p.kind = 'search' AND st.pgt_schema = p.target_schema AND st.pgt_name = p.name;

-- ── Certificates (FR-S5) for incremental projections ────────────────────────

CREATE FUNCTION isomer._engine_certificate(p isomer.projections, OUT rules jsonb, OUT assumptions jsonb, OUT checks jsonb)
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
        'joins over integrated state: a key can change only if a delta of one input reaches it through the state of the others, before or after the fold (the change of A join B is dA join B + A join dB + dA join dB), so exactly those keys are re-derived, from the state');
    assumptions := jsonb_build_array(
        'every relation the query reads has a capture trigger on every placement, ENABLE ALWAYS',
        'the unique keys the joins land on still exist (re-checked below)',
        format('bounded: deltas are folded and the keys they reach applied within the lag budget (%s); read-your-writes via isomer.wait_for', p.lag_budget));

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

    IF p.strategy = 'incremental' THEN
        SELECT c.rules, c.assumptions, c.checks INTO v_rules, v_assume, v_checks FROM isomer._engine_certificate(p) c;
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

-- ── What the tables produce ─────────────────────────────────────────────────

-- The query that gives what the tables produce: the projection's own, or, for an incremental
-- projection whose query the cluster can't run as written, its fresh derivation (truth_query).
-- Key listing, rebuilds, history baselines, verify and the auditor read through this.
CREATE FUNCTION isomer._truth(p isomer.projections)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT coalesce((p).truth_query, (p).query)
$$;

CREATE OR REPLACE FUNCTION isomer._all_keys(p_projection text)
RETURNS SETOF text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    RETURN QUERY EXECUTE format('SELECT q.%I::text FROM (%s) q', p.key_column, isomer._truth(p));
END $$;

CREATE OR REPLACE FUNCTION isomer._rebuild_all(p_projection text, p_batch integer DEFAULT 500)
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p       isomer.projections;
    v_keys  text[];
    v_total bigint := 0;
    v_ok    boolean;
    v_res   text;
    v_live  jsonb;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    FOR v_keys IN
        SELECT array_agg(k) FROM (
            SELECT k, (row_number() OVER () - 1) / p_batch AS chunk FROM isomer._all_keys(p_projection) k
        ) s GROUP BY chunk
    LOOP
        PERFORM isomer._apply(p_projection, v_keys);
        v_total := v_total + cardinality(v_keys);
    END LOOP;

    IF p.kind = 'cache' THEN
        EXECUTE format('DELETE FROM %s WHERE key NOT IN (SELECT isomer._all_keys(%L))', isomer._cache_store(p.name), p.name);
        RETURN v_total;
    END IF;

    EXECUTE format('SELECT coalesce(jsonb_agg(to_jsonb(q.%I)), ''[]''::jsonb) FROM (%s) q', p.key_column, isomer._truth(p))
    INTO v_live;
    SELECT r.p_success, r.p_result::text INTO v_ok, v_res
    FROM documentdb_api.delete(
        p.mongo_database,
        jsonb_build_object('delete', p.name, 'deletes', jsonb_build_array(jsonb_build_object(
            'q', jsonb_build_object('_id', jsonb_build_object('$nin', v_live)),
            'limit', 0)))::text::documentdb_core.bson) r;
    IF NOT v_ok THEN
        RAISE EXCEPTION 'isomer: removing stale documents for projection % failed: %', p.name, v_res;
    END IF;
    RETURN v_total;
END $$;

CREATE OR REPLACE FUNCTION isomer._audit_pairs(p isomer.projections, p_keys text[])
RETURNS TABLE (key text, expected jsonb, actual jsonb)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF p.kind = 'document' THEN
        RETURN QUERY EXECUTE format(
            $q$SELECT k, isomer._num_canon(e.doc), isomer._num_canon(a.doc)
               FROM unnest($1::text[]) k
               LEFT JOIN (SELECT q.%1$I::text AS key, q.doc::jsonb || jsonb_build_object('_id', to_jsonb(q.%1$I)) AS doc
                          FROM (%2$s) q WHERE q.%1$I = ANY ($1::%3$s[])) e ON e.key = k
               LEFT JOIN (SELECT isomer._ejson_relax(documentdb_core.bson_to_json_string(c.document)::text::jsonb) AS doc
                          FROM %4$s c) a ON a.doc ->> '_id' = k$q$,
            p.key_column, isomer._truth(p), p.key_type, isomer._collection_table(p.mongo_database, p.name))
        USING p_keys;
    ELSIF p.kind = 'cache' THEN
        RETURN QUERY EXECUTE format(
            $q$SELECT k, e.value, a.value
               FROM unnest($1::text[]) k
               LEFT JOIN (SELECT q.%1$I::text AS key, to_jsonb(q.value) AS value FROM (%2$s) q WHERE q.%1$I = ANY ($1::%3$s[])) e ON e.key = k
               LEFT JOIN %4$s a ON a.key = k$q$,
            p.key_column, isomer._truth(p), p.key_type, isomer._cache_store(p.name))
        USING p_keys;
    ELSIF p.kind = 'vector' THEN
        -- The embedding itself can't be recomputed cheaply; the contract is that it was made from
        -- the current text, which its stored hash proves.
        RETURN QUERY EXECUTE format(
            $q$SELECT k, to_jsonb(e.hash), to_jsonb(a.source_hash)
               FROM unnest($1::text[]) k
               LEFT JOIN (SELECT q.%1$I::text AS key, md5(q.input::text) AS hash FROM (%2$s) q
                          WHERE q.%1$I = ANY ($1::%3$s[]) AND q.input IS NOT NULL) e ON e.key = k
               LEFT JOIN %4$s a ON a.key = k AND a.model = %5$L$q$,
            p.key_column, isomer._truth(p), p.key_type, isomer._vector_store(p.name), p.model)
        USING p_keys;
    ELSE
        RETURN QUERY EXECUTE format(
            $q$SELECT k, e.row, a.row
               FROM unnest($1::text[]) k
               LEFT JOIN (SELECT q.%1$I::text AS key, to_jsonb(q) AS row FROM (%2$s) q WHERE q.%1$I = ANY ($1::%3$s[])) e ON e.key = k
               LEFT JOIN (SELECT t.%1$I::text AS key, to_jsonb(t) - '__pgt_row_id' AS row FROM %4$I.%5$I t) a ON a.key = k$q$,
            p.key_column, isomer._truth(p), p.key_type, p.target_schema, p.name)
        USING p_keys;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION isomer.audit(p_sample integer DEFAULT 50)
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog SET paradedb.planner_warnings = off AS $$
DECLARE
    p        isomer.projections;
    v_keys   text[];
    r        record;
    v_hash   text;
    v_more   text[];
    v_s      isomer.audit_suspects;
    v_min    interval;
    v_checks integer;
    v_drift  bigint := 0;
BEGIN
    FOR p IN SELECT * FROM isomer.projections WHERE status = 'active' LOOP
        BEGIN
            -- Keys from the tables...
            EXECUTE format('SELECT array_agg(k) FROM (SELECT q.%I::text AS k FROM (%s) q ORDER BY random() LIMIT %s) s',
                           p.key_column, isomer._truth(p), p_sample) INTO v_keys;
            -- ...and from the projection's storage.
            IF p.kind = 'document' THEN
                EXECUTE format('SELECT array_agg(k) FROM (SELECT isomer._ejson_relax(documentdb_core.bson_to_json_string(c.document)::text::jsonb) ->> ''_id'' AS k '
                               'FROM %s c ORDER BY random() LIMIT %s) s',
                               isomer._collection_table(p.mongo_database, p.name), p_sample / 5 + 1) INTO v_more;
                v_keys := v_keys || v_more;
            ELSIF p.kind = 'cache' THEN
                EXECUTE format('SELECT array_agg(key) FROM (SELECT key FROM %s ORDER BY random() LIMIT %s) s',
                               isomer._cache_store(p.name), p_sample / 5 + 1) INTO v_more;
                v_keys := v_keys || v_more;
            ELSIF p.kind = 'vector' THEN
                EXECUTE format('SELECT array_agg(key) FROM (SELECT key FROM %s WHERE model = %L ORDER BY random() LIMIT %s) s',
                               isomer._vector_store(p.name), p.model, p_sample / 5 + 1) INTO v_more;
                v_keys := v_keys || v_more;
            END IF;
            v_keys := ARRAY(SELECT DISTINCT k FROM unnest(v_keys) k WHERE k IS NOT NULL);
            v_checks := 0;
            -- Sync projections have no lag; only a concurrent write can make them briefly differ.
            v_min := CASE WHEN p.freshness = 'sync' THEN interval '2 seconds'
                          ELSE greatest(p.lag_budget * 2, interval '2 seconds') END;

            FOR r IN SELECT * FROM isomer._audit_pairs(p, v_keys) LOOP
                -- Work in flight is not drift.
                CONTINUE WHEN EXISTS (SELECT 1 FROM isomer.pending q WHERE q.projection = p.name AND q.key = r.key)
                           OR EXISTS (SELECT 1 FROM isomer.key_log q WHERE q.projection = p.name AND q.key = r.key)
                           OR EXISTS (SELECT 1 FROM isomer.embed_jobs j WHERE j.projection = p.name AND j.key = r.key);
                v_checks := v_checks + 1;
                IF r.expected IS NOT DISTINCT FROM r.actual THEN
                    DELETE FROM isomer.audit_suspects WHERE projection = p.name AND key = r.key;
                    CONTINUE;
                END IF;
                v_hash := md5(coalesce(r.expected::text, '<absent>'));
                SELECT * INTO v_s FROM isomer.audit_suspects WHERE projection = p.name AND key = r.key;
                IF NOT FOUND OR v_s.expected_hash IS DISTINCT FROM v_hash THEN
                    INSERT INTO isomer.audit_suspects (projection, key, expected_hash) VALUES (p.name, r.key, v_hash)
                    ON CONFLICT (projection, key) DO UPDATE SET expected_hash = EXCLUDED.expected_hash, first_seen = clock_timestamp();
                ELSIF clock_timestamp() - v_s.first_seen >= v_min THEN
                    INSERT INTO isomer.drift_samples (projection, key, expected, actual) VALUES (p.name, r.key, r.expected, r.actual);
                    DELETE FROM isomer.audit_suspects WHERE projection = p.name AND key = r.key;
                    UPDATE isomer.audit_stats SET drift = drift + 1 WHERE projection = p.name;
                    PERFORM isomer._log('error', p.name, format(
                        'DRIFT: key %s differs from what the tables produce (pinned as a drift sample); expected %s, found %s',
                        r.key, left(coalesce(r.expected::text, 'nothing'), 200), left(coalesce(r.actual::text, 'nothing'), 200)));
                    v_drift := v_drift + 1;
                END IF;
            END LOOP;

            INSERT INTO isomer.audit_stats AS s (projection, checks, last_run) VALUES (p.name, v_checks, now())
            ON CONFLICT (projection) DO UPDATE SET checks = s.checks + EXCLUDED.checks, last_run = EXCLUDED.last_run;
        EXCEPTION WHEN OTHERS THEN
            PERFORM isomer._log('error', p.name, format('audit failed: %s', SQLERRM));
        END;
    END LOOP;
    RETURN v_drift;
END $$;

CREATE OR REPLACE FUNCTION isomer.set_history(name text, retention interval)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p     isomer.projections;
    s     record;
    v_n   bigint;
BEGIN
    SELECT * INTO p FROM isomer.projections pr WHERE pr.name = set_history.name FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', set_history.name;
    END IF;
    IF retention IS NULL THEN
        UPDATE isomer.projections pr SET history = NULL, history_since = NULL, history_pruned_through = 0 WHERE pr.name = p.name;
        DELETE FROM isomer.changes WHERE projection = p.name;
        RETURN format('history for %s is off', p.name);
    END IF;
    IF p.kind NOT IN ('document', 'cache') THEN
        RAISE EXCEPTION 'isomer: history is kept for document and cache projections; % is a % projection', p.name, p.kind;
    END IF;
    IF retention < interval '1 minute' THEN
        RAISE EXCEPTION 'isomer: history retention must be at least a minute';
    END IF;
    IF p.history IS NOT NULL THEN
        UPDATE isomer.projections pr SET history = retention WHERE pr.name = p.name;
        RETURN format('history for %s: retention is now %s', p.name, retention);
    END IF;

    FOR s IN SELECT source_table FROM isomer.projection_sources WHERE projection = p.name LOOP
        EXECUTE format('LOCK TABLE %s IN SHARE MODE', s.source_table);
    END LOOP;
    UPDATE isomer.projections pr SET history = retention, history_since = clock_timestamp() WHERE pr.name = p.name;
    PERFORM pg_advisory_xact_lock(hashtext('isomer.changes'), hashtext(p.name));
    IF p.kind = 'document' THEN
        EXECUTE format(
            $q$INSERT INTO isomer.changes (id, projection, key, op, doc, token)
               SELECT nextval('isomer.changes_id_seq'), %4$L, q.%1$I::text, 'u',
                      q.doc::jsonb || jsonb_build_object('_id', to_jsonb(q.%1$I)), isomer.current_token()
               FROM (%2$s) q ORDER BY q.%1$I$q$,
            p.key_column, isomer._truth(p), p.key_type, p.name);
    ELSE
        EXECUTE format(
            $q$INSERT INTO isomer.changes (id, projection, key, op, doc, token)
               SELECT nextval('isomer.changes_id_seq'), %3$L, q.%1$I::text, 'u', to_jsonb(q.value), isomer.current_token()
               FROM (%2$s) q ORDER BY q.%1$I$q$,
            p.key_column, isomer._truth(p), p.name);
    END IF;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN format('history for %s is on: %s documents in the baseline, kept for %s', p.name, v_n, retention);
END $$;

-- ── Queued keys: append only (see key_log above) ───────────────────────────

CREATE OR REPLACE FUNCTION isomer._on_source_change()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    v_projection text := TG_ARGV[0];
    v_column     text := TG_ARGV[1];
    v_freshness  text := TG_ARGV[2];
    v_keys       text[];
    v_token      bigint := isomer.current_token();
BEGIN
    PERFORM set_config('isomer.last_write_token', v_token::text, false);

    -- On a worker node (no catalog): queue the keys for the coordinator to pull.
    IF NOT EXISTS (SELECT 1 FROM isomer.projections WHERE name = v_projection) THEN
        IF TG_OP = 'TRUNCATE' OR v_column = '' THEN
            v_keys := ARRAY['*'];
        ELSE
            SELECT array_agg(DISTINCT k) INTO v_keys
            FROM unnest(ARRAY[
                CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) ->> v_column END,
                CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) ->> v_column END]) k
            WHERE k IS NOT NULL;
        END IF;
        INSERT INTO isomer.node_queue (projection, key, token)
        SELECT v_projection, k, v_token FROM unnest(v_keys) k;
        RETURN NULL;
    END IF;

    IF TG_OP = 'TRUNCATE' THEN
        PERFORM isomer._log('info', v_projection, format('%s truncated: full refresh', TG_TABLE_NAME));
        PERFORM isomer._full_refresh(v_projection);
        RETURN NULL;
    END IF;

    IF v_column = '' THEN
        IF v_freshness = 'sync' THEN
            PERFORM pg_advisory_xact_lock(hashtext('isomer'), hashtext(v_projection));
            PERFORM isomer._rebuild_all(v_projection);
        ELSE
            INSERT INTO isomer.key_log (projection, key, token)
            SELECT v_projection, k, v_token FROM isomer._all_keys(v_projection) k;
        END IF;
        RETURN NULL;
    END IF;

    SELECT array_agg(DISTINCT k ORDER BY k) INTO v_keys
    FROM unnest(ARRAY[
        CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) ->> v_column END,
        CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) ->> v_column END]) k
    WHERE k IS NOT NULL;

    IF v_freshness = 'sync' THEN
        PERFORM pg_advisory_xact_lock(hashtext(v_projection), hashtext(k)) FROM unnest(v_keys) k ORDER BY k;
        PERFORM isomer._apply(v_projection, v_keys);
    ELSE
        INSERT INTO isomer.key_log (projection, key, token)
        SELECT v_projection, k, v_token FROM unnest(v_keys) k;
    END IF;
    RETURN NULL;
END $$;

-- Move a projection's committed log entries into pending: one row per key, the oldest real token
-- and the earliest time. Called by the drain, before it takes its batch.
CREATE FUNCTION isomer._merge_key_log(p_projection text)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_n integer;
BEGIN
    WITH moved AS (
        DELETE FROM isomer.key_log WHERE projection = p_projection RETURNING key, token, enqueued_at)
    INSERT INTO isomer.pending AS q (projection, key, token, enqueued_at)
    SELECT p_projection, key, coalesce(min(token) FILTER (WHERE token > 0), 0), min(enqueued_at)
    FROM moved GROUP BY key
    ON CONFLICT (projection, key) DO UPDATE
        SET token = CASE WHEN q.token = 0 THEN EXCLUDED.token
                         WHEN EXCLUDED.token = 0 THEN q.token
                         ELSE least(q.token, EXCLUDED.token) END,
            enqueued_at = least(q.enqueued_at, EXCLUDED.enqueued_at);
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END $$;
REVOKE EXECUTE ON FUNCTION isomer._merge_key_log(text) FROM PUBLIC;

-- Everything this version adds is for the extension and administrators, not for applications.
DO $$
DECLARE
    f regprocedure;
BEGIN
    FOR f IN SELECT p.oid::regprocedure FROM pg_proc p
             WHERE p.pronamespace = 'isomer'::regnamespace
               AND (p.proname LIKE '\_engine%' OR p.proname IN (
                    '_has_equality', '_is_unique', '_primary_key', '_aligned', '_coltype', '_first_placement', '_pick',
                    '_on_source_delta', 'pull_node_deltas', 'choose_strategy', 'strategy_advice', 'create_projection',
                    'alter_projection', '_truth', '_shard_groups', '_runnable', '_key_cost'))
    LOOP
        EXECUTE format('REVOKE EXECUTE ON %s %s FROM PUBLIC',
                       CASE WHEN (SELECT prokind FROM pg_proc WHERE oid = f) = 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END, f);
    END LOOP;
END $$;
