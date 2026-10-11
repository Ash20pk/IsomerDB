-- IsomerDB 0.2.0: vector projections (PRD Phase 1).
--
-- A vector projection keeps an embedding per key for a text expression. Writes to the source
-- tables queue keys exactly as for document projections; draining re-derives each key's text
-- and only queues an embedding job when the text's hash changed. Embeddings are computed by
-- the separate embedder service (services/embedder), never by a Postgres backend.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.2.0'" to load this file. \quit

-- ── Catalog ─────────────────────────────────────────────────────────────────

CREATE TABLE isomer.embedding_models (
    name              text PRIMARY KEY CHECK (name ~ '^[a-z0-9][a-z0-9_.-]{0,62}$'),
    provider          text NOT NULL CHECK (provider IN ('openai', 'local-hash')),
    endpoint          text,              -- openai: base URL ending in /v1
    model             text NOT NULL,     -- the provider's model id, pinned
    dimensions        integer NOT NULL CHECK (dimensions BETWEEN 1 AND 16000),
    api_key_env       text,              -- environment variable on the embedder holding the key
    price_per_million numeric NOT NULL DEFAULT 0,
    max_batch_tokens  integer NOT NULL DEFAULT 8000,
    max_batch_items   integer NOT NULL DEFAULT 96,
    requests_per_min  integer NOT NULL DEFAULT 3000,
    tokens_per_min    integer NOT NULL DEFAULT 1000000,
    created_at        timestamptz NOT NULL DEFAULT now(),
    CHECK (provider <> 'openai' OR endpoint IS NOT NULL)
);
SELECT pg_catalog.pg_extension_config_dump('isomer.embedding_models', '');

-- The deterministic test embedder: feature-hashed words, no network, no meaning. Test only.
INSERT INTO isomer.embedding_models (name, provider, model, dimensions)
VALUES ('local-hash-64', 'local-hash', 'local-hash', 64);

ALTER TABLE isomer.projections DROP CONSTRAINT projections_kind_check;
ALTER TABLE isomer.projections ADD CONSTRAINT projections_kind_check
    CHECK (kind IN ('document', 'search', 'vector'));
ALTER TABLE isomer.projections DROP CONSTRAINT projections_status_check;
ALTER TABLE isomer.projections ADD CONSTRAINT projections_status_check
    CHECK (status IN ('active', 'broken', 'paused_budget'));
ALTER TABLE isomer.projections
    ADD COLUMN model text REFERENCES isomer.embedding_models (name),
    ADD COLUMN pending_model text REFERENCES isomer.embedding_models (name),
    ADD COLUMN monthly_cap_usd numeric;

ALTER TABLE isomer.pending ADD COLUMN phase text NOT NULL DEFAULT 'steady'
    CHECK (phase IN ('backfill', 'steady'));

CREATE SCHEMA isomer_vectors;
REVOKE ALL ON SCHEMA isomer_vectors FROM PUBLIC;

-- One embedding to compute. The embedder leases jobs by pushing not_before forward, so a
-- crashed worker's jobs come back after the lease (a visibility timeout).
CREATE TABLE isomer.embed_jobs (
    projection  text NOT NULL REFERENCES isomer.projections (name) ON DELETE CASCADE,
    key         text NOT NULL,
    model       text NOT NULL REFERENCES isomer.embedding_models (name),
    source_hash text NOT NULL,
    input       text NOT NULL,
    tokens_est  integer NOT NULL,
    phase       text NOT NULL DEFAULT 'steady',
    attempts    integer NOT NULL DEFAULT 0,
    not_before  timestamptz NOT NULL DEFAULT now(),
    last_error  text,
    enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection, key, model)
);
CREATE INDEX embed_jobs_ready ON isomer.embed_jobs (model, not_before);

CREATE TABLE isomer.embed_dead_letter (
    projection  text NOT NULL,
    key         text NOT NULL,
    model       text NOT NULL,
    source_hash text NOT NULL,
    attempts    integer NOT NULL,
    last_error  text,
    failed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (projection, key, model)
);

CREATE TABLE isomer.embedding_usage (
    projection text NOT NULL,          -- '' for query-time embeddings
    model      text NOT NULL,
    month      date NOT NULL,
    phase      text NOT NULL CHECK (phase IN ('backfill', 'steady', 'query')),
    requests   bigint NOT NULL DEFAULT 0,
    items      bigint NOT NULL DEFAULT 0,
    tokens     bigint NOT NULL DEFAULT 0,
    cost_usd   numeric NOT NULL DEFAULT 0,
    PRIMARY KEY (projection, model, month, phase)
);

-- Query-time embedding requests (isomer.embed). Inserted over a loopback connection so the
-- embedder can see them before the asking transaction ends.
CREATE TABLE isomer.embed_requests (
    id         bigserial PRIMARY KEY,
    model      text NOT NULL REFERENCES isomer.embedding_models (name),
    input      text NOT NULL,
    result     text,
    error      text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    done_at    timestamptz
);

REVOKE ALL ON ALL TABLES IN SCHEMA isomer FROM PUBLIC;

CREATE FUNCTION isomer._notify_embedder()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    PERFORM pg_notify('isomer_embed', TG_TABLE_NAME);
    RETURN NULL;
END $$;
CREATE TRIGGER notify_embedder AFTER INSERT ON isomer.embed_jobs
    FOR EACH STATEMENT EXECUTE FUNCTION isomer._notify_embedder();
CREATE TRIGGER notify_embedder AFTER INSERT ON isomer.embed_requests
    FOR EACH STATEMENT EXECUTE FUNCTION isomer._notify_embedder();

-- ── Vector storage ──────────────────────────────────────────────────────────

CREATE FUNCTION isomer._vector_store(p_projection text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT format('isomer_vectors.%I', p_projection)
$$;

-- (Re)create the readable view for the projection's current model: key, embedding typed with
-- the model's dimensions (so the per-model HNSW index applies), and when it was computed.
CREATE FUNCTION isomer._vector_view(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p    isomer.projections;
    dims integer;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    SELECT dimensions INTO STRICT dims FROM isomer.embedding_models WHERE name = p.model;
    EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
    EXECUTE format(
        'CREATE VIEW %I.%I AS SELECT key AS %I, embedding::public.vector(%s) AS embedding, embedded_at '
        'FROM %s WHERE model = %L',
        p.target_schema, p.name, p.key_column, dims, isomer._vector_store(p.name), p.model);
END $$;

CREATE FUNCTION isomer._vector_index(p_projection text, p_model text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off AS $$
DECLARE
    dims integer;
BEGIN
    SELECT dimensions INTO STRICT dims FROM isomer.embedding_models WHERE name = p_model;
    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %s USING hnsw ((embedding::public.vector(%s)) public.vector_cosine_ops) WHERE model = %L',
        left(p_projection || '_' || regexp_replace(p_model, '[^a-z0-9_]', '_', 'g') || '_hnsw', 63),
        isomer._vector_store(p_projection), dims, p_model);
END $$;

-- ── Draining vector projections: text hash diff → embedding jobs ────────────

CREATE FUNCTION isomer._enqueue_embeddings(p_projection text, p_keys text[], p_phase text)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p       isomer.projections;
    v_n     integer := 0;
    v_found text[];
    v_model text;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;

    FOREACH v_model IN ARRAY array_remove(ARRAY[p.model, p.pending_model], NULL) LOOP
        EXECUTE format(
            $q$WITH src AS (
                   SELECT q.%1$I::text AS key, q.input::text AS input, md5(q.input::text) AS hash
                   FROM (%2$s) q
                   WHERE q.%1$I = ANY ($1::%3$s[]) AND q.input IS NOT NULL),
               changed AS (
                   SELECT s.* FROM src s
                   LEFT JOIN %4$s v ON v.key = s.key AND v.model = $2
                   WHERE v.source_hash IS DISTINCT FROM s.hash),
               ins AS (
                   INSERT INTO isomer.embed_jobs AS j (projection, key, model, source_hash, input, tokens_est, phase)
                   SELECT $3, c.key, $2, c.hash, c.input, greatest(1, ceil(length(c.input) / 4.0)::int), $4
                   FROM changed c
                   ON CONFLICT (projection, key, model) DO UPDATE
                       SET source_hash = EXCLUDED.source_hash, input = EXCLUDED.input,
                           tokens_est = EXCLUDED.tokens_est, attempts = 0, not_before = now(), last_error = NULL
                       WHERE j.source_hash <> EXCLUDED.source_hash
                   RETURNING 1)
             SELECT (SELECT count(*) FROM ins)::int, (SELECT coalesce(array_agg(key), '{}') FROM src)$q$,
            p.key_column, p.query, p.key_type, isomer._vector_store(p.name))
        INTO v_n, v_found
        USING p_keys, v_model, p.name, p_phase;
    END LOOP;

    -- Keys the query no longer produces (or whose text became NULL): remove their embeddings.
    EXECUTE format('DELETE FROM %s WHERE key = ANY ($1) AND key <> ALL ($2)', isomer._vector_store(p.name))
        USING p_keys, v_found;
    DELETE FROM isomer.embed_jobs j WHERE j.projection = p.name AND j.key = ANY (p_keys) AND j.key <> ALL (v_found);
    RETURN v_n;
END $$;

-- The embedder calls this with each finished embedding. It is written only if the job still
-- has the same text hash, so a slow result can never overwrite a newer one.
CREATE FUNCTION isomer.embed_complete(p_projection text, p_key text, p_model text,
                                      p_source_hash text, p_embedding text)
RETURNS boolean LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_job      isomer.embed_jobs;
    v_lag_ms   double precision;
    v_budget   interval;
BEGIN
    DELETE FROM isomer.embed_jobs
    WHERE projection = p_projection AND key = p_key AND model = p_model AND source_hash = p_source_hash
    RETURNING * INTO v_job;
    IF NOT FOUND THEN
        RETURN false;   -- superseded by a newer text, or the key was deleted
    END IF;
    DELETE FROM isomer.embed_dead_letter WHERE projection = p_projection AND key = p_key AND model = p_model;
    EXECUTE format(
        'INSERT INTO %s AS v (key, model, embedding, source_hash, embedded_at) VALUES ($1, $2, $3::public.vector, $4, clock_timestamp()) '
        'ON CONFLICT (key, model) DO UPDATE SET embedding = EXCLUDED.embedding, source_hash = EXCLUDED.source_hash, embedded_at = EXCLUDED.embedded_at',
        isomer._vector_store(p_projection))
    USING p_key, p_model, p_embedding, p_source_hash;

    IF v_job.phase = 'steady' THEN
        SELECT lag_budget INTO v_budget FROM isomer.projections WHERE name = p_projection;
        v_lag_ms := extract(epoch FROM clock_timestamp() - v_job.enqueued_at) * 1000;
        INSERT INTO isomer.projection_stats AS s (projection, last_applied_at, last_batch_size, last_lag_ms, max_lag_ms, budget_breaches, documents_written)
        VALUES (p_projection, clock_timestamp(), 1, v_lag_ms, v_lag_ms, (v_lag_ms > extract(epoch FROM v_budget) * 1000)::int, 1)
        ON CONFLICT (projection) DO UPDATE
            SET last_applied_at = EXCLUDED.last_applied_at, last_lag_ms = EXCLUDED.last_lag_ms,
                max_lag_ms = greatest(s.max_lag_ms, EXCLUDED.max_lag_ms),
                budget_breaches = s.budget_breaches + EXCLUDED.budget_breaches,
                documents_written = s.documents_written + 1;
    ELSE
        UPDATE isomer.projection_stats SET documents_written = documents_written + 1 WHERE projection = p_projection;
    END IF;
    RETURN true;
END $$;

-- The embedder calls this when a job fails. Retries back off exponentially (capped at 5
-- minutes, or the provider's Retry-After); after max_attempts the job moves to the dead letter.
CREATE FUNCTION isomer.embed_failed(p_projection text, p_key text, p_model text, p_source_hash text,
                                    p_error text, p_retry_after_s double precision DEFAULT NULL,
                                    p_max_attempts integer DEFAULT 8)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_job isomer.embed_jobs;
BEGIN
    SELECT * INTO v_job FROM isomer.embed_jobs
    WHERE projection = p_projection AND key = p_key AND model = p_model AND source_hash = p_source_hash
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    IF v_job.attempts >= p_max_attempts THEN
        DELETE FROM isomer.embed_jobs WHERE projection = p_projection AND key = p_key AND model = p_model;
        INSERT INTO isomer.embed_dead_letter (projection, key, model, source_hash, attempts, last_error)
        VALUES (p_projection, p_key, p_model, p_source_hash, v_job.attempts, p_error)
        ON CONFLICT (projection, key, model) DO UPDATE
            SET source_hash = EXCLUDED.source_hash, attempts = EXCLUDED.attempts,
                last_error = EXCLUDED.last_error, failed_at = now();
        PERFORM isomer._log('error', p_projection, format('embedding for key %s failed %s times, moved to dead letter: %s',
                                                          p_key, v_job.attempts, p_error));
    ELSE
        UPDATE isomer.embed_jobs
        SET last_error = p_error,
            not_before = now() + make_interval(secs => coalesce(p_retry_after_s,
                             least(300, 2 ^ v_job.attempts) * (0.75 + random() * 0.5)))
        WHERE projection = p_projection AND key = p_key AND model = p_model;
    END IF;
END $$;

-- Retry everything in the dead letter for a projection (e.g. after fixing an API key).
CREATE FUNCTION isomer.retry_dead_letters(p_projection text)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_keys text[];
BEGIN
    SELECT array_agg(key) INTO v_keys FROM (
        DELETE FROM isomer.embed_dead_letter WHERE projection = p_projection RETURNING key) d;
    INSERT INTO isomer.pending (projection, key)
    SELECT p_projection, k FROM unnest(v_keys) k ON CONFLICT DO NOTHING;
    RETURN coalesce(cardinality(v_keys), 0);
END $$;

-- ── Spend and caps (FR-V2) ──────────────────────────────────────────────────

CREATE FUNCTION isomer.record_embedding_usage(p_projection text, p_model text, p_phase text,
                                              p_items integer, p_tokens bigint)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_price numeric;
    v_cap   numeric;
    v_spent numeric;
BEGIN
    SELECT price_per_million INTO v_price FROM isomer.embedding_models WHERE name = p_model;
    INSERT INTO isomer.embedding_usage AS u (projection, model, month, phase, requests, items, tokens, cost_usd)
    VALUES (coalesce(p_projection, ''), p_model, date_trunc('month', now())::date, p_phase, 1, p_items, p_tokens,
            p_tokens * coalesce(v_price, 0) / 1000000)
    ON CONFLICT (projection, model, month, phase) DO UPDATE
        SET requests = u.requests + 1, items = u.items + EXCLUDED.items,
            tokens = u.tokens + EXCLUDED.tokens, cost_usd = u.cost_usd + EXCLUDED.cost_usd;

    IF p_projection IS NOT NULL THEN
        SELECT monthly_cap_usd INTO v_cap FROM isomer.projections WHERE name = p_projection;
        IF v_cap IS NOT NULL THEN
            SELECT sum(cost_usd) INTO v_spent FROM isomer.embedding_usage
            WHERE projection = p_projection AND month = date_trunc('month', now())::date;
            IF v_spent >= v_cap THEN
                UPDATE isomer.projections SET status = 'paused_budget' WHERE name = p_projection AND status = 'active';
                IF FOUND THEN
                    PERFORM isomer._log('warning', p_projection, format(
                        'monthly embedding cap $%s reached ($%s spent): paused until next month or a higher cap',
                        v_cap, round(v_spent, 4)));
                END IF;
            END IF;
        END IF;
    END IF;
END $$;

CREATE FUNCTION isomer.set_embedding_cap(p_projection text, p_monthly_usd numeric)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_spent numeric;
BEGIN
    UPDATE isomer.projections SET monthly_cap_usd = p_monthly_usd WHERE name = p_projection AND kind = 'vector';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no vector projection named %', p_projection;
    END IF;
    SELECT coalesce(sum(cost_usd), 0) INTO v_spent FROM isomer.embedding_usage
    WHERE projection = p_projection AND month = date_trunc('month', now())::date;
    IF p_monthly_usd IS NULL OR v_spent < p_monthly_usd THEN
        UPDATE isomer.projections SET status = 'active' WHERE name = p_projection AND status = 'paused_budget';
    END IF;
END $$;

-- Tokens and cost to embed everything the projection would embed with a model, before
-- committing to it (FR-V2). Tokens are estimated at 4 characters each.
CREATE FUNCTION isomer.estimate_embedding_cost(p_query text, p_model text)
RETURNS TABLE (rows bigint, est_tokens bigint, est_cost_usd numeric)
LANGUAGE plpgsql AS $$
DECLARE
    v_price numeric;
BEGIN
    SELECT price_per_million INTO STRICT v_price FROM isomer.embedding_models WHERE name = p_model;
    RETURN QUERY EXECUTE format(
        'SELECT count(*), coalesce(sum(greatest(1, ceil(length(q.input::text) / 4.0)))::bigint, 0), '
        'round(coalesce(sum(greatest(1, ceil(length(q.input::text) / 4.0))), 0) * $1 / 1000000, 6) '
        'FROM (%s) q WHERE q.input IS NOT NULL', p_query)
    USING v_price;
END $$;

-- ── Query-time embeddings (FR-V4) ───────────────────────────────────────────

-- Embed a query string with a model, through the embedder. The request is written over a
-- loopback connection (so the embedder sees it at once) and this call waits for the answer.
-- Needs READ COMMITTED: under REPEATABLE READ the answer is never visible to the waiting query.
CREATE FUNCTION isomer.embed(p_input text, p_model text, p_timeout interval DEFAULT '10 seconds')
RETURNS public.vector LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    v_conn   text := format('dbname=%s user=%s', quote_literal(current_database()), quote_literal(current_user));
    v_id     bigint;
    v_result text;
    v_error  text;
    v_until  timestamptz := clock_timestamp() + p_timeout;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'isomer.embed needs READ COMMITTED isolation (this transaction is %)',
            current_setting('transaction_isolation');
    END IF;
    SELECT id INTO v_id FROM public.dblink(v_conn, format(
        'INSERT INTO isomer.embed_requests (model, input) VALUES (%L, %L) RETURNING id', p_model, p_input))
        AS t(id bigint);
    LOOP
        SELECT result, error INTO v_result, v_error FROM isomer.embed_requests WHERE id = v_id;
        EXIT WHEN v_result IS NOT NULL OR v_error IS NOT NULL;
        IF clock_timestamp() > v_until THEN
            RAISE EXCEPTION 'isomer.embed: no answer from the embedder within % (is it running?)', p_timeout;
        END IF;
        PERFORM pg_sleep(0.005);
    END LOOP;
    PERFORM public.dblink_exec(v_conn, format('DELETE FROM isomer.embed_requests WHERE id = %s', v_id));
    IF v_error IS NOT NULL THEN
        RAISE EXCEPTION 'isomer.embed: %', v_error;
    END IF;
    RETURN v_result::public.vector;
END $$;

-- Embed with a vector projection's current model.
CREATE FUNCTION isomer.embed_for(p_projection text, p_input text)
RETURNS public.vector LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    v_model text;
BEGIN
    SELECT model INTO STRICT v_model FROM isomer.projections WHERE name = p_projection AND kind = 'vector';
    RETURN isomer.embed(p_input, v_model);
END $$;

-- ── Model upgrades (FR-V3) ──────────────────────────────────────────────────

-- Switch a vector projection to another model: embed everything with the new model in the
-- background, keep serving the old one, then swap atomically when the new one is complete.
CREATE FUNCTION isomer.set_projection_model(p_projection text, p_model text)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection AND kind = 'vector';
    IF p.model = p_model THEN
        RETURN format('%s already uses %s', p_projection, p_model);
    END IF;
    PERFORM 1 FROM isomer.embedding_models WHERE name = p_model;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no embedding model named %', p_model;
    END IF;
    UPDATE isomer.projections SET pending_model = p_model WHERE name = p_projection;
    PERFORM isomer._vector_index(p_projection, p_model);
    INSERT INTO isomer.pending (projection, key, phase)
    SELECT p_projection, k, 'backfill' FROM isomer._all_keys(p_projection) k
    ON CONFLICT (projection, key) DO UPDATE SET phase = 'backfill';
    PERFORM isomer._log('info', p_projection, format('re-embedding with %s in the background; %s keeps serving reads', p_model, p.model));
    RETURN format('%s: re-embedding with %s; reads stay on %s until it completes', p_projection, p_model, p.model);
END $$;

-- Swap to the pending model once every key has an embedding with it.
CREATE FUNCTION isomer._maybe_swap_model(p_projection text)
RETURNS boolean LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p          isomer.projections;
    v_expected bigint;
    v_have     bigint;
    v_old      text;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection FOR UPDATE;
    IF p.pending_model IS NULL
       OR EXISTS (SELECT 1 FROM isomer.pending WHERE projection = p.name)
       OR EXISTS (SELECT 1 FROM isomer.embed_jobs WHERE projection = p.name AND model = p.pending_model) THEN
        RETURN false;
    END IF;
    EXECUTE format('SELECT count(*) FROM (%s) q WHERE q.input IS NOT NULL', p.query) INTO v_expected;
    EXECUTE format('SELECT count(*) FROM %s WHERE model = $1', isomer._vector_store(p.name)) INTO v_have USING p.pending_model;
    IF v_have < v_expected THEN
        RETURN false;
    END IF;
    v_old := p.model;
    UPDATE isomer.projections SET model = pending_model, pending_model = NULL WHERE name = p.name;
    PERFORM isomer._vector_view(p.name);
    EXECUTE format('DELETE FROM %s WHERE model = $1', isomer._vector_store(p.name)) USING v_old;
    EXECUTE format('DROP INDEX IF EXISTS isomer_vectors.%I',
                   left(p.name || '_' || regexp_replace(v_old, '[^a-z0-9_]', '_', 'g') || '_hnsw', 63));
    PERFORM isomer._log('info', p.name, format('switched from %s to %s (%s embeddings)', v_old, p.pending_model, v_have));
    RETURN true;
END $$;

-- ── Install / drop / drain: extended for vector projections ─────────────────

CREATE OR REPLACE FUNCTION isomer._install(p_projection text)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p         isomer.projections;
    s         record;
    v_trigger text := format('isomer_%s', p_projection);
    v_target  text;
    v_count   bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;

    IF p.kind IN ('document', 'vector') THEN
        IF p.kind = 'document' THEN
            PERFORM documentdb_api.create_collection(p.mongo_database, p.name);
        ELSE
            PERFORM set_config('citus.enable_ddl_propagation', 'off', true);
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %s (key text NOT NULL, model text NOT NULL, embedding public.vector NOT NULL, '
                'source_hash text NOT NULL, embedded_at timestamptz NOT NULL, PRIMARY KEY (key, model))',
                isomer._vector_store(p.name));
            PERFORM isomer._vector_index(p.name, p.model);
            PERFORM isomer._vector_view(p.name);
        END IF;
        FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger, s.source_table);
            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger || '_truncate', s.source_table);
            EXECUTE format(
                'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW '
                'EXECUTE FUNCTION isomer._on_source_change(%L, %L, %L)',
                v_trigger, s.source_table, p.name, coalesce(s.key_column, ''), p.freshness);
            EXECUTE format(
                'CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT '
                'EXECUTE FUNCTION isomer._on_source_change(%L, %L, %L)',
                v_trigger || '_truncate', s.source_table, p.name, coalesce(s.key_column, ''), p.freshness);
            EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %I', s.source_table, v_trigger);
            EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %I', s.source_table, v_trigger || '_truncate');
        END LOOP;
        INSERT INTO isomer.projection_stats (projection) VALUES (p.name) ON CONFLICT DO NOTHING;
        IF p.kind = 'document' THEN
            v_count := isomer._rebuild_all(p.name);
            RETURN format('document projection %s: %s documents in collection %s.%s (%s)',
                          p.name, v_count, p.mongo_database, p.name, p.freshness);
        END IF;
        INSERT INTO isomer.pending (projection, key, phase)
        SELECT p.name, k, 'backfill' FROM isomer._all_keys(p.name) k
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

-- Re-derive a whole projection after its sources were truncated or replaced.
CREATE FUNCTION isomer._full_refresh(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    IF p.kind = 'document' THEN
        PERFORM isomer._rebuild_all(p.name);
    ELSIF p.kind = 'vector' THEN
        EXECUTE format('DELETE FROM %s WHERE key NOT IN (SELECT isomer._all_keys(%L))',
                       isomer._vector_store(p.name), p.name);
        DELETE FROM isomer.embed_jobs j WHERE j.projection = p.name
            AND j.key NOT IN (SELECT isomer._all_keys(p.name));
        INSERT INTO isomer.pending (projection, key)
        SELECT p.name, k FROM isomer._all_keys(p.name) k ON CONFLICT DO NOTHING;
    END IF;
END $$;

-- Source trigger, now kind-aware for TRUNCATE. Vector projections are always bounded (an
-- embedding call can't run inside the writing transaction), so they never take the sync path.
CREATE OR REPLACE FUNCTION isomer._on_source_change()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_projection text := TG_ARGV[0];
    v_column     text := TG_ARGV[1];
    v_freshness  text := TG_ARGV[2];
    v_keys       text[];
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        PERFORM isomer._log('info', v_projection, format('%s truncated: full refresh', TG_TABLE_NAME));
        PERFORM isomer._full_refresh(v_projection);
        RETURN NULL;
    END IF;

    IF v_column = '' THEN
        IF v_freshness = 'sync' THEN
            PERFORM isomer._rebuild_all(v_projection);
        ELSE
            INSERT INTO isomer.pending (projection, key)
            SELECT v_projection, k FROM isomer._all_keys(v_projection) k
            ON CONFLICT DO NOTHING;
        END IF;
        RETURN NULL;
    END IF;

    SELECT array_agg(DISTINCT k) INTO v_keys
    FROM unnest(ARRAY[
        CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) ->> v_column END,
        CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) ->> v_column END]) k
    WHERE k IS NOT NULL;

    IF v_freshness = 'sync' THEN
        PERFORM isomer._apply(v_projection, v_keys);
    ELSE
        INSERT INTO isomer.pending (projection, key)
        SELECT v_projection, k FROM unnest(v_keys) k
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION isomer.drain(p_batch integer DEFAULT 500)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        record;
    v_keys   text[];
    v_phases text[];
    v_oldest timestamptz;
    v_n      integer;
    v_total  integer := 0;
    v_lag_ms double precision;
BEGIN
    FOR p IN SELECT name, kind, lag_budget FROM isomer.projections
             WHERE kind IN ('document', 'vector') AND freshness = 'bounded' AND status = 'active'
    LOOP
        LOOP
            BEGIN
                WITH batch AS (
                    DELETE FROM isomer.pending
                    WHERE (projection, key) IN (
                        SELECT projection, key FROM isomer.pending
                        WHERE projection = p.name
                        ORDER BY enqueued_at LIMIT p_batch
                        FOR UPDATE SKIP LOCKED)
                    RETURNING key, enqueued_at, phase)
                SELECT array_agg(key), array_agg(DISTINCT phase), min(enqueued_at), count(*)
                INTO v_keys, v_phases, v_oldest, v_n FROM batch;

                EXIT WHEN v_n = 0;
                IF p.kind = 'document' THEN
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
                    -- Vector: jobs carry the time the change was first seen, for lag accounting.
                    PERFORM isomer._enqueue_embeddings(p.name, v_keys,
                        CASE WHEN 'backfill' = ANY (v_phases) THEN 'backfill' ELSE 'steady' END);
                    UPDATE isomer.embed_jobs SET enqueued_at = least(enqueued_at, v_oldest)
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

-- create_projection gains kind 'vector' and model; the rest is unchanged.
DROP FUNCTION isomer.create_projection(text, text, text, text, text, interval, jsonb, text, text[], text);

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
    model         text     DEFAULT NULL)
-- No SET search_path here: the query and source names resolve with the caller's search path,
-- then get stored schema-qualified. Probe views are local, so Citus must not propagate them.
RETURNS text LANGUAGE plpgsql SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_probe    text := format('pg_temp.isomer_create_%s', pg_backend_pid());
    v_cols     record;
    v_key_type text;
    v_has_doc  boolean := false;
    v_has_input boolean := false;
    v_tables   regclass[];
    v_t        regclass;
    v_src      record;
    v_index    text[];
    v_attached text;
BEGIN
    IF kind NOT IN ('document', 'search', 'vector') THEN
        RAISE EXCEPTION 'isomer: kind must be ''document'', ''search'' or ''vector'', not %', kind;
    END IF;
    IF freshness NOT IN ('sync', 'bounded') THEN
        RAISE EXCEPTION 'isomer: freshness must be ''sync'' or ''bounded'', not %', freshness;
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

    v_tables := ARRAY(SELECT isomer._query_tables(query));
    IF cardinality(v_tables) = 0 THEN
        RAISE EXCEPTION 'isomer: the query reads no tables';
    END IF;
    FOREACH v_t IN ARRAY v_tables LOOP
        IF isomer._is_distributed(v_t) THEN
            RAISE EXCEPTION 'isomer: % is a Citus distributed table; projections over distributed tables are not supported yet (planned for primary mode, PRD Phase 2)', v_t;
        END IF;
        v_attached := coalesce(v_attached, isomer._attachment_of(v_t));
    END LOOP;
    IF freshness = 'sync' AND v_attached IS NOT NULL THEN
        RAISE EXCEPTION 'isomer: projection % cannot be sync: it reads tables attached from another database (attachment %)', name, v_attached
            USING HINT = 'Use freshness => ''bounded''. Sync projections need IsomerDB to be the primary for their tables.';
    END IF;

    INSERT INTO isomer.projections (name, kind, query, key_column, key_type, freshness, lag_budget,
                                    mongo_database, target_schema, index_columns, model)
    VALUES (name, kind, query, key, v_key_type, freshness, lag,
            CASE WHEN kind = 'document' THEN database END,
            CASE WHEN kind IN ('search', 'vector') THEN target_schema END,
            CASE WHEN kind = 'search' THEN key || coalesce(index_columns, v_index) END,
            CASE WHEN kind = 'vector' THEN model END);

    IF kind IN ('document', 'vector') THEN
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

    RETURN isomer._install(name);
END $$;

CREATE OR REPLACE FUNCTION isomer.drop_projection(name text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p isomer.projections;
    s record;
BEGIN
    SELECT * INTO p FROM isomer.projections WHERE projections.name = drop_projection.name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', name;
    END IF;
    IF p.kind IN ('document', 'vector') THEN
        FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
            IF to_regclass(s.source_table) IS NOT NULL THEN
                EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', 'isomer_' || p.name, s.source_table);
                EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', 'isomer_' || p.name || '_truncate', s.source_table);
            END IF;
        END LOOP;
        IF p.kind = 'document' THEN
            PERFORM documentdb_api.drop_collection(p.mongo_database, p.name);
        ELSE
            EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
            EXECUTE format('DROP TABLE IF EXISTS %s', isomer._vector_store(p.name));
        END IF;
    ELSE
        PERFORM set_config('isomer.dropping_projection', p.name, true);
        PERFORM pgtrickle.drop_stream_table(format('%I.%I', p.target_schema, p.name), false);
    END IF;
    DELETE FROM isomer.embed_dead_letter WHERE projection = p.name;
    DELETE FROM isomer.projections WHERE projections.name = p.name;
END $$;

CREATE OR REPLACE FUNCTION isomer.dump_projections()
RETURNS SETOF text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT format('SELECT isomer.create_projection(name => %L, query => %L, kind => %L, key => %L, '
                  'freshness => %L, lag => %L, sources => %L, database => %L, index_columns => %L, target_schema => %L, model => %L);',
                  p.name, p.query, p.kind, p.key_column, p.freshness, p.lag_budget,
                  (SELECT jsonb_object_agg(s.source_table, s.key_column) FROM isomer.projection_sources s WHERE s.projection = p.name),
                  coalesce(p.mongo_database, 'isomer'), p.index_columns, coalesce(p.target_schema, 'public'), p.model)
    FROM isomer.projections p ORDER BY p.created_at
$$;

CREATE FUNCTION isomer.add_embedding_model(
    name              text,
    provider          text,
    model             text,
    dimensions        integer,
    endpoint          text    DEFAULT NULL,
    api_key_env       text    DEFAULT NULL,
    price_per_million numeric DEFAULT 0,
    max_batch_tokens  integer DEFAULT 8000,
    max_batch_items   integer DEFAULT 96,
    requests_per_min  integer DEFAULT 3000,
    tokens_per_min    integer DEFAULT 1000000)
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    INSERT INTO isomer.embedding_models (name, provider, model, dimensions, endpoint, api_key_env, price_per_million,
                                         max_batch_tokens, max_batch_items, requests_per_min, tokens_per_min)
    VALUES (name, provider, model, dimensions, endpoint, api_key_env, price_per_million,
            max_batch_tokens, max_batch_items, requests_per_min, tokens_per_min)
$$;

-- ── Status (FR-O1/O2) ───────────────────────────────────────────────────────

DROP VIEW isomer.projection_status;
CREATE VIEW isomer.projection_status AS
SELECT p.name, p.kind, p.freshness, p.lag_budget, p.status,
       (SELECT count(*) FROM isomer.pending q WHERE q.projection = p.name) AS pending_keys,
       (SELECT clock_timestamp() - min(q.enqueued_at) FROM isomer.pending q WHERE q.projection = p.name) AS oldest_pending,
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
       p.monthly_cap_usd
FROM isomer.projections p
LEFT JOIN isomer.projection_stats s ON s.projection = p.name
LEFT JOIN pgtrickle.pgt_status() t ON p.kind = 'search' AND t.name = format('%s.%s', p.target_schema, p.name)
LEFT JOIN pgtrickle.pgt_stream_tables st ON p.kind = 'search' AND st.pgt_schema = p.target_schema AND st.pgt_name = p.name;

-- Apply-worker errors are cumulative in pg_stat_subscription_stats; remember the last count so
-- health checks report new errors rather than lifetime totals.
ALTER TABLE isomer.attachments ADD COLUMN last_apply_errors bigint NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION isomer.check_attachments(p_max_retained_wal bigint DEFAULT 1024 * 1024 * 1024)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a  isomer.attachments;
    st record;
BEGIN
    FOR a IN SELECT * FROM isomer.attachments WHERE status = 'active' LOOP
        BEGIN
            BEGIN
                PERFORM isomer._remote_call(a.conninfo, 'SELECT pg_logical_emit_message(false, ''isomer.heartbeat'', now()::text)');
            EXCEPTION WHEN insufficient_privilege THEN
                NULL;
            END;

            SELECT * INTO st FROM isomer.attachment_status(a.name);
            IF st.wal_status IN ('lost') OR (st.slot_active IS NULL AND st.wal_status IS NULL) THEN
                PERFORM isomer._log('error', a.name, format('replication slot %s is %s on the source: re-snapshotting',
                                                            a.slot, coalesce(st.wal_status, 'missing')));
                PERFORM isomer.resync(a.name);
                CONTINUE;
            END IF;
            IF st.retained_wal_bytes > p_max_retained_wal THEN
                PERFORM isomer._log('warning', a.name, format('slot %s is holding %s of WAL on the source',
                                                              a.slot, pg_size_pretty(st.retained_wal_bytes)));
            END IF;
            IF coalesce(st.apply_errors, 0) > a.last_apply_errors THEN
                PERFORM isomer._log('warning', a.name, format(
                    'the apply worker hit %s error(s) since the last check (a source schema change not yet applied here causes this until the next check applies it)',
                    st.apply_errors - a.last_apply_errors));
                UPDATE isomer.attachments SET last_apply_errors = st.apply_errors WHERE name = a.name;
            END IF;

            IF a.needs_refresh THEN
                PERFORM isomer._loopback(format('ALTER SUBSCRIPTION %I REFRESH PUBLICATION', a.subscription));
                UPDATE isomer.attachments SET needs_refresh = false WHERE name = a.name;
            END IF;
            PERFORM isomer._sync_schema(a);
        EXCEPTION WHEN OTHERS THEN
            PERFORM isomer._log('error', a.name, format('health check failed: %s', SQLERRM));
        END;
    END LOOP;
END $$;

-- Alerts: anything an operator should look at now. Polled by the console, or by monitoring.
CREATE VIEW isomer.alerts AS
SELECT 'lag_budget' AS alert, name AS subject,
       format('pending for %s (budget %s)', oldest_pending, lag_budget) AS detail
FROM isomer.projection_status
WHERE kind IN ('document', 'vector') AND oldest_pending > lag_budget
UNION ALL
SELECT 'embed_lag', name, format('oldest embedding job waiting %s (budget %s)', oldest_embed_job, lag_budget)
FROM isomer.projection_status WHERE kind = 'vector' AND status = 'active' AND oldest_embed_job > lag_budget
UNION ALL
SELECT 'search_stale', name, format('stale for %s (budget %s)', search_staleness, lag_budget)
FROM isomer.projection_status WHERE kind = 'search' AND search_staleness > lag_budget * 3
UNION ALL
SELECT 'dead_letters', name, format('%s embeddings failed permanently', embed_dead_letters)
FROM isomer.projection_status WHERE embed_dead_letters > 0
UNION ALL
SELECT 'budget_paused', name, format('monthly cap $%s reached', monthly_cap_usd)
FROM isomer.projection_status WHERE status = 'paused_budget'
UNION ALL
SELECT 'attachment', attachment, format('%s: slot wal_status %s', status, wal_status)
FROM isomer.attachment_status()
WHERE status <> 'active' OR wal_status IS DISTINCT FROM 'reserved' AND wal_status IS DISTINCT FROM 'extended'
UNION ALL
SELECT level, subject, message FROM isomer.events
WHERE level IN ('error', 'warning') AND at > now() - interval '15 minutes';
