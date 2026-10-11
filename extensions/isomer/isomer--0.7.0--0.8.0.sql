-- IsomerDB 0.8.0: projection history. One change log per projection backs three features:
--   time travel   isomer.as_of(projection, key, at) and isomer.as_of(projection, at)
--   event feeds   isomer.changes_since(projection, cursor): every change, in commit order, gap-free
--   live sync     the HTTP API streams the same feed (services/api), woken by NOTIFY isomer_changes
-- plus the pieces the HTTP API needs: key lookup for any key-addressed projection, API keys mapped
-- to Postgres roles, and the access rule (a role reads a projection only if it can read every
-- table behind it, and none of them has row-level security).

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.8.0'" to load this file. \quit

ALTER TABLE isomer.projections ADD COLUMN history interval;                 -- retention; NULL = off
ALTER TABLE isomer.projections ADD COLUMN history_since timestamptz;        -- when the log was started
ALTER TABLE isomer.projections ADD COLUMN history_pruned_through bigint NOT NULL DEFAULT 0;

CREATE SEQUENCE isomer.changes_id_seq;

-- Ids are assigned while holding a per-projection transaction lock, so within one projection they
-- are in commit order: once id N is visible, every smaller id of that projection is committed or
-- was rolled back. That is what makes a cursor (id > N) gap-free.
CREATE TABLE isomer.changes (
    id         bigint PRIMARY KEY,
    projection text NOT NULL REFERENCES isomer.projections (name) ON DELETE CASCADE,
    key        text NOT NULL,
    op         char(1) NOT NULL CHECK (op IN ('u', 'd')),   -- upsert or delete
    doc        jsonb,                                         -- NULL for a delete
    token      bigint NOT NULL,
    at         timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX changes_key ON isomer.changes (projection, key, id);
CREATE INDEX changes_feed ON isomer.changes (projection, id);
CREATE INDEX changes_at ON isomer.changes (projection, at);

CREATE TABLE isomer.api_keys (
    id         bigserial PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    key_sha256 bytea NOT NULL UNIQUE,
    role       name NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);
REVOKE ALL ON isomer.changes, isomer.api_keys FROM PUBLIC;

-- ── the change log ──────────────────────────────────────────────────────────

-- Record what changed for these keys. p_docs maps each key that has a row to its new document;
-- a key missing from it was deleted. Unchanged documents are not logged.
CREATE FUNCTION isomer._log_changes(p isomer.projections, p_keys text[], p_docs jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_n integer;
BEGIN
    IF p.history IS NULL THEN
        RETURN;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtext('isomer.changes'), hashtext(p.name));
    INSERT INTO isomer.changes (id, projection, key, op, doc, token)
    SELECT nextval('isomer.changes_id_seq'), p.name, k.key,
           CASE WHEN n.doc IS NULL THEN 'd' ELSE 'u' END, n.doc, isomer.current_token()
    FROM (SELECT DISTINCT unnest(p_keys) AS key ORDER BY 1) k
    LEFT JOIN jsonb_each(coalesce(p_docs, '{}')) n (key, doc) ON n.key = k.key
    LEFT JOIN LATERAL (SELECT c.op, c.doc FROM isomer.changes c
                       WHERE c.projection = p.name AND c.key = k.key
                       ORDER BY c.id DESC LIMIT 1) last ON true
    WHERE CASE WHEN n.doc IS NULL THEN last.op = 'u'
               ELSE last.op IS DISTINCT FROM 'u' OR last.doc IS DISTINCT FROM n.doc END;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n > 0 THEN
        PERFORM pg_notify('isomer_changes', p.name);
    END IF;
END $$;

CREATE OR REPLACE FUNCTION isomer._apply(p_projection text, p_keys text[])
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p          isomer.projections;
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

    IF p.kind = 'cache' THEN
        EXECUTE format(
            $q$WITH src AS (SELECT q.%1$I::text AS key, to_jsonb(q.value) AS value FROM (%2$s) q WHERE q.%1$I = ANY ($1::%3$s[])),
                    up AS (INSERT INTO %4$s AS c (key, value, updated_at) SELECT key, value, clock_timestamp() FROM src
                           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                           WHERE c.value IS DISTINCT FROM EXCLUDED.value RETURNING 1),
                    del AS (DELETE FROM %4$s c WHERE c.key = ANY ($1) AND c.key NOT IN (SELECT key FROM src) RETURNING 1)
               SELECT (SELECT count(*) FROM up)::int, (SELECT count(*) FROM del)::int,
                      (SELECT jsonb_object_agg(key, value) FROM src WHERE $2)$q$,
            p.key_column, p.query, p.key_type, isomer._cache_store(p.name))
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
        p.key_column, p.query, p.key_type)
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

-- Start (or stop, with NULL) keeping history for a projection. Starting takes a baseline of every
-- current document, with writes to the source tables held off for its duration so no change can
-- fall between the baseline and the log.
CREATE FUNCTION isomer.set_history(name text, retention interval)
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
            p.key_column, p.query, p.key_type, p.name);
    ELSE
        EXECUTE format(
            $q$INSERT INTO isomer.changes (id, projection, key, op, doc, token)
               SELECT nextval('isomer.changes_id_seq'), %3$L, q.%1$I::text, 'u', to_jsonb(q.value), isomer.current_token()
               FROM (%2$s) q ORDER BY q.%1$I$q$,
            p.key_column, p.query, p.name);
    END IF;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN format('history for %s is on: %s documents in the baseline, kept for %s', p.name, v_n, retention);
END $$;

-- The oldest moment history can answer for.
CREATE FUNCTION isomer.history_horizon(p_projection text)
RETURNS timestamptz LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT greatest(history_since, now() - history) FROM isomer.projections WHERE name = p_projection AND history IS NOT NULL
$$;

CREATE FUNCTION isomer._check_as_of(p_projection text, p_at timestamptz)
RETURNS void LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    v_horizon timestamptz := isomer.history_horizon(p_projection);
BEGIN
    IF v_horizon IS NULL THEN
        RAISE EXCEPTION 'isomer: projection % keeps no history', p_projection
            USING HINT = format('SELECT isomer.set_history(%L, ''7 days'')', p_projection);
    END IF;
    IF p_at < v_horizon THEN
        RAISE EXCEPTION 'isomer: % is before the history of % (it starts at %)', p_at, p_projection, v_horizon
            USING ERRCODE = 'IS002';
    END IF;
END $$;

-- One document as it was at a moment. NULL: the key had no document then.
CREATE FUNCTION isomer.as_of(p_projection text, p_key text, p_at timestamptz)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    v jsonb;
BEGIN
    PERFORM isomer._check_as_of(p_projection, p_at);
    SELECT CASE WHEN c.op = 'u' THEN c.doc END INTO v
    FROM isomer.changes c
    WHERE c.projection = p_projection AND c.key = p_key AND c.at <= p_at
    ORDER BY c.id DESC LIMIT 1;
    RETURN v;
END $$;

-- The whole projection as it was at a moment.
CREATE FUNCTION isomer.as_of(p_projection text, p_at timestamptz)
RETURNS TABLE (key text, doc jsonb) LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
BEGIN
    PERFORM isomer._check_as_of(p_projection, p_at);
    RETURN QUERY
        SELECT l.key, l.doc FROM (
            SELECT DISTINCT ON (c.key) c.key, c.op, c.doc FROM isomer.changes c
            WHERE c.projection = p_projection AND c.at <= p_at
            ORDER BY c.key, c.id DESC) l
        WHERE l.op = 'u';
END $$;

-- Every version of one document still in the log, oldest first.
CREATE FUNCTION isomer.history(p_projection text, p_key text)
RETURNS TABLE (id bigint, op text, doc jsonb, token bigint, at timestamptz)
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT c.id, CASE c.op WHEN 'u' THEN 'upsert' ELSE 'delete' END, c.doc, c.token, c.at
    FROM isomer.changes c WHERE c.projection = p_projection AND c.key = p_key ORDER BY c.id
$$;

-- The event feed: changes after a cursor, in commit order. Start at 0 for everything kept; a
-- cursor that history has already pruned past fails with IS003 (re-read a snapshot with as_of,
-- then follow from the id it reports).
CREATE FUNCTION isomer.changes_since(p_projection text, p_after bigint DEFAULT 0, p_limit integer DEFAULT 1000)
RETURNS TABLE (id bigint, key text, op text, doc jsonb, token bigint, at timestamptz)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
BEGIN
    SELECT * INTO p FROM isomer.projections pr WHERE pr.name = p_projection;
    IF NOT FOUND OR p.history IS NULL THEN
        RAISE EXCEPTION 'isomer: projection % keeps no history', p_projection
            USING HINT = format('SELECT isomer.set_history(%L, ''7 days'')', p_projection);
    END IF;
    IF p_after > 0 AND p_after < p.history_pruned_through THEN
        RAISE EXCEPTION 'isomer: cursor % of % is older than its history (pruned through %)', p_after, p_projection, p.history_pruned_through
            USING ERRCODE = 'IS003', HINT = 'Read a snapshot with isomer.snapshot(), then follow the feed from its cursor.';
    END IF;
    RETURN QUERY
        SELECT c.id, c.key, CASE c.op WHEN 'u' THEN 'upsert' ELSE 'delete' END, c.doc, c.token, c.at
        FROM isomer.changes c
        WHERE c.projection = p_projection AND c.id > p_after
        ORDER BY c.id LIMIT p_limit;
END $$;

-- A consistent starting point for a feed consumer: the current documents (taken from the log, as
-- of one cursor) and that cursor. Follow with changes_since(projection, cursor).
CREATE FUNCTION isomer.snapshot(p_projection text, OUT cursor bigint, OUT docs jsonb)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
BEGIN
    IF isomer.history_horizon(p_projection) IS NULL THEN
        RAISE EXCEPTION 'isomer: projection % keeps no history', p_projection;
    END IF;
    SELECT coalesce(max(c.id), 0) INTO cursor FROM isomer.changes c WHERE c.projection = p_projection;
    SELECT coalesce(jsonb_object_agg(l.key, l.doc), '{}') INTO docs FROM (
        SELECT DISTINCT ON (c.key) c.key, c.op, c.doc FROM isomer.changes c
        WHERE c.projection = p_projection AND c.id <= cursor
        ORDER BY c.key, c.id DESC) l
    WHERE l.op = 'u';
END $$;

-- Drop what has aged out of retention, keeping each key's version as of the cutoff, so as_of
-- still answers at the horizon. Scheduled every minute.
CREATE FUNCTION isomer.prune_history()
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        record;
    v_cutoff timestamptz;
    v_max    bigint;
    v_n      bigint;
    v_total  bigint := 0;
BEGIN
    FOR p IN SELECT name, history FROM isomer.projections WHERE history IS NOT NULL LOOP
        v_cutoff := now() - p.history;
        WITH gone AS (
            DELETE FROM isomer.changes c
            WHERE c.projection = p.name AND c.at < v_cutoff
              -- superseded before the cutoff, or a delete (a key with no version at the cutoff
              -- reads as absent either way)
              AND (c.op = 'd'
                   OR EXISTS (SELECT 1 FROM isomer.changes n
                              WHERE n.projection = c.projection AND n.key = c.key AND n.id > c.id AND n.at < v_cutoff))
            RETURNING c.id)
        SELECT count(*), max(id) INTO v_n, v_max FROM gone;
        IF v_n > 0 THEN
            UPDATE isomer.projections SET history_pruned_through = greatest(history_pruned_through, v_max) WHERE name = p.name;
            v_total := v_total + v_n;
        END IF;
    END LOOP;
    RETURN v_total;
END $$;

-- ── read your writes across nodes: check order ──────────────────────────────

-- A queued key moves from a worker's node_queue to the coordinator's pending: the pull commits the
-- coordinator's copy first, then the worker's delete. Checked in one expression, pending was read
-- with a snapshot taken before the workers were asked, so a key moving in between was seen in
-- neither place and wait_for returned early (seen in multinode.sh, about one write in five on a
-- freshly added node). Ask the workers first; then read pending in a separate statement, whose
-- fresh snapshot includes anything the workers no longer hold.
CREATE OR REPLACE FUNCTION isomer._queued_before(p_barrier timestamptz, p_projection text)
RETURNS boolean LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    v_remote boolean;
BEGIN
    -- Fail closed: a worker that doesn't answer counts as still having queued work.
    SELECT coalesce(bool_or(NOT success OR result = 't'), false) INTO v_remote
    FROM run_command_on_workers(format(
        'SELECT EXISTS (SELECT 1 FROM isomer.node_queue WHERE enqueued_at <= %L %s)',
        p_barrier, CASE WHEN p_projection IS NULL THEN '' ELSE format('AND projection = %L', p_projection) END));
    IF v_remote THEN
        RETURN true;
    END IF;
    RETURN EXISTS (SELECT 1 FROM isomer.pending
                   WHERE token > 0 AND enqueued_at <= p_barrier AND (p_projection IS NULL OR projection = p_projection))
        OR EXISTS (SELECT 1 FROM isomer.embed_jobs
                   WHERE token > 0 AND enqueued_at <= p_barrier AND (p_projection IS NULL OR projection = p_projection));
END $$;

-- ── key lookup ──────────────────────────────────────────────────────────────

-- The current document for a key, from the projection's own storage. NULL when absent.
CREATE FUNCTION isomer.get(p_projection text, p_key text)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    p    isomer.projections;
    v_id jsonb;
    v    jsonb;
BEGIN
    SELECT * INTO p FROM isomer.projections WHERE name = p_projection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', p_projection USING ERRCODE = 'IS004';
    END IF;
    IF p.kind = 'cache' THEN
        RETURN isomer.cache_get(p.name, p_key);
    ELSIF p.kind <> 'document' THEN
        RAISE EXCEPTION 'isomer: % is a % projection; look-up by key is for document and cache projections', p.name, p.kind
            USING ERRCODE = 'IS005';
    END IF;
    EXECUTE format('SELECT to_jsonb($1::%s)', p.key_type) INTO v_id USING p_key;
    EXECUTE format(
        $q$SELECT isomer._ejson_relax(documentdb_core.bson_to_json_string(c.document)::text::jsonb)
           FROM %s c
           WHERE c.shard_key_value = %s
             AND c.object_id OPERATOR(documentdb_core.=) jsonb_build_object('', $1)::text::documentdb_core.bson$q$,
        isomer._collection_table(p.mongo_database, p.name),
        (SELECT collection_id FROM documentdb_api_catalog.collections
         WHERE database_name = p.mongo_database AND collection_name = p.name))
    INTO v USING v_id;
    RETURN v;
END $$;

-- ── access for the HTTP API ─────────────────────────────────────────────────

-- The same rule as query routing: a projection never widens access. A role may read it only if it
-- can SELECT every table behind it and none of those tables has row-level security.
CREATE FUNCTION isomer.can_read(p_role name, p_projection text)
RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT EXISTS (SELECT 1 FROM isomer.projections WHERE name = p_projection)
       AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = p_role)
       AND NOT EXISTS (
           SELECT 1 FROM isomer.projection_sources s
           JOIN pg_class c ON c.oid = s.source_table::regclass
           WHERE s.projection = p_projection
             AND (NOT has_table_privilege(p_role, c.oid, 'SELECT') OR c.relrowsecurity))
$$;

-- Issue an API key that acts as a Postgres role. The key is shown once; only its hash is stored.
CREATE FUNCTION isomer.create_api_key(name text, role name)
RETURNS text LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    v_secret text := 'isk_' || replace(gen_random_uuid()::text || gen_random_uuid()::text, '-', '');
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolname = create_api_key.role) THEN
        RAISE EXCEPTION 'isomer: no role named %', create_api_key.role;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolname = create_api_key.role AND r.rolsuper) THEN
        RAISE EXCEPTION 'isomer: an API key cannot act as a superuser (%)', create_api_key.role;
    END IF;
    INSERT INTO isomer.api_keys (name, key_sha256, role)
    VALUES (create_api_key.name, sha256(convert_to(v_secret, 'UTF8')), create_api_key.role);
    RETURN v_secret;
END $$;

CREATE FUNCTION isomer.revoke_api_key(name text)
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    UPDATE isomer.api_keys k SET revoked_at = now() WHERE k.name = revoke_api_key.name AND k.revoked_at IS NULL
$$;

-- The role an API key acts as; NULL for an unknown or revoked key.
CREATE FUNCTION isomer.api_key_role(p_secret text)
RETURNS name LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT role FROM isomer.api_keys
    WHERE key_sha256 = sha256(convert_to(p_secret, 'UTF8')) AND revoked_at IS NULL
$$;

-- Projections a role may read, with what the API can serve from each.
CREATE FUNCTION isomer.api_projections(p_role name)
RETURNS TABLE (name text, kind text, freshness text, history interval)
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT p.name, p.kind, p.freshness, p.history FROM isomer.projections p
    WHERE isomer.can_read(p_role, p.name) ORDER BY p.name
$$;

REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA isomer FROM PUBLIC;
GRANT EXECUTE ON FUNCTION isomer.wait_for(bigint, text, interval), isomer.freshness(text),
                          isomer.projection_for_collection(text, text), isomer.current_token(),
                          isomer.last_write_token(), isomer.token_to_mongo_timestamp(bigint),
                          isomer.cache_get(text, text), isomer.embed(text, text, interval),
                          isomer.embed_for(text, text), isomer._routes(), isomer.router_stats()
    TO PUBLIC;

SELECT cron.schedule('isomer-prune-history', '* * * * *', 'SELECT isomer.prune_history()')
    WHERE pg_catalog.citus_is_coordinator();
