-- IsomerDB 0.28.0: the auditor counts a drifted key once.
--
--   * A key confirmed as drift was taken off the suspects, so while it stayed wrong the next audits
--     suspected it again and, once the confirmation window had passed, counted it again: drift
--     read 2, 3, ... for one bad key, more often the slower the audits ran. A confirmed suspect
--     now stays, marked confirmed, and isn't counted again while the key differs the same way.
--     Once the key matches the tables again it is taken off, so a later drift counts afresh.
--   * Audits run concurrently (the scheduled one and any called by hand): a suspect is locked
--     while it is checked, so two audits can't both confirm it.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.28.0'" to load this file. \quit

ALTER TABLE isomer.audit_suspects ADD COLUMN confirmed boolean NOT NULL DEFAULT false;

-- The audit itself (see 0.20.0; isomer.audit, since 0.22.0, takes its lock and calls this).
CREATE OR REPLACE FUNCTION isomer._audit_projections(p_sample integer DEFAULT 50)
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
    FOR p IN SELECT * FROM isomer.projections WHERE status = 'active' AND kind NOT IN ('event', 'analytics') LOOP
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
                    -- Right again (or never wrong): a later drift of this key counts afresh.
                    DELETE FROM isomer.audit_suspects WHERE projection = p.name AND key = r.key;
                    CONTINUE;
                END IF;
                v_hash := md5(coalesce(r.expected::text, '<absent>'));
                -- Locked while it is checked: another audit running now waits, then sees it confirmed.
                SELECT * INTO v_s FROM isomer.audit_suspects WHERE projection = p.name AND key = r.key FOR UPDATE;
                IF NOT FOUND OR v_s.expected_hash IS DISTINCT FROM v_hash THEN
                    INSERT INTO isomer.audit_suspects (projection, key, expected_hash) VALUES (p.name, r.key, v_hash)
                    ON CONFLICT (projection, key) DO UPDATE
                        SET expected_hash = EXCLUDED.expected_hash, first_seen = clock_timestamp(), confirmed = false;
                ELSIF v_s.confirmed THEN
                    -- Counted when it was confirmed, and still wrong the same way.
                    NULL;
                ELSIF clock_timestamp() - v_s.first_seen >= v_min THEN
                    INSERT INTO isomer.drift_samples (projection, key, expected, actual) VALUES (p.name, r.key, r.expected, r.actual);
                    UPDATE isomer.audit_suspects SET confirmed = true WHERE projection = p.name AND key = r.key;
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

REVOKE EXECUTE ON FUNCTION isomer._audit_projections(integer) FROM PUBLIC;
