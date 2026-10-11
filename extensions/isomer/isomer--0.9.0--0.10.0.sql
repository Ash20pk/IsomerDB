-- IsomerDB 0.10.0: operator access for the console and CLI (PRD FR-O3).
--
--   * isomer_admin: a role whose members may use the admin API (/v1/admin/*): projections, lag,
--     drift, alerts, proposals, the shard advisor, routes and attachments. API keys are issued as
--     before, for a member role: isomer.create_api_key('ops', 'ops_role'). (Keys never act as a
--     superuser, so membership is the way in.)

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.10.0'" to load this file. \quit

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'isomer_admin') THEN
        CREATE ROLE isomer_admin NOLOGIN;
    END IF;
END $$;

-- Whether a role may use the admin API.
CREATE FUNCTION isomer.is_admin(p_role name)
RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = p_role AND rolsuper)
        OR (EXISTS (SELECT 1 FROM pg_roles WHERE rolname = p_role)
            AND pg_has_role(p_role, 'isomer_admin', 'MEMBER'))
$$;

-- Milliseconds in an interval, for charts (NULL stays NULL).
CREATE FUNCTION isomer._ms(p interval)
RETURNS double precision LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $$
    SELECT round((extract(epoch FROM p) * 1000)::numeric, 1)::double precision
$$;

-- Every projection with its lag against budget, backlog, drift, cost and history setting.
CREATE FUNCTION isomer.admin_projections()
RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT coalesce(jsonb_agg(to_jsonb(s) || jsonb_build_object(
               'lag_budget_ms', isomer._ms(s.lag_budget),
               'oldest_pending_ms', isomer._ms(s.oldest_pending),
               'search_staleness_ms', isomer._ms(s.search_staleness),
               'oldest_embed_job_ms', isomer._ms(s.oldest_embed_job),
               'drift', d.drift, 'checks', d.checks, 'drift_since', d.since, 'drift_summary', d.summary,
               'history', p.history::text, 'native_view', p.native_view,
               'target_schema', p.target_schema, 'mongo_database', p.mongo_database)
           ORDER BY s.name), '[]')
    FROM isomer.projection_status s
    JOIN isomer.projections p ON p.name = s.name
    JOIN isomer.drift_report d ON d.projection = s.name
$$;

-- One call for the console's front page.
CREATE FUNCTION isomer.admin_overview()
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    v_alerts jsonb;
BEGIN
    BEGIN
        SELECT coalesce(jsonb_agg(to_jsonb(a)), '[]') INTO v_alerts FROM isomer.alerts a;
    EXCEPTION WHEN OTHERS THEN
        v_alerts := jsonb_build_array(jsonb_build_object('alert', 'error', 'subject', 'isomer.alerts', 'detail', SQLERRM));
    END;
    RETURN jsonb_build_object(
        'version', (SELECT extversion FROM pg_extension WHERE extname = 'isomer'),
        'token', isomer.current_token(),
        'at', clock_timestamp(),
        'projections', isomer.admin_projections(),
        'alerts', v_alerts,
        'attachments', (SELECT coalesce(jsonb_agg(jsonb_build_object('name', name, 'status', status) ORDER BY name), '[]')
                        FROM isomer.attachments),
        'nodes', (SELECT coalesce(jsonb_agg(jsonb_build_object(
                         'host', n.nodename, 'port', n.nodeport, 'role', n.noderole, 'active', n.isactive,
                         'has_shards', n.shouldhaveshards,
                         'placements', (SELECT count(*) FROM pg_dist_placement pl WHERE pl.groupid = n.groupid))
                      ORDER BY n.groupid, n.nodename), '[]')
                  FROM pg_dist_node n),
        'proposals_open', (SELECT count(*) FROM isomer.proposals WHERE status IN ('proposed', 'needs_review')),
        'routes_enabled', (SELECT count(*) FROM isomer.routes WHERE enabled));
END $$;

-- One projection in full: definition, sources, certificate, routes and recent events.
CREATE FUNCTION isomer.admin_projection(p_projection text)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    p      isomer.projections;
    v_cert jsonb;
BEGIN
    SELECT * INTO p FROM isomer.projections WHERE name = p_projection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', p_projection USING ERRCODE = 'IS004';
    END IF;
    BEGIN
        v_cert := isomer.certificate(p.name);
    EXCEPTION WHEN OTHERS THEN
        v_cert := jsonb_build_object('error', SQLERRM);
    END;
    RETURN jsonb_build_object(
        'definition', to_jsonb(p) || jsonb_build_object('lag_budget', p.lag_budget::text, 'history', p.history::text),
        'status', (SELECT e FROM jsonb_array_elements(isomer.admin_projections()) e WHERE e ->> 'name' = p.name),
        'freshness_token', isomer.freshness(p.name),
        'sources', (SELECT coalesce(jsonb_agg(jsonb_build_object('table', source_table, 'key_column', key_column)
                                              ORDER BY source_table), '[]')
                    FROM isomer.projection_sources WHERE projection = p.name),
        'certificate', v_cert,
        'routes', (SELECT coalesce(jsonb_agg(jsonb_build_object(
                          'queryid', queryid, 'source_query', source_query, 'measured_speedup', measured_speedup,
                          'verified_keys', verified_keys, 'enabled', enabled, 'created_at', created_at)), '[]')
                   FROM isomer.routes WHERE projection = p.name),
        'events', (SELECT coalesce(jsonb_agg(to_jsonb(e) ORDER BY e.id DESC), '[]')
                   FROM (SELECT * FROM isomer.events WHERE subject = p.name ORDER BY id DESC LIMIT 50) e));
END $$;

REVOKE EXECUTE ON FUNCTION isomer.is_admin(name), isomer.admin_projections(), isomer.admin_overview(),
                           isomer.admin_projection(text) FROM PUBLIC;
