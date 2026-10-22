-- IsomerDB 0.15.0: alerts reach on-call (PRD FR-K3).
--
--   * isomer.alert_channels: where alerts go. Kinds: webhook (JSON), slack (incoming webhook),
--     pagerduty (Events API v2, with trigger and resolve on one dedup key). Each channel has a
--     minimum severity. Secrets (a PagerDuty routing key, a webhook bearer token) are named by an
--     environment variable on the HTTP API, never stored here.
--   * isomer.alert_state tracks every alert (alert kind + subject) from firing to resolved.
--     isomer.pending_notifications() refreshes it from isomer.alerts and returns what has not been
--     delivered; the HTTP API delivers it and calls isomer.mark_notified once every channel took it.
--   * Severities: drift and broken attachments are critical (drift is a Sev-1 by the PRD), lag
--     budget breaches and dead letters are errors, staleness and paused budgets are warnings.
--     "unused" projections are not paged.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.15.0'" to load this file. \quit

CREATE TABLE isomer.alert_channels (
    name         text PRIMARY KEY CHECK (name ~ '^[a-z0-9][a-z0-9_.-]{0,62}$'),
    kind         text NOT NULL CHECK (kind IN ('webhook', 'slack', 'pagerduty')),
    url          text NOT NULL,
    secret_env   text,        -- environment variable on the HTTP API holding a token or routing key
    min_severity text NOT NULL DEFAULT 'warning' CHECK (min_severity IN ('info', 'warning', 'error', 'critical')),
    enabled      boolean NOT NULL DEFAULT true,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CHECK (kind <> 'pagerduty' OR secret_env IS NOT NULL)
);
SELECT pg_catalog.pg_extension_config_dump('isomer.alert_channels', '');

CREATE TABLE isomer.alert_state (
    alert             text NOT NULL,
    subject           text NOT NULL,
    severity          text NOT NULL,
    detail            text NOT NULL,
    first_seen        timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen         timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at       timestamptz,
    notified_firing   boolean NOT NULL DEFAULT false,
    notified_resolved boolean NOT NULL DEFAULT false,
    PRIMARY KEY (alert, subject)
);

-- Every delivery attempt, for audit and for seeing why a page did not arrive.
CREATE TABLE isomer.alert_deliveries (
    id        bigserial PRIMARY KEY,
    at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    channel   text NOT NULL,
    alert     text NOT NULL,
    subject   text NOT NULL,
    state     text NOT NULL CHECK (state IN ('firing', 'resolved')),
    ok        boolean NOT NULL,
    error     text
);

CREATE FUNCTION isomer._alert_severity(p_alert text)
RETURNS text LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $$
    SELECT CASE p_alert
        WHEN 'drift' THEN 'critical'
        WHEN 'attachment' THEN 'critical'
        WHEN 'error' THEN 'error'
        WHEN 'lag_budget' THEN 'error'
        WHEN 'embed_lag' THEN 'error'
        WHEN 'dead_letters' THEN 'error'
        WHEN 'search_stale' THEN 'warning'
        WHEN 'budget_paused' THEN 'warning'
        WHEN 'warning' THEN 'warning'
        ELSE 'info' END
$$;

CREATE FUNCTION isomer._severity_rank(p text)
RETURNS integer LANGUAGE sql IMMUTABLE AS $$
    SELECT array_position(ARRAY['info', 'warning', 'error', 'critical'], p)
$$;

-- Configure where alerts go.
CREATE FUNCTION isomer.add_alert_channel(name text, kind text, url text, secret_env text DEFAULT NULL,
                                         min_severity text DEFAULT 'warning')
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    INSERT INTO isomer.alert_channels (name, kind, url, secret_env, min_severity)
    VALUES (add_alert_channel.name, add_alert_channel.kind, add_alert_channel.url,
            add_alert_channel.secret_env, add_alert_channel.min_severity)
    ON CONFLICT ON CONSTRAINT alert_channels_pkey DO UPDATE
        SET kind = EXCLUDED.kind, url = EXCLUDED.url, secret_env = EXCLUDED.secret_env,
            min_severity = EXCLUDED.min_severity, enabled = true
$$;

CREATE FUNCTION isomer.drop_alert_channel(name text)
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    DELETE FROM isomer.alert_channels c WHERE c.name = drop_alert_channel.name
$$;

-- Bring alert_state in line with isomer.alerts, then return every transition not yet delivered.
-- Event-derived alerts (the level of a logged event) are keyed by subject, so a burst of the same
-- warning about one projection is one alert, not a page per line.
CREATE FUNCTION isomer.pending_notifications()
RETURNS TABLE (alert text, subject text, severity text, detail text, state text, first_seen timestamptz,
               resolved_at timestamptz)
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
#variable_conflict use_column
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('isomer'), hashtext('alerts'));
    CREATE TEMP TABLE IF NOT EXISTS isomer_current_alerts (alert text, subject text, detail text) ON COMMIT DROP;
    TRUNCATE isomer_current_alerts;
    INSERT INTO isomer_current_alerts
    SELECT DISTINCT ON (a.alert, a.subject) a.alert, a.subject, a.detail
    FROM isomer.alerts a WHERE a.alert <> 'unused'
    ORDER BY a.alert, a.subject, a.detail;

    -- New, or firing again after resolving.
    INSERT INTO isomer.alert_state AS s (alert, subject, severity, detail)
    SELECT c.alert, c.subject, isomer._alert_severity(c.alert), c.detail FROM isomer_current_alerts c
    ON CONFLICT (alert, subject) DO UPDATE
        SET detail = EXCLUDED.detail, last_seen = clock_timestamp(),
            first_seen = CASE WHEN s.resolved_at IS NOT NULL THEN clock_timestamp() ELSE s.first_seen END,
            notified_firing = CASE WHEN s.resolved_at IS NOT NULL THEN false ELSE s.notified_firing END,
            notified_resolved = CASE WHEN s.resolved_at IS NOT NULL THEN false ELSE s.notified_resolved END,
            resolved_at = NULL;
    -- Gone: resolved.
    UPDATE isomer.alert_state s SET resolved_at = clock_timestamp()
    WHERE s.resolved_at IS NULL
      AND NOT EXISTS (SELECT 1 FROM isomer_current_alerts c WHERE c.alert = s.alert AND c.subject = s.subject);

    RETURN QUERY
    SELECT s.alert, s.subject, s.severity, s.detail,
           CASE WHEN s.resolved_at IS NULL THEN 'firing' ELSE 'resolved' END, s.first_seen, s.resolved_at
    FROM isomer.alert_state s
    WHERE (s.resolved_at IS NULL AND NOT s.notified_firing)
       OR (s.resolved_at IS NOT NULL AND s.notified_firing AND NOT s.notified_resolved)
    ORDER BY s.first_seen;
END $$;

-- The channels an alert of this severity goes to.
CREATE FUNCTION isomer.alert_channels_for(p_severity text)
RETURNS SETOF isomer.alert_channels LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT * FROM isomer.alert_channels
    WHERE enabled AND isomer._severity_rank(p_severity) >= isomer._severity_rank(min_severity)
    ORDER BY name
$$;

-- The channels that still have to take this transition: those for its severity that have not
-- accepted it since the alert (last) started firing. A retry goes only to the ones that failed.
CREATE FUNCTION isomer.channels_to_notify(p_alert text, p_subject text, p_state text)
RETURNS SETOF isomer.alert_channels LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT c.* FROM isomer.alert_state s, isomer.alert_channels_for(s.severity) c
    WHERE s.alert = p_alert AND s.subject = p_subject
      AND NOT EXISTS (SELECT 1 FROM isomer.alert_deliveries d
                      WHERE d.channel = c.name AND d.alert = s.alert AND d.subject = s.subject
                        AND d.state = p_state AND d.ok AND d.at >= s.first_seen)
    ORDER BY c.name
$$;

CREATE FUNCTION isomer.record_delivery(p_channel text, p_alert text, p_subject text, p_state text,
                                       p_ok boolean, p_error text DEFAULT NULL)
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    INSERT INTO isomer.alert_deliveries (channel, alert, subject, state, ok, error)
    VALUES (p_channel, p_alert, p_subject, p_state, p_ok, p_error)
$$;

-- Called once every channel accepted the transition.
CREATE FUNCTION isomer.mark_notified(p_alert text, p_subject text, p_state text)
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    UPDATE isomer.alert_state SET notified_firing = notified_firing OR p_state = 'firing',
                                  notified_resolved = notified_resolved OR p_state = 'resolved'
    WHERE alert = p_alert AND subject = p_subject
$$;

-- Keep a month of delivery history; drop resolved alerts a week after they were delivered.
CREATE FUNCTION isomer.prune_alerts()
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    DELETE FROM isomer.alert_deliveries WHERE at < now() - interval '30 days';
    DELETE FROM isomer.alert_state WHERE notified_resolved AND resolved_at < now() - interval '7 days';
$$;

REVOKE EXECUTE ON FUNCTION isomer.add_alert_channel(text, text, text, text, text), isomer.drop_alert_channel(text),
                           isomer.pending_notifications(), isomer.alert_channels_for(text), isomer.channels_to_notify(text, text, text),
                           isomer.record_delivery(text, text, text, text, boolean, text),
                           isomer.mark_notified(text, text, text), isomer.prune_alerts() FROM PUBLIC;

SELECT cron.schedule('isomer-prune-alerts', '17 3 * * *', 'SELECT isomer.prune_alerts()')
WHERE pg_catalog.citus_is_coordinator();
