//! Delivers alerts to on-call (PRD FR-K3): every ISOMER_ALERT_INTERVAL_SECS (default 15), take the
//! alert transitions not yet delivered (isomer.pending_notifications), send each to every channel
//! whose minimum severity it meets (isomer.alert_channels), record every attempt, and mark the
//! transition delivered once all of them accepted it. A failed send is retried on the next pass,
//! to the channels that have not accepted it yet.
//!
//!   webhook    POST {cluster, alert, subject, severity, state, detail, first_seen, resolved_at}
//!              (Authorization: Bearer $secret_env, when set)
//!   slack      POST {text} to an incoming-webhook URL
//!   pagerduty  Events API v2: trigger on firing, resolve on resolved, one dedup key per alert

use std::time::Duration;

use serde_json::{json, Value};
use tracing::{info, warn};

use crate::Shared;

struct Pending {
    alert: String,
    subject: String,
    severity: String,
    detail: String,
    state: String,
    first_seen: String,
    resolved_at: Option<String>,
}

struct Channel {
    name: String,
    kind: String,
    url: String,
    secret_env: Option<String>,
}

pub async fn deliver_loop(app: Shared) {
    let every = std::env::var("ISOMER_ALERT_INTERVAL_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(15u64);
    let cluster = std::env::var("ISOMER_CLUSTER_NAME").unwrap_or_else(|_| "isomerdb".into());
    let http = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("http client");
    loop {
        tokio::time::sleep(Duration::from_secs(every)).await;
        if let Err(e) = deliver_once(&app, &http, &cluster).await {
            warn!("alert delivery pass failed: {e:#}");
        }
    }
}

async fn deliver_once(app: &Shared, http: &reqwest::Client, cluster: &str) -> anyhow::Result<()> {
    let db = app.pool.get().await?;
    let rows = db
        .query(
            "SELECT alert, subject, severity, detail, state,
                    to_char(first_seen AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                    to_char(resolved_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
             FROM isomer.pending_notifications()",
            &[],
        )
        .await?;
    for r in rows {
        let p = Pending {
            alert: r.get(0),
            subject: r.get(1),
            severity: r.get(2),
            detail: r.get(3),
            state: r.get(4),
            first_seen: r.get(5),
            resolved_at: r.get(6),
        };
        let configured: i64 = db
            .query_one("SELECT count(*) FROM isomer.alert_channels_for($1)", &[&p.severity])
            .await?
            .get(0);
        if configured == 0 {
            continue; // nowhere to send it yet: it stays pending until a channel exists
        }
        // Only the channels that have not accepted this transition yet (a retry skips the rest).
        let channels: Vec<Channel> = db
            .query(
                "SELECT name, kind, url, secret_env FROM isomer.channels_to_notify($1, $2, $3)",
                &[&p.alert, &p.subject, &p.state],
            )
            .await?
            .iter()
            .map(|c| Channel { name: c.get(0), kind: c.get(1), url: c.get(2), secret_env: c.get(3) })
            .collect();
        let mut all_ok = true;
        for ch in &channels {
            let result = send(http, ch, &p, cluster).await;
            let (ok, error) = match &result {
                Ok(()) => (true, None),
                Err(e) => (false, Some(format!("{e:#}"))),
            };
            all_ok &= ok;
            db.execute(
                "SELECT isomer.record_delivery($1, $2, $3, $4, $5, $6)",
                &[&ch.name, &p.alert, &p.subject, &p.state, &ok, &error],
            )
            .await?;
            match result {
                Ok(()) => info!(channel = %ch.name, alert = %p.alert, subject = %p.subject, state = %p.state, "alert delivered"),
                Err(e) => warn!(channel = %ch.name, alert = %p.alert, subject = %p.subject, "alert delivery failed: {e:#}"),
            }
        }
        if all_ok {
            db.execute("SELECT isomer.mark_notified($1, $2, $3)", &[&p.alert, &p.subject, &p.state]).await?;
        }
    }
    Ok(())
}

fn secret(ch: &Channel) -> anyhow::Result<Option<String>> {
    match &ch.secret_env {
        None => Ok(None),
        Some(var) => std::env::var(var)
            .map(Some)
            .map_err(|_| anyhow::anyhow!("environment variable {var} (channel {}) is not set on the API", ch.name)),
    }
}

fn payload(ch: &Channel, p: &Pending, cluster: &str, secret: Option<&str>) -> Value {
    let summary = format!("[{}] {} on {}: {}", p.severity, p.alert.replace('_', " "), p.subject, p.detail);
    match ch.kind.as_str() {
        "slack" => {
            let lead = if p.state == "resolved" { "Resolved" } else { "Firing" };
            json!({ "text": format!("{lead} on {cluster}: {summary}") })
        }
        "pagerduty" => json!({
            "routing_key": secret.unwrap_or_default(),
            "event_action": if p.state == "resolved" { "resolve" } else { "trigger" },
            "dedup_key": format!("isomer:{cluster}:{}:{}", p.alert, p.subject),
            "payload": {
                "summary": summary,
                "source": cluster,
                "severity": p.severity,
                "component": p.subject,
                "class": p.alert,
                "custom_details": { "detail": p.detail, "first_seen": p.first_seen },
            },
        }),
        _ => json!({
            "cluster": cluster,
            "alert": p.alert,
            "subject": p.subject,
            "severity": p.severity,
            "state": p.state,
            "detail": p.detail,
            "first_seen": p.first_seen,
            "resolved_at": p.resolved_at,
        }),
    }
}

async fn send(http: &reqwest::Client, ch: &Channel, p: &Pending, cluster: &str) -> anyhow::Result<()> {
    let secret = secret(ch)?;
    let mut req = http.post(&ch.url).json(&payload(ch, p, cluster, secret.as_deref()));
    if ch.kind == "webhook" {
        if let Some(token) = &secret {
            req = req.bearer_auth(token);
        }
    }
    let resp = req.send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body: String = resp.text().await.unwrap_or_default().chars().take(200).collect();
        anyhow::bail!("{status}: {body}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending(state: &str) -> Pending {
        Pending {
            alert: "drift".into(),
            subject: "customer_card".into(),
            severity: "critical".into(),
            detail: "drift 3 rows".into(),
            state: state.into(),
            first_seen: "2026-10-12T10:00:00Z".into(),
            resolved_at: None,
        }
    }

    fn channel(kind: &str) -> Channel {
        Channel { name: "c".into(), kind: kind.into(), url: "http://x".into(), secret_env: None }
    }

    #[test]
    fn pagerduty_triggers_and_resolves_on_one_dedup_key() {
        let fire = payload(&channel("pagerduty"), &pending("firing"), "prod", Some("rk"));
        let clear = payload(&channel("pagerduty"), &pending("resolved"), "prod", Some("rk"));
        assert_eq!(fire["event_action"], "trigger");
        assert_eq!(clear["event_action"], "resolve");
        assert_eq!(fire["dedup_key"], clear["dedup_key"]);
        assert_eq!(fire["dedup_key"], "isomer:prod:drift:customer_card");
        assert_eq!(fire["payload"]["severity"], "critical");
        assert_eq!(fire["routing_key"], "rk");
    }

    #[test]
    fn slack_and_webhook_payloads() {
        let s = payload(&channel("slack"), &pending("firing"), "prod", None);
        assert_eq!(s["text"], "Firing on prod: [critical] drift on customer_card: drift 3 rows");
        let w = payload(&channel("webhook"), &pending("resolved"), "prod", None);
        assert_eq!(w["state"], "resolved");
        assert_eq!(w["cluster"], "prod");
    }
}
