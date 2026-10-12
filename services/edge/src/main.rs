//! IsomerDB edge replica: projections served close to users, fed by the origin cluster's change
//! feed (the README's edge replicas).
//!
//! For each projection it follows `/v1/projections/{name}/live` on the origin's HTTP API: the
//! snapshot, then every change in commit order, resuming with `Last-Event-ID` after a disconnect
//! and taking a new snapshot if the origin has pruned past its cursor. Reads are answered from
//! memory:
//!
//!   GET /healthz
//!   GET /v1/projections/{name}/docs/{key}   the document as of the replica's cursor
//!   GET /v1/edge/status                     each projection: documents, cursor, connected, lag
//!
//! Access is the origin's: a client's API key is checked against the origin (the projections its
//! role may read), and the answer is cached for ISOMER_EDGE_AUTH_TTL_SECS, so the edge never
//! widens what a key can read. Read your writes works as at the origin: with `X-Isomer-After:
//! <token>` the edge asks the origin for the feed cursor that reflects the write
//! (`/v1/projections/{name}/cursor`), and answers once it has applied the feed that far, or 503
//! with `Retry-After`.
//!
//! The projections need history on (`isomer.set_history`): the feed is what the edge follows.
//!
//! Configuration (environment):
//!   ISOMER_ORIGIN               the origin's HTTP API, e.g. http://api:8080
//!   ISOMER_ORIGIN_KEY           an API key whose role may read the projections replicated
//!   ISOMER_EDGE_PROJECTIONS     comma-separated projection names
//!   ISOMER_EDGE_LISTEN          address to listen on (default 0.0.0.0:8081)
//!   ISOMER_EDGE_WAIT_MS         longest wait for X-Isomer-After (default 5000)
//!   ISOMER_EDGE_AUTH_TTL_SECS   how long a key's access is cached (default 30)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::{watch, RwLock};
use tracing::{info, warn};

/// One replicated projection.
struct Replica {
    docs: RwLock<HashMap<String, Value>>,
    /// The feed cursor applied so far; readers wait on it.
    cursor: watch::Sender<i64>,
    connected: std::sync::atomic::AtomicBool,
    last_event: std::sync::Mutex<Option<Instant>>,
}

struct Edge {
    origin: String,
    origin_key: String,
    http: reqwest::Client,
    replicas: HashMap<String, Arc<Replica>>,
    wait: Duration,
    auth_ttl: Duration,
    /// API key -> (projections it may read, when that was learned).
    access: RwLock<HashMap<String, (Vec<String>, Instant)>>,
}

type Shared = Arc<Edge>;

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();
    let origin = std::env::var("ISOMER_ORIGIN").context("ISOMER_ORIGIN")?.trim_end_matches('/').to_string();
    let origin_key = std::env::var("ISOMER_ORIGIN_KEY").context("ISOMER_ORIGIN_KEY")?;
    let names: Vec<String> = std::env::var("ISOMER_EDGE_PROJECTIONS")
        .context("ISOMER_EDGE_PROJECTIONS")?
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let replicas = names
        .iter()
        .map(|n| {
            (n.clone(), Arc::new(Replica {
                docs: RwLock::new(HashMap::new()),
                cursor: watch::channel(0).0,
                connected: false.into(),
                last_event: std::sync::Mutex::new(None),
            }))
        })
        .collect();
    let edge = Arc::new(Edge {
        origin,
        origin_key,
        // No overall timeout: the live stream stays open.
        http: reqwest::Client::builder().connect_timeout(Duration::from_secs(5)).build()?,
        replicas,
        wait: Duration::from_millis(env_or("ISOMER_EDGE_WAIT_MS", 5000)),
        auth_ttl: Duration::from_secs(env_or("ISOMER_EDGE_AUTH_TTL_SECS", 30)),
        access: RwLock::new(HashMap::new()),
    });
    for name in &names {
        tokio::spawn(follow(edge.clone(), name.clone()));
    }
    let router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/edge/status", get(status))
        .route("/v1/projections/:name/docs/:key", get(get_doc))
        .with_state(edge);
    let listen = std::env::var("ISOMER_EDGE_LISTEN").unwrap_or_else(|_| "0.0.0.0:8081".into());
    let socket = tokio::net::TcpListener::bind(&listen).await?;
    info!(%listen, projections = ?names, "isomer edge listening");
    axum::serve(socket, router)
        .with_graceful_shutdown(async { tokio::signal::ctrl_c().await.ok(); })
        .await?;
    Ok(())
}

// ── following the feed ──────────────────────────────────────────────────────

/// Keep one projection's replica current, forever.
async fn follow(edge: Shared, name: String) {
    let replica = edge.replicas[&name].clone();
    let mut delay = Duration::from_millis(250);
    loop {
        match stream_once(&edge, &name, &replica).await {
            Ok(()) => warn!(%name, "live stream ended; reconnecting"),
            Err(e) => warn!(%name, "live stream: {e:#}; reconnecting"),
        }
        replica.connected.store(false, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(5));
    }
}

/// One connection to the origin's live stream, applied as it arrives.
async fn stream_once(edge: &Edge, name: &str, replica: &Replica) -> Result<()> {
    let cursor = *replica.cursor.borrow();
    let mut req = edge
        .http
        .get(format!("{}/v1/projections/{name}/live", edge.origin))
        .bearer_auth(&edge.origin_key)
        .header("accept", "text/event-stream");
    if cursor > 0 {
        req = req.header("last-event-id", cursor.to_string());
    }
    let resp = req.send().await?;
    if resp.status() == StatusCode::GONE {
        // The origin pruned its history past our cursor: start again from a snapshot.
        warn!(%name, cursor, "cursor is older than the origin's history; taking a new snapshot");
        replica.cursor.send_replace(0);
        return Ok(());
    }
    if !resp.status().is_success() {
        anyhow::bail!("{}: {}", resp.status(), resp.text().await.unwrap_or_default());
    }
    replica.connected.store(true, std::sync::atomic::Ordering::Relaxed);
    info!(%name, cursor, "following the origin's feed");
    let mut body = resp.bytes_stream();
    let mut buf = String::new();
    while let Some(chunk) = body.next().await {
        buf.push_str(&String::from_utf8_lossy(&chunk?));
        // Server-sent events are separated by a blank line.
        while let Some(end) = buf.find("\n\n") {
            let raw: String = buf.drain(..end + 2).collect();
            let (mut event, mut data) = ("message".to_string(), String::new());
            for line in raw.lines() {
                if let Some(v) = line.strip_prefix("event:") {
                    event = v.trim().to_string();
                } else if let Some(v) = line.strip_prefix("data:") {
                    data.push_str(v.trim_start());
                }
            }
            if data.is_empty() {
                continue; // keep-alive comment
            }
            apply(name, replica, &event, &data).await?;
        }
    }
    Ok(())
}

async fn apply(name: &str, replica: &Replica, event: &str, data: &str) -> Result<()> {
    let v: Value = serde_json::from_str(data).context("event data")?;
    match event {
        "snapshot" => {
            let docs: HashMap<String, Value> = serde_json::from_value(v["docs"].clone()).unwrap_or_default();
            let cursor = v["cursor"].as_i64().unwrap_or(0);
            let n = docs.len();
            *replica.docs.write().await = docs;
            replica.cursor.send_replace(cursor);
            info!(%name, documents = n, cursor, "snapshot applied");
        }
        "change" => {
            let key = v["key"].as_str().unwrap_or_default().to_string();
            {
                let mut docs = replica.docs.write().await;
                if v["op"] == "delete" {
                    docs.remove(&key);
                } else {
                    docs.insert(key, v["doc"].clone());
                }
            }
            if let Some(id) = v["id"].as_i64() {
                replica.cursor.send_replace(id);
            }
        }
        "error" => anyhow::bail!("origin: {data}"),
        _ => {}
    }
    *replica.last_event.lock().unwrap() = Some(Instant::now());
    Ok(())
}

// ── serving ─────────────────────────────────────────────────────────────────

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    let mut r = (status, Json(json!({ "error": message.into() }))).into_response();
    if status == StatusCode::SERVICE_UNAVAILABLE {
        r.headers_mut().insert("retry-after", HeaderValue::from_static("1"));
    }
    r
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers.get("authorization")?.to_str().ok()?.strip_prefix("Bearer ")
}

/// May this key read this projection? Asked of the origin, and remembered for a while.
async fn may_read(edge: &Edge, key: &str, projection: &str) -> std::result::Result<bool, Response> {
    if let Some((names, at)) = edge.access.read().await.get(key) {
        if at.elapsed() < edge.auth_ttl {
            return Ok(names.iter().any(|n| n == projection));
        }
    }
    let resp = edge
        .http
        .get(format!("{}/v1/projections", edge.origin))
        .bearer_auth(key)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, format!("origin unreachable: {e}")))?;
    if resp.status() == StatusCode::UNAUTHORIZED {
        return Err(error(StatusCode::UNAUTHORIZED, "unknown or revoked API key"));
    }
    if !resp.status().is_success() {
        return Err(error(StatusCode::SERVICE_UNAVAILABLE, format!("origin: {}", resp.status())));
    }
    let body: Value = resp.json().await.map_err(|e| error(StatusCode::BAD_GATEWAY, e.to_string()))?;
    let names: Vec<String> = body["projections"]
        .as_array()
        .map(|a| a.iter().filter_map(|p| p["name"].as_str().map(str::to_owned)).collect())
        .unwrap_or_default();
    let ok = names.iter().any(|n| n == projection);
    edge.access.write().await.insert(key.to_string(), (names, Instant::now()));
    Ok(ok)
}

/// With X-Isomer-After: the origin's feed cursor that reflects the write.
async fn cursor_after(edge: &Edge, key: &str, projection: &str, token: &str) -> std::result::Result<i64, Response> {
    let resp = edge
        .http
        .get(format!("{}/v1/projections/{projection}/cursor", edge.origin))
        .bearer_auth(key)
        .header("x-isomer-after", token)
        .timeout(edge.wait + Duration::from_secs(2))
        .send()
        .await
        .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, format!("origin unreachable: {e}")))?;
    match resp.status() {
        s if s.is_success() => {}
        StatusCode::SERVICE_UNAVAILABLE => return Err(error(StatusCode::SERVICE_UNAVAILABLE, "not yet")),
        StatusCode::BAD_REQUEST => return Err(error(StatusCode::BAD_REQUEST, "X-Isomer-After must be a write token (an integer)")),
        s => return Err(error(StatusCode::SERVICE_UNAVAILABLE, format!("origin: {s}"))),
    }
    let body: Value = resp.json().await.map_err(|e| error(StatusCode::BAD_GATEWAY, e.to_string()))?;
    Ok(body["cursor"].as_i64().unwrap_or(0))
}

async fn get_doc(State(edge): State<Shared>, headers: HeaderMap, Path((name, key)): Path<(String, String)>) -> Response {
    let Some(api_key) = bearer(&headers) else {
        return error(StatusCode::UNAUTHORIZED, "missing Authorization: Bearer <api key>");
    };
    let Some(replica) = edge.replicas.get(&name) else {
        return error(StatusCode::NOT_FOUND, format!("this edge does not serve {name}"));
    };
    match may_read(&edge, api_key, &name).await {
        Ok(true) => {}
        Ok(false) => return error(StatusCode::NOT_FOUND, format!("no projection named {name}")),
        Err(r) => return r,
    }
    if let Some(token) = headers.get("x-isomer-after").and_then(|v| v.to_str().ok()) {
        let target = match cursor_after(&edge, api_key, &name, token).await {
            Ok(c) => c,
            Err(r) => return r,
        };
        let mut rx = replica.cursor.subscribe();
        let caught_up = tokio::time::timeout(edge.wait, rx.wait_for(|c| *c >= target)).await;
        if !matches!(caught_up, Ok(Ok(_))) {
            return error(StatusCode::SERVICE_UNAVAILABLE, format!("this edge has not caught up to the write yet (cursor {target})"));
        }
    }
    let cursor = *replica.cursor.borrow();
    let doc = replica.docs.read().await.get(&key).cloned();
    match doc {
        Some(doc) => {
            let mut r = Json(doc).into_response();
            r.headers_mut().insert("x-isomer-edge-cursor", HeaderValue::from(cursor));
            r
        }
        None => error(StatusCode::NOT_FOUND, format!("{name} has no document for key {key}")),
    }
}

async fn status(State(edge): State<Shared>) -> Json<Value> {
    let mut out = serde_json::Map::new();
    for (name, r) in &edge.replicas {
        let age = r.last_event.lock().unwrap().map(|t| t.elapsed().as_millis() as u64);
        out.insert(name.clone(), json!({
            "documents": r.docs.read().await.len(),
            "cursor": *r.cursor.borrow(),
            "connected": r.connected.load(std::sync::atomic::Ordering::Relaxed),
            "last_event_ms_ago": age,
        }));
    }
    Json(Value::Object(out))
}
