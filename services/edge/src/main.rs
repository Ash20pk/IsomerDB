//! IsomerDB edge replica: projections served close to users, fed by the origin cluster's change
//! feed (the README's edge replicas).
//!
//! For each projection it follows `/v1/projections/{name}/live` on the origin's HTTP API: the
//! snapshot, then every change in commit order, resuming with `Last-Event-ID` after a disconnect
//! and taking a new snapshot if the origin has pruned past its cursor. Reads are answered from
//! memory, over HTTP and over the MongoDB wire protocol (`mongo`):
//!
//!   GET /healthz
//!   GET /v1/projections/{name}/docs/{key}   the document as of the replica's cursor
//!   GET /v1/edge/status                     each projection: documents, cursor, connected, lag
//!
//! With ISOMER_EDGE_DATA_DIR the copy is kept on disk too (`store`), so a restarted edge serves
//! at once from what it had and resumes the feed from its cursor instead of a new snapshot.
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
//!   ISOMER_EDGE_DATA_DIR        keep the copy on disk here (default: memory only)
//!   ISOMER_EDGE_MONGO_LISTEN    serve MongoDB on this address, e.g. 0.0.0.0:27017 (default: off)
//!   ISOMER_EDGE_TLS_CERT, ISOMER_EDGE_TLS_KEY   PEM files: MongoDB over TLS
//!   ISOMER_EDGE_MONGO_PLAINTEXT 1: allow MongoDB without TLS (API keys then cross in the clear)
//!   ISOMER_EDGE_WAIT_MS         longest wait for X-Isomer-After (default 5000)
//!   ISOMER_EDGE_AUTH_TTL_SECS   how long a key's access is cached (default 30)

mod mongo;
mod query;
mod store;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
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

use store::{Lineage, Store};

/// What the origin says about a replicated projection.
#[derive(Clone, Debug)]
struct Meta {
    /// The MongoDB database the origin keeps it in.
    database: Option<String>,
    /// When its history was turned on: a new value is a new feed.
    since: String,
}

/// One replicated projection.
pub struct Replica {
    docs: RwLock<HashMap<String, Value>>,
    /// The feed cursor applied so far; readers wait on it.
    cursor: watch::Sender<i64>,
    connected: AtomicBool,
    /// Serving a copy restored from disk, not yet reconnected to the origin.
    restored: AtomicBool,
    last_event: std::sync::Mutex<Option<Instant>>,
    meta: std::sync::RwLock<Option<Meta>>,
    store: Option<std::sync::Mutex<Store>>,
}

impl Replica {
    pub fn database(&self) -> Option<String> {
        self.meta.read().unwrap().as_ref().and_then(|m| m.database.clone())
    }
}

/// The projections an API key may read, as the origin answered.
pub struct Access {
    names: Vec<String>,
}

impl Access {
    pub fn may_read(&self, projection: &str) -> bool {
        self.names.iter().any(|n| n == projection)
    }
}

pub struct Edge {
    origin: String,
    origin_key: String,
    http: reqwest::Client,
    replicas: HashMap<String, Arc<Replica>>,
    wait: Duration,
    auth_ttl: Duration,
    /// API key -> (projections it may read, when that was learned).
    access: RwLock<HashMap<String, (Arc<Vec<String>>, Instant)>>,
}

pub type Shared = Arc<Edge>;

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
    let data_dir = std::env::var("ISOMER_EDGE_DATA_DIR").ok().filter(|s| !s.is_empty()).map(PathBuf::from);
    let mut replicas = HashMap::new();
    for n in &names {
        let store = match &data_dir {
            Some(dir) => Some(std::sync::Mutex::new(Store::open(dir, n)?)),
            None => None,
        };
        replicas.insert(n.clone(), Arc::new(Replica {
            docs: RwLock::new(HashMap::new()),
            cursor: watch::channel(0).0,
            connected: false.into(),
            restored: false.into(),
            last_event: std::sync::Mutex::new(None),
            meta: std::sync::RwLock::new(None),
            store,
        }));
    }
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

    if let Some(listen) = std::env::var("ISOMER_EDGE_MONGO_LISTEN").ok().filter(|s| !s.is_empty()) {
        let set = |name| std::env::var(name).ok().filter(|v: &String| !v.is_empty());
        let tls = match (set("ISOMER_EDGE_TLS_CERT"), set("ISOMER_EDGE_TLS_KEY")) {
            (Some(cert), Some(key)) => Some(mongo::tls_acceptor(&cert, &key)?),
            _ if env_or("ISOMER_EDGE_MONGO_PLAINTEXT", 0) == 1 => {
                warn!("MongoDB without TLS: API keys cross the network in the clear");
                None
            }
            _ => anyhow::bail!(
                "ISOMER_EDGE_MONGO_LISTEN needs ISOMER_EDGE_TLS_CERT and ISOMER_EDGE_TLS_KEY: clients send their API key \
                 with SASL PLAIN (set ISOMER_EDGE_MONGO_PLAINTEXT=1 to allow it without TLS)"
            ),
        };
        let edge = edge.clone();
        tokio::spawn(async move {
            if let Err(e) = mongo::serve(edge, listen, tls).await {
                tracing::error!("MongoDB listener: {e:#}");
                std::process::exit(1);
            }
        });
    }

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
    info!(%listen, projections = ?names, data_dir = ?data_dir, "isomer edge listening");
    let shutdown = async {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    };
    axum::serve(socket, router).with_graceful_shutdown(shutdown).await?;
    Ok(())
}

// ── following the feed ──────────────────────────────────────────────────────

/// What the origin says about this projection now.
async fn fetch_meta(edge: &Edge, name: &str) -> Result<Meta> {
    let resp = edge
        .http
        .get(format!("{}/v1/projections", edge.origin))
        .bearer_auth(&edge.origin_key)
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("listing projections: {}", resp.status());
    }
    let body: Value = resp.json().await?;
    let p = body["projections"]
        .as_array()
        .and_then(|a| a.iter().find(|p| p["name"] == name))
        .with_context(|| format!("the origin key can't read projection {name}, or it doesn't exist"))?;
    Ok(Meta {
        database: p["database"].as_str().map(str::to_owned),
        since: p["history_since"].as_str().unwrap_or_default().to_string(),
    })
}

/// Keep one projection's replica current, forever.
async fn follow(edge: Shared, name: String) {
    let replica = edge.replicas[&name].clone();
    let mut delay = Duration::from_millis(250);
    let mut first = true;
    loop {
        match stream_once(&edge, &name, &replica, first).await {
            Ok(()) => warn!(%name, "live stream ended; reconnecting"),
            Err(e) => warn!(%name, "live stream: {e:#}; reconnecting"),
        }
        first = false;
        replica.connected.store(false, Ordering::Relaxed);
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(5));
    }
}

fn lineage(edge: &Edge, name: &str, meta: &Meta) -> Lineage {
    Lineage { origin: edge.origin.clone(), projection: name.to_string(), since: meta.since.clone() }
}

/// Before (re)connecting: learn the projection's incarnation, restore the copy on disk on the
/// first pass, and start over if the origin's projection is not the one the copy came from.
async fn prepare(edge: &Edge, name: &str, replica: &Replica, first: bool) -> Result<()> {
    let meta = fetch_meta(edge, name).await?;
    let changed = {
        let old = replica.meta.read().unwrap();
        old.as_ref().is_some_and(|m| m.since != meta.since)
    };
    *replica.meta.write().unwrap() = Some(meta.clone());
    if changed {
        warn!(%name, "the projection's history was restarted at the origin; taking a new snapshot");
        replica.cursor.send_replace(0);
    }
    if first {
        if let Some(store) = &replica.store {
            let l = lineage(edge, name, &meta);
            let restored = tokio::task::block_in_place(|| store.lock().unwrap().restore(&l))?;
            if let Some(r) = restored {
                info!(%name, documents = r.docs.len(), cursor = r.cursor, replayed = r.replayed, "restored the copy on disk");
                *replica.docs.write().await = r.docs;
                replica.cursor.send_replace(r.cursor);
                replica.restored.store(true, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

/// One connection to the origin's live stream, applied as it arrives.
async fn stream_once(edge: &Edge, name: &str, replica: &Replica, first: bool) -> Result<()> {
    prepare(edge, name, replica, first).await?;
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
    replica.connected.store(true, Ordering::Relaxed);
    // Connected: whatever happened while this copy was offline now streams in.
    replica.restored.store(false, Ordering::Relaxed);
    info!(%name, cursor, "following the origin's feed");
    let mut body = resp.bytes_stream();
    let mut buf = String::new();
    while let Some(chunk) = body.next().await {
        buf.push_str(&String::from_utf8_lossy(&chunk?));
        // Server-sent events are separated by a blank line.
        let mut applied = false;
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
            apply(edge, name, replica, &event, &data).await?;
            applied = true;
        }
        if applied {
            persist_batch(edge, name, replica).await?;
        }
    }
    Ok(())
}

/// Make the batch just applied durable, folding the log into a new snapshot once it is long.
async fn persist_batch(edge: &Edge, name: &str, replica: &Replica) -> Result<()> {
    let Some(store) = &replica.store else { return Ok(()) };
    let docs = replica.docs.read().await;
    let cursor = *replica.cursor.borrow();
    let meta = replica.meta.read().unwrap().clone().context("no metadata")?;
    tokio::task::block_in_place(|| {
        let mut s = store.lock().unwrap();
        s.sync()?;
        if s.wants_compaction(docs.len()) {
            s.snapshot(&lineage(edge, name, &meta), cursor, &docs)?;
            info!(%name, documents = docs.len(), cursor, "folded the change log into a new snapshot");
        }
        Ok(())
    })
}

async fn apply(edge: &Edge, name: &str, replica: &Replica, event: &str, data: &str) -> Result<()> {
    let v: Value = serde_json::from_str(data).context("event data")?;
    match event {
        "snapshot" => {
            let docs: HashMap<String, Value> = serde_json::from_value(v["docs"].clone()).unwrap_or_default();
            let cursor = v["cursor"].as_i64().unwrap_or(0);
            let n = docs.len();
            if let Some(store) = &replica.store {
                let meta = replica.meta.read().unwrap().clone().context("no metadata")?;
                tokio::task::block_in_place(|| store.lock().unwrap().snapshot(&lineage(edge, name, &meta), cursor, &docs))?;
            }
            *replica.docs.write().await = docs;
            replica.cursor.send_replace(cursor);
            info!(%name, documents = n, cursor, "snapshot applied");
        }
        "change" => {
            let key = v["key"].as_str().unwrap_or_default().to_string();
            let id = v["id"].as_i64();
            // Events (event projections) are a log, not documents: they only move the cursor.
            if v["op"] != "event" {
                let doc = if v["op"] == "delete" { None } else { Some(v["doc"].clone()) };
                if let (Some(store), Some(id)) = (&replica.store, id) {
                    store.lock().unwrap().append(id, &key, doc.as_ref())?;
                }
                let mut docs = replica.docs.write().await;
                match doc {
                    None => docs.remove(&key),
                    Some(d) => docs.insert(key, d),
                };
            }
            if let Some(id) = id {
                replica.cursor.send_replace(id);
            }
        }
        "error" => anyhow::bail!("origin: {data}"),
        _ => {}
    }
    *replica.last_event.lock().unwrap() = Some(Instant::now());
    Ok(())
}

// ── access and read your writes ─────────────────────────────────────────────

impl Edge {
    /// What this key may read, asked of the origin and remembered for a while.
    pub async fn access_of(&self, key: &str) -> std::result::Result<Access, String> {
        if let Some((names, at)) = self.access.read().await.get(key) {
            if at.elapsed() < self.auth_ttl {
                return Ok(Access { names: names.to_vec() });
            }
        }
        let resp = self
            .http
            .get(format!("{}/v1/projections", self.origin))
            .bearer_auth(key)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|e| format!("origin unreachable: {e}"))?;
        if resp.status() == StatusCode::UNAUTHORIZED {
            return Err("unknown or revoked API key".into());
        }
        if !resp.status().is_success() {
            return Err(format!("origin: {}", resp.status()));
        }
        let body: Value = resp.json().await.map_err(|e| e.to_string())?;
        let names: Vec<String> = body["projections"]
            .as_array()
            .map(|a| a.iter().filter_map(|p| p["name"].as_str().map(str::to_owned)).collect())
            .unwrap_or_default();
        self.access.write().await.insert(key.to_string(), (Arc::new(names.clone()), Instant::now()));
        Ok(Access { names })
    }

    /// Wait until the replica reflects the write with this token: (status, message) if not.
    pub async fn wait_after(&self, key: &str, projection: &str, replica: &Replica, token: &str) -> std::result::Result<(), (u16, String)> {
        let resp = self
            .http
            .get(format!("{}/v1/projections/{projection}/cursor", self.origin))
            .bearer_auth(key)
            .header("x-isomer-after", token)
            .timeout(self.wait + Duration::from_secs(2))
            .send()
            .await
            .map_err(|e| (503, format!("origin unreachable: {e}")))?;
        match resp.status() {
            s if s.is_success() => {}
            StatusCode::SERVICE_UNAVAILABLE => return Err((503, "the origin has not applied the write yet".into())),
            StatusCode::BAD_REQUEST => return Err((400, "the write token must be an integer".into())),
            s => return Err((503, format!("origin: {s}"))),
        }
        let body: Value = resp.json().await.map_err(|e| (502, e.to_string()))?;
        let target = body["cursor"].as_i64().unwrap_or(0);
        let mut rx = replica.cursor.subscribe();
        let caught_up = matches!(tokio::time::timeout(self.wait, rx.wait_for(|c| *c >= target)).await, Ok(Ok(_)));
        if caught_up {
            Ok(())
        } else {
            Err((503, format!("this edge has not caught up to the write yet (cursor {target})")))
        }
    }
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

async fn get_doc(State(edge): State<Shared>, headers: HeaderMap, Path((name, key)): Path<(String, String)>) -> Response {
    let Some(api_key) = bearer(&headers) else {
        return error(StatusCode::UNAUTHORIZED, "missing Authorization: Bearer <api key>");
    };
    let Some(replica) = edge.replicas.get(&name) else {
        return error(StatusCode::NOT_FOUND, format!("this edge does not serve {name}"));
    };
    match edge.access_of(api_key).await {
        Ok(a) if a.may_read(&name) => {}
        Ok(_) => return error(StatusCode::NOT_FOUND, format!("no projection named {name}")),
        Err(e) if e.starts_with("unknown") => return error(StatusCode::UNAUTHORIZED, e),
        Err(e) => return error(StatusCode::SERVICE_UNAVAILABLE, e),
    }
    if let Some(token) = headers.get("x-isomer-after").and_then(|v| v.to_str().ok()) {
        if let Err((status, msg)) = edge.wait_after(api_key, &name, replica, token).await {
            return error(StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE), msg);
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
            "connected": r.connected.load(Ordering::Relaxed),
            "restored": r.restored.load(Ordering::Relaxed),
            "database": r.database(),
            "last_event_ms_ago": age,
        }));
    }
    Json(Value::Object(out))
}
