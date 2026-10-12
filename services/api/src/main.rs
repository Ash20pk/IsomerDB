//! IsomerDB HTTP API: projections served directly to applications and clients.
//!
//!   GET /healthz
//!   GET /v1/projections                               projections the key's role may read
//!   GET /v1/projections/{name}/docs/{key}             current document (?as_of=<timestamp> for time travel)
//!   GET /v1/projections/{name}/docs/{key}/history     every version still kept
//!   GET /v1/projections/{name}/snapshot               all documents plus the feed cursor they are as of
//!   GET /v1/projections/{name}/changes?after=&limit=  the change feed, in commit order, gap-free
//!   GET /v1/projections/{name}/live?after=            the same feed as server-sent events
//!   GET /v1/projections/{name}/cursor                 the newest feed cursor (after X-Isomer-After: the
//!                                                     cursor that reflects that write, for edge replicas)
//!   /v1/admin/...                                     operator endpoints for the console and CLI (admin.rs)
//!   GET /console/                                     the console (static files)
//!
//! Every request carries `Authorization: Bearer <key>` (from `isomer.create_api_key`). A key acts
//! as a Postgres role and reads a projection only where `isomer.can_read` allows it: the role can
//! read every table behind the projection and none has row-level security, the same rule query
//! routing follows. A projection the role may not read answers 404, as one that does not exist.
//!
//! Read your writes: send the token of a write (`isomer.last_write_token()`, or the Mongo
//! operationTime) as `X-Isomer-After`; the read waits until the projection has caught up, and
//! answers 503 with `Retry-After` if it does not within `ISOMER_API_WAIT_MS`.
//!
//! Configuration (environment):
//!   ISOMER_DATABASE_URL  libpq-style connection string (default: local Unix socket as postgres)
//!   ISOMER_API_LISTEN    address to listen on (default 0.0.0.0:8080)
//!   ISOMER_API_POOL      database connections (default 16)
//!   ISOMER_API_WAIT_MS   longest wait for X-Isomer-After (default 5000)
//!   ISOMER_CONSOLE_DIR   the built console (default /usr/share/isomer/console)
//!   ISOMER_ALERT_INTERVAL_SECS, ISOMER_CLUSTER_NAME   alert delivery (alerts.rs)

mod admin;
mod alerts;

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Json, Router};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures_util::{stream, Stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio_postgres::{AsyncMessage, NoTls};
use tower_http::services::{ServeDir, ServeFile};
use tracing::{error, info, warn};

struct App {
    pool: Pool,
    /// Projection names from NOTIFY isomer_changes.
    changes: broadcast::Sender<String>,
    wait_ms: u64,
    /// Data requests per role since the last flush to isomer.usage (metering, FR-CL2).
    requests: std::sync::Mutex<std::collections::HashMap<String, i64>>,
}

type Shared = Arc<App>;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();
    let url = std::env::var("ISOMER_DATABASE_URL")
        .unwrap_or_else(|_| "host=/var/run/postgresql user=postgres dbname=postgres".into());
    let listen = std::env::var("ISOMER_API_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let pg: tokio_postgres::Config = url.parse().context("ISOMER_DATABASE_URL")?;
    let manager = Manager::from_config(pg, NoTls, ManagerConfig { recycling_method: RecyclingMethod::Fast });
    let pool = Pool::builder(manager).max_size(env_or("ISOMER_API_POOL", 16)).build()?;

    let (changes, _) = broadcast::channel(1024);
    tokio::spawn(listen_for_changes(url.clone(), changes.clone()));

    let app = Arc::new(App {
        pool,
        changes,
        wait_ms: env_or("ISOMER_API_WAIT_MS", 5000),
        requests: Default::default(),
    });
    tokio::spawn(flush_usage(app.clone()));
    tokio::spawn(alerts::deliver_loop(app.clone()));
    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/projections", get(list_projections))
        .route("/v1/projections/:name/docs/:key", get(get_doc))
        .route("/v1/projections/:name/docs/:key/history", get(get_history))
        .route("/v1/projections/:name/snapshot", get(get_snapshot))
        .route("/v1/projections/:name/changes", get(get_changes))
        .route("/v1/projections/:name/live", get(live))
        .route("/v1/projections/:name/cursor", get(get_cursor))
        .merge(admin::routes())
        .with_state(app);
    let console = std::env::var("ISOMER_CONSOLE_DIR").unwrap_or_else(|_| "/usr/share/isomer/console".into());
    let router = router
        .route("/", get(|| async { Redirect::temporary("/console/") }))
        .nest_service(
            "/console",
            ServeDir::new(&console).fallback(ServeFile::new(format!("{console}/index.html"))),
        );
    let socket = tokio::net::TcpListener::bind(&listen).await?;
    info!(%listen, "isomer api listening");
    axum::serve(socket, router)
        .with_graceful_shutdown(async { tokio::signal::ctrl_c().await.ok(); })
        .await?;
    Ok(())
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Count a data request (projection reads, not the admin API) for metering.
fn count_request(app: &App, role: &str) {
    if let Ok(mut m) = app.requests.lock() {
        *m.entry(role.to_string()).or_insert(0) += 1;
    }
}

/// Every 15 seconds, add the counted requests to isomer.usage. Counts that fail to write are kept
/// for the next attempt.
async fn flush_usage(app: Shared) {
    loop {
        tokio::time::sleep(Duration::from_secs(15)).await;
        let batch: Vec<(String, i64)> = match app.requests.lock() {
            Ok(mut m) => m.drain().collect(),
            Err(_) => continue,
        };
        for (role, n) in batch {
            let ok = match app.pool.get().await {
                Ok(db) => db
                    .execute("SELECT isomer.record_usage('api_requests', $1, $2::bigint)", &[&role, &n])
                    .await
                    .is_ok(),
                Err(_) => false,
            };
            if !ok {
                warn!(%role, n, "could not record API usage yet; keeping it");
                if let Ok(mut m) = app.requests.lock() {
                    *m.entry(role).or_insert(0) += n;
                }
            }
        }
    }
}

/// One connection LISTENs for isomer_changes and fans the projection names out to live streams.
/// Streams also poll on a timer, so a lost listener only adds latency.
async fn listen_for_changes(url: String, tx: broadcast::Sender<String>) {
    loop {
        match tokio_postgres::connect(&url, NoTls).await {
            Ok((client, mut connection)) => {
                let tx = tx.clone();
                let pump = tokio::spawn(async move {
                    let mut messages = stream::poll_fn(move |cx| connection.poll_message(cx));
                    while let Some(message) = messages.next().await {
                        match message {
                            Ok(AsyncMessage::Notification(n)) => {
                                let _ = tx.send(n.payload().to_string());
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("change listener lost its connection: {e}");
                                return;
                            }
                        }
                    }
                });
                if let Err(e) = client.batch_execute("LISTEN isomer_changes").await {
                    warn!("LISTEN isomer_changes failed: {e}");
                }
                let _ = pump.await;
                drop(client);
            }
            Err(e) => warn!("change listener cannot connect yet: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

// ── errors ──────────────────────────────────────────────────────────────────

struct ApiError {
    status: StatusCode,
    message: String,
    code: Option<String>,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        ApiError { status, message: message.into(), code: None }
    }
    fn not_found(name: &str) -> Self {
        Self::new(StatusCode::NOT_FOUND, format!("no projection named {name}"))
    }
}

impl From<tokio_postgres::Error> for ApiError {
    fn from(e: tokio_postgres::Error) -> Self {
        let Some(db) = e.as_db_error() else {
            error!("database: {e}");
            return Self::new(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
        };
        let code = db.code().code().to_string();
        let status = match code.as_str() {
            "IS001" => StatusCode::SERVICE_UNAVAILABLE, // not caught up yet
            "IS002" | "IS003" => StatusCode::GONE,      // before the history horizon / cursor pruned
            "IS004" => StatusCode::NOT_FOUND,
            "IS005" | "22P02" | "22007" | "22008" | "22003" => StatusCode::BAD_REQUEST,
            _ => {
                error!("database: {code} {}", db.message());
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        ApiError { status, message: db.message().to_string(), code: Some(code) }
    }
}

impl From<deadpool_postgres::PoolError> for ApiError {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        error!("pool: {e}");
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "database unavailable")
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut r = (self.status, Json(json!({ "error": self.message, "code": self.code }))).into_response();
        if self.status == StatusCode::SERVICE_UNAVAILABLE {
            r.headers_mut().insert("retry-after", HeaderValue::from_static("1"));
        }
        r
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

// ── auth ────────────────────────────────────────────────────────────────────

/// The Postgres role the request's API key acts as.
async fn role_of(app: &App, headers: &HeaderMap) -> ApiResult<String> {
    let secret = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing Authorization: Bearer <api key>"))?;
    let db = app.pool.get().await?;
    let row = db.query_one("SELECT isomer.api_key_role($1)::text", &[&secret]).await?;
    row.get::<_, Option<String>>(0)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "unknown or revoked API key"))
}

/// The role, once it is known to be allowed to read the projection.
async fn authorize(app: &App, headers: &HeaderMap, projection: &str) -> ApiResult<String> {
    let role = role_of(app, headers).await?;
    let db = app.pool.get().await?;
    let ok: bool = db
        .query_one("SELECT isomer.can_read($1::text::name, $2)", &[&role, &projection])
        .await?
        .get(0);
    if !ok {
        return Err(ApiError::not_found(projection));
    }
    count_request(app, &role);
    Ok(role)
}

/// Honour X-Isomer-After: wait until the projection reflects that write.
async fn wait_after(app: &App, headers: &HeaderMap, projection: &str) -> ApiResult<()> {
    let Some(raw) = headers.get("x-isomer-after") else { return Ok(()) };
    let token: i64 = raw
        .to_str()
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "X-Isomer-After must be a write token (an integer)"))?;
    let db = app.pool.get().await?;
    db.execute(
        "SELECT isomer.wait_for($1, $2, make_interval(secs => $3::float8 / 1000))",
        &[&token, &projection, &(app.wait_ms as f64)],
    )
    .await?;
    Ok(())
}

async fn with_token(app: &App, body: Value) -> ApiResult<Response> {
    let db = app.pool.get().await?;
    let token: i64 = db.query_one("SELECT isomer.current_token()", &[]).await?.get(0);
    let mut r = Json(body).into_response();
    r.headers_mut().insert("x-isomer-token", HeaderValue::from(token));
    Ok(r)
}

// ── handlers ────────────────────────────────────────────────────────────────

async fn healthz(State(app): State<Shared>) -> ApiResult<&'static str> {
    app.pool.get().await?.execute("SELECT 1", &[]).await?;
    Ok("ok")
}

async fn list_projections(State(app): State<Shared>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    let role = role_of(&app, &headers).await?;
    count_request(&app, &role);
    let db = app.pool.get().await?;
    let row = db
        .query_one(
            "SELECT coalesce(jsonb_agg(jsonb_build_object('name', name, 'kind', kind, 'freshness', freshness,
                                                          'history', history::text)), '[]')
             FROM isomer.api_projections($1::text::name)",
            &[&role],
        )
        .await?;
    Ok(Json(json!({ "projections": row.get::<_, Value>(0) })))
}

#[derive(Deserialize)]
struct DocQuery {
    as_of: Option<String>,
}

async fn get_doc(
    State(app): State<Shared>,
    headers: HeaderMap,
    Path((name, key)): Path<(String, String)>,
    Query(q): Query<DocQuery>,
) -> ApiResult<Response> {
    authorize(&app, &headers, &name).await?;
    wait_after(&app, &headers, &name).await?;
    let db = app.pool.get().await?;
    let row = match &q.as_of {
        Some(at) => db.query_one("SELECT isomer.as_of($1, $2, $3::text::timestamptz)", &[&name, &key, at]).await?,
        None => db.query_one("SELECT isomer.get($1, $2)", &[&name, &key]).await?,
    };
    match row.get::<_, Option<Value>>(0) {
        Some(doc) => with_token(&app, doc).await,
        None => Err(ApiError::new(StatusCode::NOT_FOUND, format!("{name} has no document for key {key}"))),
    }
}

async fn get_history(
    State(app): State<Shared>,
    headers: HeaderMap,
    Path((name, key)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    authorize(&app, &headers, &name).await?;
    let db = app.pool.get().await?;
    let row = db
        .query_one(
            "SELECT coalesce(jsonb_agg(jsonb_build_object('id', id, 'op', op, 'doc', doc, 'token', token, 'at', at)
                                       ORDER BY id), '[]')
             FROM isomer.history($1, $2)",
            &[&name, &key],
        )
        .await?;
    Ok(Json(json!({ "key": key, "versions": row.get::<_, Value>(0) })))
}

async fn get_snapshot(State(app): State<Shared>, headers: HeaderMap, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    authorize(&app, &headers, &name).await?;
    wait_after(&app, &headers, &name).await?;
    let db = app.pool.get().await?;
    let row = db.query_one("SELECT cursor, docs FROM isomer.snapshot($1)", &[&name]).await?;
    Ok(Json(json!({ "cursor": row.get::<_, i64>(0), "docs": row.get::<_, Value>(1) })))
}

/// The newest change in the projection's feed. With X-Isomer-After it is taken once the projection
/// reflects that write, so any change up to it, the write's included, is at or before it: an edge
/// replica that has applied the feed this far can answer a read that must see the write.
async fn get_cursor(State(app): State<Shared>, headers: HeaderMap, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    authorize(&app, &headers, &name).await?;
    wait_after(&app, &headers, &name).await?;
    let db = app.pool.get().await?;
    let row = db
        .query_one("SELECT coalesce(max(id), 0) FROM isomer.changes WHERE projection = $1", &[&name])
        .await?;
    Ok(Json(json!({ "cursor": row.get::<_, i64>(0) })))
}

#[derive(Deserialize)]
struct FeedQuery {
    after: Option<i64>,
    limit: Option<i32>,
}

/// One page of the feed as JSON, and the cursor to continue from.
async fn feed_page(app: &App, name: &str, after: i64, limit: i32) -> ApiResult<(Vec<Value>, i64)> {
    let db = app.pool.get().await?;
    let rows = db
        .query(
            "SELECT jsonb_build_object('id', id, 'key', key, 'op', op, 'doc', doc, 'token', token, 'at', at), id
             FROM isomer.changes_since($1, $2, $3)",
            &[&name, &after, &limit],
        )
        .await?;
    let cursor = rows.last().map(|r| r.get::<_, i64>(1)).unwrap_or(after);
    Ok((rows.iter().map(|r| r.get::<_, Value>(0)).collect(), cursor))
}

async fn get_changes(
    State(app): State<Shared>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(q): Query<FeedQuery>,
) -> ApiResult<Json<Value>> {
    authorize(&app, &headers, &name).await?;
    let (changes, cursor) = feed_page(&app, &name, q.after.unwrap_or(0), q.limit.unwrap_or(1000).clamp(1, 10_000)).await?;
    Ok(Json(json!({ "changes": changes, "cursor": cursor })))
}

/// Server-sent events. Without a cursor (`?after=` or `Last-Event-ID`) the stream opens with a
/// `snapshot` event holding every document and the cursor it is as of, then `change` events
/// follow from that cursor, each with its id as the event id, so a reconnecting EventSource
/// resumes exactly where it stopped.
async fn live(
    State(app): State<Shared>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(q): Query<FeedQuery>,
) -> ApiResult<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>> {
    authorize(&app, &headers, &name).await?;
    let resume = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
        .or(q.after);
    let first = match resume {
        Some(cursor) => {
            // Fail now, with a status code, if the cursor is already pruned.
            feed_page(&app, &name, cursor, 1).await?;
            (None, cursor)
        }
        None => {
            let db = app.pool.get().await?;
            let row = db.query_one("SELECT cursor, docs FROM isomer.snapshot($1)", &[&name]).await?;
            let cursor: i64 = row.get(0);
            (Some(json!({ "cursor": cursor, "docs": row.get::<_, Value>(1) })), cursor)
        }
    };
    let mut wake = app.changes.subscribe();
    let stream = async_stream::stream! {
        let (snapshot, mut cursor) = first;
        if let Some(s) = snapshot {
            yield Ok(Event::default().event("snapshot").id(cursor.to_string()).data(s.to_string()));
        }
        loop {
            match feed_page(&app, &name, cursor, 500).await {
                Ok((changes, next)) => {
                    for c in &changes {
                        let id = c.get("id").and_then(Value::as_i64).unwrap_or(next);
                        yield Ok(Event::default().event("change").id(id.to_string()).data(c.to_string()));
                    }
                    let more = next != cursor && changes.len() == 500;
                    cursor = next;
                    if more { continue; }
                }
                Err(e) => {
                    yield Ok(Event::default().event("error").data(json!({ "error": e.message, "code": e.code }).to_string()));
                    if e.status == StatusCode::GONE { break; }
                }
            }
            // Sleep until this projection changes, or a second passes.
            let deadline = tokio::time::sleep(Duration::from_secs(1));
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    _ = &mut deadline => break,
                    msg = wake.recv() => match msg {
                        Ok(p) if p == name => break,
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(_)) => break,
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}
