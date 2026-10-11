//! Admin API for the console and CLI (PRD FR-O3): projections, lag, drift, alerts, proposals,
//! the shard advisor, routes and attachments.
//!
//!   GET  /v1/admin/overview                     projections with lag and drift, alerts, nodes
//!   GET  /v1/admin/projections/{name}           definition, sources, certificate, routes, events
//!   POST /v1/admin/projections/{name}/repair    re-derive the keys the auditor found drifted (isomer.repair)
//!   POST /v1/admin/audit                        run the auditor now; body {"sample": 50}
//!   GET  /v1/admin/proposals                    projections proposed from the query log
//!   POST /v1/admin/propose                      mine the query log now
//!   POST /v1/admin/proposals/{id}/accept        body {"name", "freshness", "database"} (all optional)
//!   POST /v1/admin/proposals/{id}/reject
//!   POST /v1/admin/proposals/{id}/route         verify and route the original query onto it
//!   GET  /v1/admin/routes                       query routes and what this connection routed
//!   POST /v1/admin/routes/{queryid}             body {"enabled": bool}
//!   GET  /v1/admin/advice                       the shard advisor
//!   GET  /v1/admin/attachments                  attach-mode sources, slots and lag
//!   GET  /v1/admin/events?after=&limit=         the event log, newest first
//!
//! A key may use these only if its role is a member of `isomer_admin` (keys never act as superusers).

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{role_of, ApiError, ApiResult, Shared};

pub fn routes() -> Router<Shared> {
    Router::new()
        .route("/v1/admin/overview", get(overview))
        .route("/v1/admin/projections/:name", get(projection))
        .route("/v1/admin/projections/:name/repair", post(repair))
        .route("/v1/admin/audit", post(audit))
        .route("/v1/admin/proposals", get(proposals))
        .route("/v1/admin/propose", post(propose))
        .route("/v1/admin/proposals/:id/accept", post(accept))
        .route("/v1/admin/proposals/:id/reject", post(reject))
        .route("/v1/admin/proposals/:id/route", post(route_proposal))
        .route("/v1/admin/routes", get(routes_list))
        .route("/v1/admin/routes/:queryid", post(set_route))
        .route("/v1/admin/advice", get(advice))
        .route("/v1/admin/attachments", get(attachments))
        .route("/v1/admin/events", get(events))
}

/// The request's role, if it may use the admin API.
async fn admin(app: &Shared, headers: &HeaderMap) -> ApiResult<String> {
    let role = role_of(app, headers).await?;
    let db = app.pool.get().await?;
    let ok: bool = db.query_one("SELECT isomer.is_admin($1::text::name)", &[&role]).await?.get(0);
    if ok {
        Ok(role)
    } else {
        Err(ApiError::new(StatusCode::FORBIDDEN, format!("role {role} is not an IsomerDB admin (grant it isomer_admin)")))
    }
}

/// Run a query that returns one jsonb value.
async fn one(app: &Shared, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> ApiResult<Value> {
    let db = app.pool.get().await?;
    Ok(db.query_one(sql, params).await?.get::<_, Option<Value>>(0).unwrap_or(Value::Null))
}

async fn overview(State(app): State<Shared>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    Ok(Json(one(&app, "SELECT isomer.admin_overview()", &[]).await?))
}

async fn projection(State(app): State<Shared>, headers: HeaderMap, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    Ok(Json(one(&app, "SELECT isomer.admin_projection($1)", &[&name]).await?))
}

async fn repair(State(app): State<Shared>, headers: HeaderMap, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    let n = one(&app, "SELECT to_jsonb(isomer.repair($1))", &[&name]).await?;
    Ok(Json(json!({ "projection": name, "repaired": n })))
}

#[derive(Deserialize, Default)]
struct AuditBody {
    sample: Option<i32>,
}

async fn audit(State(app): State<Shared>, headers: HeaderMap, body: Option<Json<AuditBody>>) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    let sample = body.map(|b| b.0).unwrap_or_default().sample.unwrap_or(50).clamp(1, 10_000);
    let drift = one(&app, "SELECT to_jsonb(isomer.audit($1))", &[&sample]).await?;
    let report = one(&app, "SELECT coalesce(jsonb_agg(to_jsonb(d) ORDER BY d.projection), '[]') FROM isomer.drift_report d", &[]).await?;
    Ok(Json(json!({ "drift_found": drift, "report": report })))
}

async fn proposals(State(app): State<Shared>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    let rows = one(
        &app,
        "SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY p.status <> 'proposed', p.benefit_ms_per_s DESC NULLS LAST, p.id), '[]')
         FROM isomer.proposals p",
        &[],
    )
    .await?;
    Ok(Json(json!({ "proposals": rows })))
}

async fn propose(State(app): State<Shared>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    let n = one(&app, "SELECT to_jsonb(isomer.propose())", &[]).await?;
    Ok(Json(json!({ "proposed": n })))
}

#[derive(Deserialize, Default)]
struct AcceptBody {
    name: Option<String>,
    freshness: Option<String>,
    database: Option<String>,
}

async fn accept(
    State(app): State<Shared>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    body: Option<Json<AcceptBody>>,
) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    let b = body.map(|b| b.0).unwrap_or_default();
    let freshness = b.freshness.unwrap_or_else(|| "bounded".into());
    let database = b.database.unwrap_or_else(|| "isomer".into());
    let message = one(
        &app,
        "SELECT to_jsonb(isomer.accept_proposal($1, name => $2, freshness => $3, database => $4))",
        &[&id, &b.name, &freshness, &database],
    )
    .await?;
    Ok(Json(json!({ "id": id, "result": message })))
}

async fn reject(State(app): State<Shared>, headers: HeaderMap, Path(id): Path<i64>) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    app.pool.get().await?.execute("SELECT isomer.reject_proposal($1)", &[&id]).await?;
    Ok(Json(json!({ "id": id, "status": "rejected" })))
}

async fn route_proposal(State(app): State<Shared>, headers: HeaderMap, Path(id): Path<i64>) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    let message = one(&app, "SELECT to_jsonb(isomer.route_proposal($1))", &[&id]).await?;
    Ok(Json(json!({ "id": id, "result": message })))
}

async fn routes_list(State(app): State<Shared>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    let rows = one(
        &app,
        "SELECT coalesce(jsonb_agg(jsonb_build_object('queryid', queryid::text, 'proposal', proposal, 'projection', projection,
                                                      'source_query', source_query, 'measured_speedup', measured_speedup,
                                                      'verified_keys', verified_keys, 'enabled', enabled, 'created_at', created_at)
                                   ORDER BY created_at), '[]')
         FROM isomer.routes",
        &[],
    )
    .await?;
    Ok(Json(json!({ "routes": rows })))
}

#[derive(Deserialize)]
struct RouteBody {
    enabled: bool,
}

async fn set_route(
    State(app): State<Shared>,
    headers: HeaderMap,
    Path(queryid): Path<String>,
    Json(body): Json<RouteBody>,
) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    let queryid: i64 = queryid
        .parse()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "queryid must be an integer"))?;
    let n = app
        .pool
        .get()
        .await?
        .execute("UPDATE isomer.routes SET enabled = $2 WHERE queryid = $1", &[&queryid, &body.enabled])
        .await?;
    if n == 0 {
        return Err(ApiError::new(StatusCode::NOT_FOUND, format!("no route for query {queryid}")));
    }
    Ok(Json(json!({ "queryid": queryid.to_string(), "enabled": body.enabled })))
}

async fn advice(State(app): State<Shared>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    let rows = one(&app, "SELECT coalesce(jsonb_agg(to_jsonb(a)), '[]') FROM isomer.shard_advice() a", &[]).await?;
    Ok(Json(json!({ "advice": rows })))
}

async fn attachments(State(app): State<Shared>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    let rows = one(
        &app,
        "SELECT coalesce(jsonb_agg(to_jsonb(a) || jsonb_build_object('apply_lag_ms', isomer._ms(a.apply_lag)) ORDER BY a.attachment), '[]')
         FROM isomer.attachment_status() a",
        &[],
    )
    .await?;
    Ok(Json(json!({ "attachments": rows })))
}

#[derive(Deserialize)]
struct EventsQuery {
    after: Option<i64>,
    limit: Option<i64>,
    subject: Option<String>,
}

async fn events(State(app): State<Shared>, headers: HeaderMap, Query(q): Query<EventsQuery>) -> ApiResult<Json<Value>> {
    admin(&app, &headers).await?;
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let rows = one(
        &app,
        "SELECT coalesce(jsonb_agg(to_jsonb(e) ORDER BY e.id DESC), '[]')
         FROM (SELECT * FROM isomer.events
               WHERE id > coalesce($1::bigint, 0) AND ($3::text IS NULL OR subject = $3)
               ORDER BY id DESC LIMIT $2) e",
        &[&q.after, &limit, &q.subject],
    )
    .await?;
    Ok(Json(json!({ "events": rows })))
}
