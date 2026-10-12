//! IsomerDB embedder: computes embeddings for vector projections outside Postgres (PRD FR-V1a).
//!
//! The database decides *what* needs embedding (`isomer.embed_jobs`, written when a row's text
//! hash changes) and this service decides *how*: it leases batches packed by token count,
//! rate-limits per model, calls the provider, and hands results back through
//! `isomer.embed_complete`, which only writes a result whose text hash still matches. Failures go
//! through `isomer.embed_failed` (backoff, `Retry-After`, dead letter). Query-time requests from
//! `isomer.embed()` are answered first. A large backfill for a model registered with
//! `batch_api` goes through the provider's Batch API instead (`batch.rs`).
//!
//! Configuration (environment):
//!   ISOMER_DATABASE_URL   libpq-style connection string (default: local Unix socket as postgres)
//!   ISOMER_WORKERS        concurrent batch loops (default 2)
//!   ISOMER_LEASE_SECONDS  how long a claimed batch stays invisible to other workers (default 60)
//!   ISOMER_BATCH_POLL_SECONDS  how often provider batches are started and checked (default 30)
//!   ISOMER_BATCH_MAX_ITEMS     the most items one provider batch carries (default 50000)
//!   <api_key_env>         per model, the variable named in isomer.embedding_models.api_key_env

mod batch;
mod local;
mod providers;
mod ratelimit;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{stream, StreamExt};
use tokio::sync::{Mutex, Notify};
use tokio_postgres::{AsyncMessage, Client, NoTls};
use tracing::{error, info, warn};

use providers::{EmbedError, Model};
use ratelimit::RateLimiter;

struct Job {
    projection: String,
    key: String,
    model: String,
    source_hash: String,
    input: String,
    tokens_est: i32,
    phase: String,
}

struct Shared {
    http: reqwest::Client,
    limiters: Mutex<HashMap<String, Arc<RateLimiter>>>,
    wake: Notify,
    lease_seconds: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into()))
        .init();

    let url = std::env::var("ISOMER_DATABASE_URL")
        .unwrap_or_else(|_| "host=/var/run/postgresql user=postgres dbname=postgres".into());
    let workers: usize = env_or("ISOMER_WORKERS", 2);
    let shared = Arc::new(Shared {
        http: reqwest::Client::builder().timeout(Duration::from_secs(60)).build()?,
        limiters: Mutex::new(HashMap::new()),
        wake: Notify::new(),
        lease_seconds: env_or("ISOMER_LEASE_SECONDS", 60),
    });

    // One connection listens for NOTIFY isomer_embed and wakes the workers.
    let listener = connect_with_retry(&url).await?;
    {
        let (client, mut connection) = listener;
        let shared = shared.clone();
        tokio::spawn(async move {
            let mut messages = stream::poll_fn(move |cx| connection.poll_message(cx));
            while let Some(message) = messages.next().await {
                match message {
                    Ok(AsyncMessage::Notification(_)) => shared.wake.notify_waiters(),
                    Ok(_) => {}
                    Err(e) => {
                        error!("listener connection lost: {e}");
                        std::process::exit(1);
                    }
                }
            }
        });
        client.batch_execute("LISTEN isomer_embed").await?;
        // Keep the client alive for the lifetime of the process.
        std::mem::forget(client);
    }

    // One more connection drives provider batches for large backfills.
    {
        let (client, connection) = connect_with_retry(&url).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("batch connection lost: {e}");
                std::process::exit(1);
            }
        });
        let shared = shared.clone();
        tokio::spawn(async move { batch_loop(client, shared).await });
    }

    info!(workers, "embedder started");
    let mut handles = Vec::new();
    for id in 0..workers {
        let (client, connection) = connect_with_retry(&url).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("worker {id} connection lost: {e}");
                std::process::exit(1);
            }
        });
        let shared = shared.clone();
        handles.push(tokio::spawn(async move { worker_loop(id, client, shared).await }));
    }
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("shutting down"),
        r = futures_util::future::select_all(handles) => { error!("a worker stopped: {:?}", r.0); }
    }
    Ok(())
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

async fn connect_with_retry(
    url: &str,
) -> Result<(Client, tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>)> {
    let mut delay = Duration::from_millis(500);
    for attempt in 1..=60 {
        match tokio_postgres::connect(url, NoTls).await {
            Ok(c) => return Ok(c),
            Err(e) => {
                warn!(attempt, "cannot connect to Postgres yet: {e}");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
        }
    }
    anyhow::bail!("could not connect to Postgres")
}

async fn worker_loop(id: usize, client: Client, shared: Arc<Shared>) -> Result<()> {
    loop {
        let mut did_work = false;
        match load_models(&client).await {
            Ok(models) => {
                if id == 0 {
                    did_work |= answer_queries(&client, &shared, &models).await.unwrap_or_else(|e| {
                        error!("query embeddings: {e:#}");
                        false
                    });
                }
                for model in models.values() {
                    match run_batch(&client, &shared, model).await {
                        Ok(n) => did_work |= n > 0,
                        Err(e) => error!(model = %model.name, "batch: {e:#}"),
                    }
                }
            }
            Err(e) => error!("loading models: {e:#}"),
        }
        if !did_work {
            // Sleep until NOTIFY, or poll again after half a second (jobs whose backoff expired
            // produce no notification).
            let _ = tokio::time::timeout(Duration::from_millis(500), shared.wake.notified()).await;
        }
    }
}

async fn load_models(client: &Client) -> Result<HashMap<String, Model>> {
    let rows = client
        .query(
            "SELECT name, provider, endpoint, model, dimensions, api_key_env, max_batch_tokens,
                    max_batch_items, requests_per_min, tokens_per_min, batch_api, batch_min_items
             FROM isomer.embedding_models",
            &[],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let m = Model {
                name: r.get(0),
                provider: r.get(1),
                endpoint: r.get(2),
                model: r.get(3),
                dimensions: r.get::<_, i32>(4) as usize,
                api_key_env: r.get(5),
                max_batch_tokens: r.get::<_, i32>(6) as i64,
                max_batch_items: r.get::<_, i32>(7) as i64,
                requests_per_min: r.get::<_, i32>(8) as f64,
                tokens_per_min: r.get::<_, i32>(9) as f64,
                batch_api: r.get(10),
                batch_min_items: r.get::<_, i32>(11) as i64,
            };
            (m.name.clone(), m)
        })
        .collect())
}

async fn limiter_for(shared: &Shared, model: &Model) -> Arc<RateLimiter> {
    let mut limiters = shared.limiters.lock().await;
    limiters
        .entry(model.name.clone())
        .or_insert_with(|| Arc::new(RateLimiter::new(model.requests_per_min, model.tokens_per_min)))
        .clone()
}

/// Lease a batch of jobs for one model, embed it, and report each result.
async fn run_batch(client: &Client, shared: &Shared, model: &Model) -> Result<usize> {
    let jobs = lease_jobs(client, model, shared.lease_seconds).await?;
    if jobs.is_empty() {
        return Ok(0);
    }
    embed_jobs(client, shared, model, &jobs).await?;
    Ok(jobs.len())
}

async fn embed_jobs(client: &Client, shared: &Shared, model: &Model, jobs: &[Job]) -> Result<()> {
    let tokens: i64 = jobs.iter().map(|j| j.tokens_est as i64).sum();
    limiter_for(shared, model).await.acquire(tokens as f64).await;

    let inputs: Vec<&str> = jobs.iter().map(|j| j.input.as_str()).collect();
    match providers::embed(&shared.http, model, &inputs).await {
        Ok(result) => {
            let mut written = 0;
            for (job, vector) in jobs.iter().zip(result.vectors.iter()) {
                let applied: bool = client
                    .query_one(
                        "SELECT isomer.embed_complete($1, $2, $3, $4, $5)",
                        &[&job.projection, &job.key, &job.model, &job.source_hash, &providers::to_pgvector(vector)],
                    )
                    .await?
                    .get(0);
                written += applied as usize;
            }
            record_usage(client, jobs, result.tokens.unwrap_or(tokens)).await?;
            info!(model = %model.name, items = jobs.len(), written, tokens, "embedded batch");
        }
        // One bad input fails the whole request: retry the items one by one so only it fails.
        Err(EmbedError::Permanent(message)) if jobs.len() > 1 => {
            warn!(model = %model.name, items = jobs.len(), "batch rejected ({message}); retrying items individually");
            for job in jobs {
                Box::pin(embed_jobs(client, shared, model, std::slice::from_ref(job))).await?;
            }
        }
        Err(err) => {
            let (message, retry_after) = match &err {
                EmbedError::RateLimited { retry_after, message } => (message.clone(), *retry_after),
                EmbedError::Transient(m) | EmbedError::Permanent(m) => (m.clone(), None),
            };
            warn!(model = %model.name, items = jobs.len(), "embedding failed: {message}");
            let max_attempts: i32 = if matches!(err, EmbedError::Permanent(_)) { 0 } else { 8 };
            for job in jobs {
                client
                    .execute(
                        "SELECT isomer.embed_failed($1, $2, $3, $4, $5, $6, $7)",
                        &[&job.projection, &job.key, &job.model, &job.source_hash, &message, &retry_after, &max_attempts],
                    )
                    .await?;
            }
        }
    }
    Ok(())
}

/// Claim ready jobs for a model, steady-state work before backfill, packed so the batch stays
/// within the model's token and item limits. Leasing pushes `not_before` forward, so if this
/// worker dies the jobs become visible again when the lease expires. For a model with a Batch
/// API, a backfill big enough for a provider batch is left to the batch loop.
async fn lease_jobs(client: &Client, model: &Model, lease_seconds: i64) -> Result<Vec<Job>> {
    let rows = client
        .query(
            "WITH locked AS (
                 SELECT j.projection, j.key, j.model, j.source_hash, j.input, j.tokens_est, j.phase, j.enqueued_at
                 FROM isomer.embed_jobs j
                 JOIN isomer.projections p ON p.name = j.projection AND p.status = 'active'
                 WHERE j.model = $1 AND j.not_before <= now()
                   AND NOT (j.phase = 'backfill' AND $5
                            AND (SELECT count(*) FROM isomer.embed_jobs b
                                 WHERE b.model = $1 AND b.phase = 'backfill' AND b.batch_id IS NULL) >= $6)
                 ORDER BY (j.phase = 'backfill'), j.enqueued_at, j.key
                 LIMIT $2
                 FOR UPDATE OF j SKIP LOCKED),
             candidates AS (
                 SELECT l.*, sum(l.tokens_est) OVER (ORDER BY (l.phase = 'backfill'), l.enqueued_at, l.key) AS running
                 FROM locked l),
             chosen AS (
                 SELECT * FROM candidates
                 WHERE running <= $3 OR running = (SELECT min(running) FROM candidates))
             UPDATE isomer.embed_jobs j
             SET attempts = j.attempts + 1, not_before = now() + make_interval(secs => $4)
             FROM chosen c
             WHERE j.projection = c.projection AND j.key = c.key AND j.model = c.model
             RETURNING c.projection, c.key, c.model, c.source_hash, c.input, c.tokens_est, c.phase",
            &[&model.name, &model.max_batch_items, &model.max_batch_tokens, &(lease_seconds as f64),
              &model.batch_api, &model.batch_min_items],
        )
        .await
        .context("leasing jobs")?;
    Ok(rows
        .into_iter()
        .map(|r| Job {
            projection: r.get(0),
            key: r.get(1),
            model: r.get(2),
            source_hash: r.get(3),
            input: r.get(4),
            tokens_est: r.get(5),
            phase: r.get(6),
        })
        .collect())
}

/// Attribute a batch's tokens to (projection, phase) in proportion to each job's estimate.
async fn record_usage(client: &Client, jobs: &[Job], tokens: i64) -> Result<()> {
    let estimated: i64 = jobs.iter().map(|j| j.tokens_est as i64).sum::<i64>().max(1);
    let mut groups: HashMap<(&str, &str), (i32, i64)> = HashMap::new();
    for job in jobs {
        let entry = groups.entry((&job.projection, &job.phase)).or_default();
        entry.0 += 1;
        entry.1 += job.tokens_est as i64;
    }
    for ((projection, phase), (items, est)) in groups {
        let share = (tokens as f64 * est as f64 / estimated as f64).round() as i64;
        client
            .execute(
                "SELECT isomer.record_embedding_usage($1, $2, $3, $4, $5)",
                &[&projection, &jobs[0].model, &phase, &items, &share],
            )
            .await?;
    }
    Ok(())
}

/// Answer `isomer.embed()` calls: they are waiting in a SQL session, so they go first.
async fn answer_queries(client: &Client, shared: &Shared, models: &HashMap<String, Model>) -> Result<bool> {
    let rows = client
        .query(
            "SELECT id, model, input FROM isomer.embed_requests
             WHERE result IS NULL AND error IS NULL ORDER BY id LIMIT 64",
            &[],
        )
        .await?;
    if rows.is_empty() {
        return Ok(false);
    }
    let mut by_model: HashMap<String, Vec<(i64, String)>> = HashMap::new();
    for r in rows {
        by_model.entry(r.get(1)).or_default().push((r.get(0), r.get(2)));
    }
    for (name, requests) in by_model {
        let Some(model) = models.get(&name) else { continue };
        let inputs: Vec<&str> = requests.iter().map(|(_, i)| i.as_str()).collect();
        let tokens: i64 = inputs.iter().map(|i| (i.len() as i64 / 4).max(1)).sum();
        // A SQL session is waiting (isomer.embed's default timeout is 10s), so retry rate limits
        // and transient errors a few times here rather than failing the query at once.
        let mut attempt = 0;
        let outcome = loop {
            limiter_for(shared, model).await.acquire(tokens as f64).await;
            match providers::embed(&shared.http, model, &inputs).await {
                Err(EmbedError::RateLimited { retry_after, .. }) if attempt < 3 => {
                    tokio::time::sleep(Duration::from_secs_f64(retry_after.unwrap_or(0.5).min(3.0))).await;
                }
                Err(EmbedError::Transient(_)) if attempt < 3 => {
                    tokio::time::sleep(Duration::from_millis(250 << attempt)).await;
                }
                other => break other,
            }
            attempt += 1;
        };
        match outcome {
            Ok(result) => {
                for ((id, _), vector) in requests.iter().zip(result.vectors.iter()) {
                    client
                        .execute(
                            "UPDATE isomer.embed_requests SET result = $2, done_at = clock_timestamp() WHERE id = $1",
                            &[id, &providers::to_pgvector(vector)],
                        )
                        .await?;
                }
                client
                    .execute(
                        "SELECT isomer.record_embedding_usage(NULL, $1, 'query', $2, $3)",
                        &[&model.name, &(requests.len() as i32), &result.tokens.unwrap_or(tokens)],
                    )
                    .await?;
            }
            Err(e) => {
                let message = e.to_string();
                for (id, _) in &requests {
                    client
                        .execute(
                            "UPDATE isomer.embed_requests SET error = $2, done_at = clock_timestamp() WHERE id = $1",
                            &[id, &message],
                        )
                        .await?;
                }
            }
        }
    }
    Ok(true)
}

// ── Provider batches (FR-V1a) ────────────────────────────────────────────────

async fn batch_loop(client: Client, shared: Arc<Shared>) {
    let every = Duration::from_secs(env_or("ISOMER_BATCH_POLL_SECONDS", 30));
    loop {
        if let Err(e) = batch_pass(&client, &shared).await {
            error!("batches: {e:#}");
        }
        tokio::time::sleep(every).await;
    }
}

/// Check every open batch, then start new ones where a model has enough backfill waiting.
async fn batch_pass(client: &Client, shared: &Shared) -> Result<()> {
    let models = load_models(client).await?;
    for row in client
        .query("SELECT id, model, status, provider_batch_id FROM isomer.embed_batches_open()", &[])
        .await?
    {
        let (id, model_name, status, provider_id): (i64, String, String, Option<String>) =
            (row.get(0), row.get(1), row.get(2), row.get(3));
        let Some(model) = models.get(&model_name) else { continue };
        let Some(provider_id) = provider_id.filter(|_| status == "submitted") else {
            // Leased but never submitted: this process stopped in between. Give the jobs back.
            client
                .execute("SELECT isomer.embed_batch_finished($1, 'failed', NULL, 'not submitted: the embedder stopped first')", &[&id])
                .await?;
            continue;
        };
        if let Err(e) = check_batch(client, shared, model, id, &provider_id).await {
            warn!(model = %model.name, batch = id, "checking batch: {e:#}");
        }
    }
    let max_items: i32 = env_or("ISOMER_BATCH_MAX_ITEMS", 50_000);
    for model in models.values().filter(|m| m.batch_api) {
        let rows = client
            .query("SELECT batch_id, item, input FROM isomer.embed_batch_lease($1, $2)", &[&model.name, &max_items])
            .await?;
        if rows.is_empty() {
            continue;
        }
        let id: i64 = rows[0].get(0);
        let items: Vec<(i32, String)> = rows.iter().map(|r| (r.get(1), r.get(2))).collect();
        match batch::submit(&shared.http, model, &items).await {
            Ok(s) => {
                client
                    .execute("SELECT isomer.embed_batch_submitted($1, $2, $3)", &[&id, &s.batch_id, &s.input_file_id])
                    .await?;
                info!(model = %model.name, batch = id, provider_batch = %s.batch_id, items = items.len(), "submitted backfill batch");
            }
            Err(e) => {
                let message = e.to_string();
                client
                    .execute("SELECT isomer.embed_batch_finished($1, 'failed', NULL, $2)", &[&id, &message])
                    .await?;
                warn!(model = %model.name, batch = id, "submitting batch failed, items go back to ordinary requests: {message}");
            }
        }
    }
    Ok(())
}

async fn check_batch(client: &Client, shared: &Shared, model: &Model, id: i64, provider_id: &str) -> Result<()> {
    let st = batch::status(&shared.http, model, provider_id).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    if !matches!(st.status.as_str(), "completed" | "failed" | "expired" | "cancelled") {
        client.execute("UPDATE isomer.embed_batches SET last_checked_at = clock_timestamp() WHERE id = $1", &[&id]).await?;
        return Ok(());
    }
    // Results (all of them when completed; what finished before an expiry or cancellation).
    let mut tokens: i64 = 0;
    let (mut written, mut failed) = (0usize, 0usize);
    for file_id in [st.output_file_id.as_deref(), st.error_file_id.as_deref()].into_iter().flatten() {
        let text = batch::file(&shared.http, model, file_id).await.map_err(|e| anyhow::anyhow!("{e}"))?;
        for r in batch::parse(&text) {
            tokens += r.tokens;
            match r.embedding.filter(|v| v.len() == model.dimensions) {
                Some(v) => {
                    let applied: bool = client
                        .query_one("SELECT isomer.embed_batch_result($1, $2, $3)", &[&id, &r.item, &providers::to_pgvector(&v)])
                        .await?
                        .get(0);
                    written += applied as usize;
                }
                None => {
                    let message = r.error.unwrap_or_else(|| format!("wrong number of dimensions (registered {})", model.dimensions));
                    client.execute("SELECT isomer.embed_batch_item_failed($1, $2, $3)", &[&id, &r.item, &message]).await?;
                    failed += 1;
                }
            }
        }
    }
    let left: i32 = client
        .query_one("SELECT isomer.embed_batch_finished($1, $2, $3, $4)", &[&id, &st.status, &tokens, &st.error])
        .await?
        .get(0);
    info!(model = %model.name, batch = id, status = %st.status, written, failed, back_to_ordinary = left, tokens, "batch finished");
    shared.wake.notify_waiters();
    Ok(())
}
