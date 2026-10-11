/*-------------------------------------------------------------------------
 * IsomerDB: projection-aware request handling in the gateway.
 *
 * Collections that are IsomerDB document projections are derived from SQL tables, so:
 *   * writes to them are refused (applications write to the tables), and
 *   * reads that carry readConcern.afterClusterTime wait until the projection reflects every
 *     change up to that time, through isomer.wait_for(). This gives read-your-writes across the
 *     SQL write path and the MongoDB read path with unmodified drivers: the application advances
 *     its causally-consistent session to the token returned by isomer.last_write_token()
 *     (session.advanceOperationTime(Timestamp(t, i))), and the driver sends it on every read.
 *
 * Token <-> Timestamp: a token is the Citus cluster clock packed as (ms << 22) | counter, and a
 * Mongo Timestamp(t, i) carries it as t = ms / 1000, i = ((ms % 1000) << 22) | counter, which is
 * lossless (i < 1000 * 2^22 < 2^32) and order-preserving.
 *-------------------------------------------------------------------------
 */

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio_postgres::types::Type;

use crate::{
    context::RequestContext,
    error::{DocumentDBError, ErrorCode, Result},
    postgres::{conn_mgmt::StatementError, PgDataClient},
    requests::RequestType,
};

/// How long a "is this collection a projection?" answer is reused.
const PROJECTION_CACHE_TTL: Duration = Duration::from_secs(2);
/// How long a read waits for its projection to catch up before failing with "not yet".
const WAIT_TIMEOUT: &str = "5 seconds";

type CacheEntry = (Option<String>, Instant);

fn projection_cache() -> &'static DashMap<(String, String), CacheEntry> {
    static CACHE: OnceLock<DashMap<(String, String), CacheEntry>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

/// Convert a Mongo cluster Timestamp(t, i) to an IsomerDB token.
#[must_use]
pub fn token_from_timestamp(time: u32, increment: u32) -> i64 {
    let ms = i64::from(time) * 1000 + i64::from(increment >> 22);
    let counter = i64::from(increment & 0x3F_FFFF);
    (ms << 22) | counter
}

/// Convert an IsomerDB token to a Mongo cluster Timestamp(t, i).
#[must_use]
pub fn timestamp_from_token(token: i64) -> (u32, u32) {
    let ms = token >> 22;
    let counter = token & 0x3F_FFFF;
    (
        u32::try_from(ms / 1000).unwrap_or(u32::MAX),
        u32::try_from(((ms % 1000) << 22) | counter).unwrap_or(u32::MAX),
    )
}

const fn is_read(request_type: RequestType) -> bool {
    matches!(
        request_type,
        RequestType::Find | RequestType::Aggregate | RequestType::Count | RequestType::Distinct
    )
}

const fn is_write(request_type: RequestType) -> bool {
    matches!(
        request_type,
        RequestType::Insert
            | RequestType::Update
            | RequestType::Delete
            | RequestType::FindAndModify
            | RequestType::Drop
            | RequestType::RenameCollection
    )
}

fn after_cluster_time(request_context: &RequestContext<'_>) -> Option<(u32, u32)> {
    let read_concern = request_context.request().document().get_document("readConcern").ok()?;
    let ts = read_concern.get_timestamp("afterClusterTime").ok()?;
    (ts.time != 0 || ts.increment != 0).then_some((ts.time, ts.increment))
}

fn statement_error(error: StatementError) -> DocumentDBError {
    if let StatementError::Postgres(e) = &error {
        if let Some(db) = e.as_db_error() {
            if db.code().code() == "IS001" {
                return DocumentDBError::documentdb_error(ErrorCode::ExceededTimeLimit, db.message().to_owned());
            }
        }
    }
    DocumentDBError::from(error)
}

async fn projection_for(
    pg_data_client: &impl PgDataClient,
    db: &str,
    collection: &str,
) -> Result<Option<String>> {
    let key = (db.to_owned(), collection.to_owned());
    if let Some(entry) = projection_cache().get(&key) {
        if entry.1.elapsed() < PROJECTION_CACHE_TTL {
            return Ok(entry.0.clone());
        }
    }
    let connection = pg_data_client.pull_connection_with_transaction(false).await?;
    let rows = connection
        .query(
            "SELECT isomer.projection_for_collection($1, $2) WHERE EXISTS \
             (SELECT 1 FROM pg_extension WHERE extname = 'isomer')",
            &[Type::TEXT, Type::TEXT],
            &[&db, &collection],
        )
        .await
        .map_err(statement_error)?;
    let projection: Option<String> = rows.first().and_then(|r| r.get(0));
    projection_cache().insert(key, (projection.clone(), Instant::now()));
    Ok(projection)
}

/// Runs before every request. Refuses writes to projection collections, and makes reads with
/// afterClusterTime wait for their projection.
///
/// # Errors
///
/// Returns an error for a write to a projection, or when a projection has not caught up to the
/// requested cluster time within the wait timeout.
pub async fn before_request(
    request_context: &RequestContext<'_>,
    pg_data_client: &impl PgDataClient,
) -> Result<()> {
    let request_type = request_context.execution_request_type();
    let read = is_read(request_type);
    if !read && !is_write(request_type) {
        return Ok(());
    }
    let request = request_context.request();
    let Ok(collection) = request.collection() else {
        return Ok(());
    };
    if collection.is_empty() {
        return Ok(());
    }
    let wait = if read { after_cluster_time(request_context) } else { None };
    if read && wait.is_none() {
        return Ok(());
    }
    let Some(projection) = projection_for(pg_data_client, request.db(), collection).await? else {
        return Ok(());
    };
    if !read {
        // Code 166, as MongoDB returns for writes to a (read-only) view. Code 20 would make
        // drivers report "retryable writes not supported" instead of this message.
        return Err(DocumentDBError::documentdb_error(
            ErrorCode::CommandNotSupportedOnView,
            format!(
                "{}.{collection} is an IsomerDB projection derived from SQL tables and is read-only; \
                 write to its source tables instead",
                request.db()
            ),
        ));
    }
    if let Some((time, increment)) = wait {
        let cluster_position = token_from_timestamp(time, increment);
        let connection = pg_data_client.pull_connection_with_transaction(false).await?;
        connection
            .query(
                &format!("SELECT isomer.wait_for($1, $2, '{WAIT_TIMEOUT}')"),
                &[Type::INT8, Type::TEXT],
                &[&cluster_position, &projection],
            )
            .await
            .map_err(statement_error)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trips_through_a_mongo_timestamp() {
        for token in [0_i64, (1_791_705_332_694_i64 << 22) | 3, (1_791_705_332_999_i64 << 22) | 0x3F_FFFF] {
            let (t, i) = timestamp_from_token(token);
            assert_eq!(token_from_timestamp(t, i), token);
        }
    }

    #[test]
    fn timestamps_order_like_tokens() {
        let a = (1_791_705_332_694_i64 << 22) | 9;
        let b = (1_791_705_332_695_i64 << 22) | 1;
        assert!(timestamp_from_token(a) < timestamp_from_token(b));
    }
}
