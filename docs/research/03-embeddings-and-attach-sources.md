# Research: embedding execution and billing; attach-mode sources

Researched 11 October 2026 against official docs, except items marked [unverified] or [3rd-party].

## Q1. Where should embeddings run, and who pays?

### How others do it

| System | Where the model runs | Who pays | Change detection / batching |
|---|---|---|---|
| **pgai Vectorizer** | Started in-database; v0.10 (Apr 2025) moved to an external Python worker. **Unmaintained Feb 2026; archived 27 May 2026**; Tiger Cloud managed vectorizer removed 30 Jun 2026. No reason given. | Your own key | Queue table plus worker. https://github.com/timescale/pgai, https://www.tigerdata.com/docs/deploy/tiger-cloud/vectorizer-deprecation |
| **Supabase** | Edge Function, outside the database; gte-small can run on CPU in the Edge Runtime | Your own key (OpenAI) plus Edge compute | Triggers → pgmq queue (only when source columns change) → pg_cron every 10 s → pg_net. Batches of 10×10, 5-min visibility timeout as retry, delete after write. https://supabase.com/docs/guides/ai/automatic-embeddings |
| **Neon** | No managed pipeline; experimental `pgrag` (local bge-small ONNX on CPU, or remote APIs) | Your own key | Manual SQL |
| **MongoDB Atlas automated embedding** | Voyage AI service, called by `mongot` (MongoDB 8.2+) | **Both**: Atlas-issued key bills the Atlas org, or a Voyage key bills Voyage | Billed on initial sync, writes and query text. voyage-4-lite $0.02/M, voyage-4 $0.06/M, -large $0.12/M; 200M free tokens per model, once. Rate limits delay indexing but don't disable it. https://www.mongodb.com/docs/vector-search/crud-embeddings/automated-embedding/billing/ |
| **Databricks Vector Search** | Model Serving endpoint (pay-per-token, or provisioned throughput; GPU recommended) | Bundled into DBUs | Delta Change Data Feed; only changed rows; storage-optimised endpoints reuse unchanged embeddings |
| **Elastic `semantic_text`** | ML nodes in the cluster (billed by VCU) **or** the managed Elastic Inference Service (per M tokens) | Bundled or your own key | Automatic chunking. Default model changed between versions (Jina v5 on 9.4), and Elastic warns mixing endpoints raises cost. EIS limits 6,000 RPM, 6M ingest TPM per model. |
| **Pinecone** | Hosted | Bundled; llama-text-embed-v2 $0.16/M, e5-large $0.08/M; 5–10M tokens/mo included | At upsert and query time |
| **Weaviate** | API modules or local sidecars; also a hosted Weaviate Embeddings service ($0.025–0.040/M) | Your own key per request (never stored in the schema), or Weaviate Cloud billing | Per-client RPM/TPM headers |
| **Snowflake Cortex** | In-platform | Credits, input tokens only, plus warehouse time | Usage view `CORTEX_FUNCTIONS_USAGE_HISTORY` |

### List prices (per million input tokens)

| Model | Price |
|---|---|
| OpenAI text-embedding-3-small / -large | $0.02 / $0.13 |
| Voyage 4-lite / 4 / 4-large | $0.02 / $0.06 / $0.12; Batch API 33% off (12-h window) |
| Cohere embed-v4.0 | ~$0.12 [3rd-party] |
| Self-hosted bge-m3 on an L4 GPU | ~$0.007–0.01 at full utilisation [3rd-party / arithmetic] |

**Break-even for our own GPUs:** an always-on L4 costs about $600/month. At $0.02/M that buys about
30B tokens, so our own GPUs only pay off above roughly 30B tokens a month, and only if they stay
busy.

### Lessons

1. Nobody runs inference inside a Postgres backend in production. pgai moved out and was then shut
   down.
2. Bring-your-own-key and bundled billing both exist; MongoDB and Elastic offer both.
3. The standard pipeline is per-row change detection, a durable queue, and delete-after-write.
4. Pin the model version, or a changed default silently changes cost and quality.

### Recommendation for v1

- **Execution: a separate, horizontally scaled embedding worker service.** Never call HTTP from
  Postgres backends.
  - Shape creation installs change capture into a durable queue.
  - Each row keeps a `source_hash` of the EMBED expression, and work is queued only when it
    changes.
  - In attach mode, the same queue is fed from applied changes.
- **Providers:**
  1. **Bring your own key** (OpenAI, Voyage, Cohere, any OpenAI-compatible endpoint, including
     customer-hosted vLLM/TEI). Keys live in a secret store and are referenced by name, never kept
     in the catalog. This is the production default.
  2. **Bundled models:** one cheap and one quality option (for example text-embedding-3-small and
     voyage-4), metered per token at list price plus a small markup, with a one-time free allowance.
  3. **Optional in-cluster CPU model** (gte-small / bge-small ONNX) for the free tier, dev and
     air-gapped installs.
  4. **Defer our own GPU fleet** until bundled volume passes about 30B tokens a month.
- **Model pinning:** the shape records model, version and dimensions. Changing the model creates a
  new shape version: re-embed in the background, then swap atomically.
- **Batching and retries:**
  - Pack batches by token count.
  - Client-side token bucket per key (RPM and TPM).
  - On 429, honour `Retry-After`; otherwise exponential backoff with jitter.
  - Visibility-timeout retry, then a dead-letter queue.
  - Make writes idempotent on (pk, source_hash) so stale results never overwrite new ones.
  - Separate ingest and query keys.
  - Use provider Batch APIs for the backfill.
- **Cost controls:**
  - Estimate tokens and cost before a backfill.
  - Per-shape monthly caps that pause rather than fail.
  - Usage split into backfill / steady state / query.

## Q2. Which managed Postgres providers can attach mode follow?

### Postgres 18 changes that matter

- Generated columns can be published (`publish_generated_columns = stored`).
- `CREATE SUBSCRIPTION` defaults to `streaming = parallel`.
- Conflicts are logged and counted in `pg_stat_subscription_stats`. Use this as a check that the
  copy hasn't diverged before cutover.
- `idle_replication_slot_timeout`.
- `max_active_replication_origins`.
- `pg_createsubscriber --all / --clean`, `pg_recvlogical --enable-failover`.
- From PG 17: failover slots (`failover = true`, `sync_replication_slots`, `hot_standby_feedback`,
  `synchronized_standby_slots`).
- https://www.postgresql.org/docs/18/release-18.html

### Core limits

From https://www.postgresql.org/docs/18/logical-replication-restrictions.html:

- DDL, sequences and large objects are not replicated.
- Only tables replicate (no views, matviews or foreign tables).
- TRUNCATE fails across foreign keys if some linked tables aren't in the subscription.
- `REPLICA IDENTITY FULL` breaks on types without a btree/hash operator class.
- **`FOR ALL TABLES` / `FOR TABLES IN SCHEMA` need superuser**; adding a single table needs
  ownership.
- **Risk to the customer:** publishing a table with no primary key or replica identity makes the
  customer's own UPDATE and DELETE statements on that table fail.
- Unchanged TOAST values are not resent on UPDATE.
- Slots retain WAL until the subscriber confirms.

### Provider matrix

| Provider | How to enable | Role | `FOR ALL TABLES` | Failover slots | Notes |
|---|---|---|---|---|---|
| **AWS RDS Postgres** | `rds.logical_replication=1` (reboot) | `rds_superuser` to enable; `rds_replication` to stream | Yes | PG 17+ slot sync (documented for replica promotion) | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication.html |
| **Aurora Postgres** | Same, in a cluster parameter group | Same | Likely [unverified] | Not documented | No logical decoding from readers |
| **Cloud SQL** | `cloudsql.logical_decoding=on` | `REPLICATION` + `cloudsqlsuperuser` | Probably | PG 17+, **Enterprise Plus only** | Restoring the publisher breaks replication |
| **AlloyDB** | `alloydb.logical_decoding=on` | `REPLICATION IN ROLE alloydbsuperuser` | [unverified] | [unverified] | Public IP + SSL |
| **Azure Flexible Server** | `wal_level=logical`, ≥16 workers | `REPLICATION`; `azure_pg_admin` is not a superuser | Probably **no** | PG 17+ native; PG ≤16 needs `pg_failover_slots` | **Auto-drops unused slots when storage is low** |
| **Supabase** | On by default | `postgres` or a REPLICATION role | Likely | [unverified] | Direct connection only; subscriber needs IPv6 or the IPv4 add-on |
| **Neon** | Project setting (irreversible, restarts compute) | `neon_superuser` | [unverified] | n/a; restoring a branch deletes slots | **Inactive slots removed after ~40 h**; 10 slots; no scale-to-zero while subscribed |
| **Crunchy Bridge** | On by default | Real superuser | Yes | [unverified] | Product direction uncertain after the Snowflake acquisition |
| **PlanetScale Postgres** | `wal_level=logical` | REPLICATION user with branch suffix | Add tables individually | **Required** (non-failover slots get a 3-day warning) | Set `max_slot_wal_keep_size` > 4 GB |
| **DigitalOcean** | Conflicting sources: Electric says it works, PeerDB says not | `doadmin` | No superuser | [unverified] | Treat as experimental |
| **Heroku** | **Not supported** ("We don't support logical replication from external instances") | — | — | — | https://help.heroku.com/TVS8OHTR/ |
| **Render** | Support ticket; Pro plan and ≥10 GB | Dedicated role | Only Render support can run it | [unverified] | |
| **Tiger Cloud** | **Not supported as a publisher** [3rd-party] | — | — | — | |

### Launch-support tiers

- **Tier 1** (self-serve, tested in CI): RDS, Aurora, Cloud SQL, Azure Flexible Server, Supabase,
  Neon (report progress well within its 40-hour slot timeout).
- **Tier 2** (preflight check plus docs): AlloyDB, PlanetScale Postgres, Crunchy Bridge, Render,
  DigitalOcean (experimental).
- **Unsupported at launch:** Heroku, Tiger Cloud. Offer dump/restore or query-based sync later.

### Design implications

1. **Never depend on `FOR ALL TABLES`.** Generate an explicit `FOR TABLE` list, created by the
   table owner. Watch for new tables and `ALTER PUBLICATION … ADD TABLE`.
2. **Preflight every table before publishing it.** Refuse tables with no primary key or replica
   identity, or set RI FULL only with consent. Also detect large objects, unlogged tables,
   foreign tables and matviews, RI FULL on types without an operator class, and generated columns.
3. **Use `failover = true` on PG 17+ sources.** Monitor retained WAL and lag, and re-snapshot
   automatically if a provider drops the slot (Azure, Neon, `max_slot_wal_keep_size`).
4. **Handle DDL ourselves.** Poll the source schema, apply additive changes first, and pause on
   destructive ones.
5. **Cutover:**
   - `setval` the sequences from the source (required).
   - Confirm zero conflicts in `pg_stat_subscription_stats`.
   - Optionally set up reverse replication for rollback.
6. **Feed vector-shape re-embedding from applied changes**, using the source-hash rule above.

## Not verified

- `FOR ALL TABLES` on Aurora, Azure, Supabase, Neon and AlloyDB.
- Failover-slot behaviour on Aurora, AlloyDB, Supabase, Crunchy, Render, and RDS / Cloud SQL
  standard HA.
- DigitalOcean and Tiger Cloud status in 2026.
- Dollar rates for Elastic EIS, Cortex and Cohere.
- PG 18 availability dates per provider.
