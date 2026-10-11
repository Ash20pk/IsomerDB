# Engineering notes

Things we learned building the stack that are not obvious from the upstream docs. Newest first.

## 2026-10-11 — Phase 1: vector projections and the embedder

`tests/phase1.sh` passes. It runs against `tests/mockai`, an OpenAI-compatible stand-in that
rate-limits every third request and counts every input it embeds. It covers:

- the cost estimate before creating;
- backfill, with one bad input isolated in the dead letter;
- each text sent to the provider exactly once despite 429 retries;
- query-time embedding, with the HNSW index used;
- only changed text re-embedded (a price change embeds nothing);
- deletes;
- spend tracking by phase, plus a monthly cap that pauses and resumes;
- a model upgrade with an atomic swap from 8 to 16 dimensions;
- the network-free test embedder;
- alerts.

How it works:

- **The database decides what needs embedding; the embedder decides how.** Source writes queue
  keys exactly as for document projections. Draining recomputes each key's `input` text and only
  writes an `embed_jobs` row when `md5(input)` differs from the stored embedding's hash.
- **`services/embedder` (Rust) leases jobs** by pushing `not_before` forward (a visibility
  timeout, so a crashed worker's jobs come back). It packs batches by estimated tokens, takes
  steady-state work before backfill, and rate-limits each model with token buckets. Results go
  back through `isomer.embed_complete`, which only writes if the job still has the same text
  hash, so a slow result can never overwrite a newer one.
- **Failures** go through `isomer.embed_failed`: exponential backoff with jitter, the provider's
  `Retry-After` on 429, and the dead letter after 8 attempts. A permanent failure (4xx) on a
  batch is retried item by item, so one bad input doesn't dead-letter its neighbours.
- **`isomer.embed()` answers query-time requests without HTTP in Postgres.** It inserts the
  request over a loopback connection (so the embedder sees it before the caller's transaction
  ends), the embedder answers it first, and the caller polls for the result. This needs READ
  COMMITTED and is checked.
- **Model upgrades.** `set_projection_model` creates the new model's partial HNSW index and
  re-embeds in the background, while the view keeps serving the old model. When every key has a
  new embedding, the drain swaps the view, the catalog and the indexes in one transaction.
- **Storage.** `isomer_vectors.<name>` holds `(key, model, embedding, source_hash)` with one
  partial HNSW index per model, on `embedding::vector(dims)`. The public view casts to the same
  expression, so `ORDER BY embedding <=> …` uses the index.

Gaps:

- **No real provider has been exercised.** The OpenAI-compatible path is tested against the mock
  only. A real key just needs `OPENAI_API_KEY` set on the embedder and
  `isomer.add_embedding_model('text-embedding-3-small', 'openai', 'text-embedding-3-small', 1536,
  endpoint => 'https://api.openai.com/v1', api_key_env => 'OPENAI_API_KEY', price_per_million => 0.02)`.
- **The in-cluster CPU model (gte-small / bge-small ONNX) is not built.** `local-hash-64` is a
  deterministic word-hashing embedder for tests and air-gapped smoke checks, with no semantic
  understanding.
- **Token counts are estimated** at 4 characters per token for batching and estimates. Billing
  uses the provider's reported usage when it gives one.
- **Bundled-model billing** (list price plus markup) is a pricing decision; `price_per_million`
  is whatever is registered.

## 2026-10-11 — Phase 0: attach mode, document and search projections

`tests/phase0.sh` passes against a stand-in customer database (the compose `source` service). It
covers preflight, attach, bounded document and search projections kept fresh from the source,
schema changes, new tables, slot health, the drop guard, and `dump_projections`.

Design decisions this phase forced:

- **Document projections are maintained by key, not by pg_trickle.** pg_trickle refuses
  `IMMEDIATE` (and true incremental) maintenance for any query using `jsonb_build_object` /
  `jsonb_agg`, because Postgres marks them STABLE. Instead, each source table declares which column
  holds the projection key. A row trigger collects the affected keys, and IsomerDB re-runs the
  projection's own query for just those keys and upserts the results through DocumentDB's SQL API.
  This is correct by construction (each document is recomputed from the tables), and the PRD's
  "recompute only the affected document" behaviour. `create_projection` refuses a query that reads
  a table without a key mapping, so no write can go untracked.
- **pg_trickle stays the engine for flat projections** (search today, aggregates later), where its
  differential maintenance works.
- **The extension is SQL/PL-pgSQL for now**, not Rust/pgrx as the PRD planned. Phase 0 is
  orchestration of Postgres features, so SQL was the shortest correct path. Rust comes in where it
  is needed: the embedding worker (Phase 1), the gateway (Phase 2), and the cross-shard engine
  (Phase 3).
- **The native `CREATE MATERIALIZED VIEW … USING isomer_*` path (FR-S1a) is not built.** It needs
  key-mapping inference from the query. The function API is the interface for now.
- **Projections over Citus distributed tables are refused** with a clear message. Attached tables
  are local, and sharding-aware projections are Phase 2 work.

Things that bit us:

- **Replication apply workers run with an empty `search_path`** (Postgres 17+). Every IsomerDB
  function pins `search_path = pg_catalog` and qualifies every name. User queries are
  canonicalised at create time by deparsing a probe view under an empty search path, which
  schema-qualifies every table and function.
- **Apply workers fire only row-level triggers, and only `ENABLE ALWAYS`/`REPLICA` ones.**
  pg_trickle defaults to statement-level, origin-only CDC triggers, so its stream tables silently
  went stale over attached tables (2m48s stale in our first test). Fixed with
  `pg_trickle.cdc_trigger_mode = 'row'` plus a patch that creates its triggers `ENABLE ALWAYS`
  (see `vendor/README.md`).
- **Patching pg_trickle's trigger creation in the wrong place broke it.** Running `ALTER TABLE`
  before pg_trickle records the source in `pgt_change_tracking` sends its own DDL hook looking for
  a buffer under the wrong name. Its DDL tracker also suspends every stream table on a source for
  any `ALTER TABLE` it can't prove harmless, which now includes enabling triggers.
- **`pg_trickle.block_source_ddl` must be off for attach mode.** Otherwise the `ADD COLUMN` we
  replay from the source is refused. pg_trickle then extends its change buffers in place.
- **Citus intercepts DDL on temp objects.** Probe views fail with `schema "pg_temp" does not
  exist` on DROP. IsomerDB's probe functions run with `citus.enable_ddl_propagation = off`.
- **`CREATE SUBSCRIPTION` can run inside a function only with `create_slot = false`.** We create
  the slot ourselves over dblink, as a failover slot on PG 17+ sources. `ALTER SUBSCRIPTION …
  REFRESH PUBLICATION` can't run inside a function at all, so it goes over a loopback dblink
  connection, one health check after the new local table commits.
- **`dblink_exec` refuses row-returning statements**, so slot functions go through
  `isomer._remote_call`.
- **Document keys must keep their type.** `_id: 42` and `_id: "42"` are different documents. Keys
  travel through the queue as text and are cast back to the key column's type before they reach
  DocumentDB. An early version deleted every document it had just written because of this.
- **JSON fidelity.** Documents are built as `jsonb`, so keys come out sorted (jsonb order),
  `numeric` becomes a BSON double (`89.00` → `89`), and timestamps become ISO strings rather than
  BSON dates. Fine for Phase 0; a BSON-native builder is the fix.
- **Projection collections are writable through the gateway.** Nothing stops an app writing to
  them yet. The gateway work in Phase 2 should reject writes to projection collections.

## 2026-10-11 — Base image (Postgres 18.6 + Citus 14.2 + DocumentDB 0.117 + pg_search 0.26.1 + pgvector 0.8.7 + pg_trickle 0.108.3)

`scripts/smoke.sh` passes on a single-node stack (coordinator holds shards), and passes on a
re-run. It covers:

- Citus distributes a table (32 shards).
- pgvector nearest-neighbour search works on the distributed table.
- pg_search BM25 works on the distributed table.
- A pg_trickle `IMMEDIATE` stream table updates in the same transaction as the insert.
- The DocumentDB gateway accepts a `mongosh` insert and read over TLS with SCRAM auth.

What we found along the way:

- **The DocumentDB gateway only supports password-less connections to Postgres.** It rejects
  `PGPASSWORD` and passwords in its config on purpose (`setup.rs`, "Track 1"). A `PostgresHostName`
  that starts with `/` is treated as a Unix-socket directory, so we run the gateway with Postgres's
  socket directory shared through a volume. Its defaults for `PostgresSystemUser` and
  `PostgresDataUser` are the OS user (`root` in a container), so we set both to `postgres`. For the
  managed cloud this means the gateway must sit next to Postgres (same pod), or we add password or
  certificate auth to it.
- **pg_trickle can't drop a stream table whose source table was dropped first.**
  `drop_stream_table` fails with "SpiTupleTable positioned before the start or after the end" once
  the stream table is suspended (`SOURCE_DROPPED` / `SOURCE_RECREATED`). Dropping in the normal
  order (stream table, then source) works. IsomerDB's projection lifecycle must drop the projection
  before the source table, or intercept `DROP TABLE` and do it in that order. Worth reporting
  upstream.
- **Citus warns about pg_trickle's validation views.** Creating a stream table logs `view
  __pgt_validate_output_* has dependency on unsupported object "schema pg_temp_*" … will be created
  only locally`. pg_trickle builds a temp view to validate the query, and Citus declines to
  propagate it. It's harmless, but it is the first sign of the Citus × pg_trickle interaction the
  PRD flags. Stream tables over **distributed** tables are still untested.
- **pg_search 0.26 deprecated `key_field`.** `WITH (key_field = …)` is now a no-op that warns; the PRD §15.2 example has been updated to drop it.
- **"isomer.conf contains errors" at first boot is harmless.** The image's entrypoint runs a
  temporary server during `initdb`, and `listen_addresses` can't change on reload. The real server
  starts cleanly afterwards.
- **No `rum` package exists for PG 18.** DocumentDB uses its own `pg_documentdb_extended_rum`, set by
  `documentdb.rum_library_load_option` and `documentdb.alternate_index_handler_name`.
- **DocumentDB version choice.** We vendored the latest stable, v0.117-0. v1.0-RC2 was published
  on 9 Oct 2026. Re-vendor before we start modifying the gateway, so our changes sit on 1.0.
- **Disk.** A cold build needs roughly 10–15 GB of Docker space (Rust toolchains plus three
  compiles). The first attempt failed because the Mac's disk was full, which left Docker's VM
  read-only until Docker Desktop was force-restarted.
