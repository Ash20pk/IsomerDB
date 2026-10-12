# IsomerDB — Product Requirements Document

| | |
|---|---|
| **Product** | IsomerDB |
| **Status** | Draft v4 — renamed from Prism DB to IsomerDB; "shapes" renamed to projections; open questions decided (see §15) |
| **Date** | 11 October 2026 |
| **Owner** | Aswin |
| **Source** | System design, draft 3 (`reference/system-design-draft-3.html`) |
| **Research** | `research/01`–`04` (naming, DDL and tokens, embeddings and attach sources, pricing / partners / NFRs) |

> **One database. Write once, read in any projection.** SQL tables stay the single source of truth.
> Documents, search, cache, vectors and analytics are declared *projections* that IsomerDB builds,
> shards, keeps consistent, and can prove correct. Adoption starts by attaching to the Postgres a
> team already runs, with no migration.

---

## 1. Problem

Products start on Postgres. As reads grow, teams bolt on MongoDB for nested documents,
Elasticsearch for search, Redis for hot keys and a vector store for embeddings. Every extra store
needs a sync pipeline (typically Debezium → Kafka → custom consumers), and every pipeline is a place
where data silently goes missing or stale.

What that costs a team today:

- **Unknown lag.** Drift between stores is discovered when a customer reports it.
- **Schema changes multiply.** One column change means editing every store and every pipeline.
- **No read-your-writes.** No transaction spans the stores, so a user can write and then not see it.
- **Operational load.** Five systems to run, back up, secure and page on, and four sync jobs.

### Who feels it

- Teams already running Postgres **plus** MongoDB, Elastic or a vector store, with CDC in between.
- Products whose read patterns (nested documents, search, feeds, recommendations) look nothing like
  how the data is written.
- Anyone who has debugged "search results don't match the database" in production.

### Who does not, yet (out of target)

- Small teams on a single Postgres using JSONB, built-in full-text search and pgvector — the
  "just use Postgres" approach already serves them.
- Teams using Redis for what only Redis does well (queues, pub/sub, rich data structures), not as a
  cache.

### Market signals (as reported, October 2026)

- MongoDB revenue +30% YoY to $771.8M (Q2 FY2027) while Postgres also grows — mixed stacks are the
  norm, not a phase.
- Databricks bought Neon (~$1B); Snowflake bought Crunchy Data (~$250M).
- Supabase reported ~$10.5B valuation (June 2026), a $150M round and the Turso acquisition.
- Supabase and Neon each report 70–80% of new databases are created by AI agents — apps are spun up
  fast and nobody hand-tunes them.
- A 2025 Stifel poll of 25 MongoDB customers found Postgres unlikely to displace MongoDB because
  they serve different workloads — supporting a product that serves both.

---

## 2. Vision and positioning

IsomerDB replaces the polyglot stack and its pipelines with one cluster, one log and one backup.
Teams model data once, normalized, in SQL. They declare the other forms it must be readable in, and
the database maintains them incrementally on every write.

**What we sell is the combination**, not any one piece:

1. A **relational source of truth** with full SQL, joins and `GROUP BY`.
2. **MongoDB wire compatibility**, so document-style apps connect unchanged.
3. **Projections maintained across shards**, with per-projection freshness and read-your-writes.
4. **Attach mode**, so adoption starts without a migration.

**Signature capabilities** that make the combination hard to copy:

- **Proven projections** — a compile-time correctness certificate, a live production auditor, and public
  Jepsen results.
- **Automatic projections** — the database watches its own query log and proposes the read models it
  should maintain.

---

## 3. Goals and non-goals

### Goals

| # | Goal |
|---|---|
| G1 | A team can retire a MongoDB read model, an Elastic sync job or an embedding pipeline without changing how it writes data. |
| G2 | Every projection has a declared, monitored freshness, and every read can report exactly what data it reflects. |
| G3 | Projection correctness is demonstrable: certified at compile time, audited continuously, tested publicly. |
| G4 | Adoption requires no migration: attach to an existing Postgres first, cut over to primary later. |
| G5 | Horizontal scale through entity-key sharding, with most joins staying on one shard. |
| G6 | A managed cloud that customers pay for. |

### Non-goals (v1)

- A full drop-in MongoDB replacement. Projections are the main document read path; operator coverage is
  published as a compatibility matrix, not promised as 100%.
- Replacing Redis for non-cache workloads (queues, pub/sub, streams).
- A general-purpose streaming engine competing with Materialize/RisingWave on arbitrary streams.
- Analytics (columnar) projections, live/event projections, time travel, edge projections and projections-as-API — all
  on the roadmap after Phase 3, not in v1 scope.
- A fork of Postgres. IsomerDB ships as extensions and services on stock Postgres 18.

---

## 4. Personas

| Persona | Context | What they need from IsomerDB |
|---|---|---|
| **Platform / data engineer** | Owns the CDC pipelines between Postgres, Mongo, Elastic and a vector store. | Fewer systems, no sync jobs, lag visibility, one backup and one failover story. |
| **Application developer** | Writes product features; reads documents, search results and "similar items". | Read projections through existing MongoDB or Postgres drivers; read-your-writes after a mutation. |
| **Engineering lead / CTO** | Decides on the stack; worried about migration risk and data correctness. | Start without a migration; evidence the data is correct; a clear cutover path. |
| **AI-feature builder** | Adding retrieval or assistants to an existing app. | Embeddings that match the database right now, without a separate pipeline or vector store. |
| **On-call / SRE** | Pages when things drift or lag. | Per-projection lag budgets, drift alerts that pin the bad input, per-projection write cost. |

---

## 5. Key concepts

- **Table** — ordinary, normalized SQL; the single source of truth.
- **Projection** — a declared alternative read form of table data, built and maintained incrementally by
  the database. Types: `DOCUMENT`, `SEARCH`, `VECTOR`, `CACHE` (later: analytics, live, event).
- **Projection key** — the key a projection is addressed by; co-located with the shard key where possible.
- **Freshness tier** — declared per projection:
  - `sync`: updated inside the writing transaction; readers never see a projection that disagrees with
    its tables. Only allowed when all sources are co-located on the projection key (or are reference
    tables) and IsomerDB is the primary.
  - `bounded '<duration>'`: maintained asynchronously from the replication stream with a stated lag
    budget, measured continuously and alerted on.
- **Write token** — the commit position (LSN) returned on write. A read that passes it waits until
  the projection has caught up to that position, or returns a clear "not yet" error.
- **Attach mode** — IsomerDB follows an existing Postgres via logical replication; all writes stay
  on the customer's database; all projections run `bounded`.
- **Primary mode** — IsomerDB is the database; unlocks `sync` projections, read-your-writes and sharding.

### Example

```sql
-- Model once, sharded by customer
SELECT isomer.distribute('customers', 'orders', 'order_items', by => 'customer_id');

CREATE PROJECTION customer_profile AS DOCUMENT
  KEY c.id
  FROM customers c
  NEST orders o ON o.customer_id = c.id
    NEST order_items i ON i.order_id = o.id
  FRESHNESS sync;

CREATE PROJECTION order_search AS SEARCH
  FROM orders o JOIN customers c ON c.id = o.customer_id
  INDEX (c.name, o.status, o.placed_at)
  FRESHNESS bounded '2s';

CREATE PROJECTION product_vectors AS VECTOR
  FROM products p
  EMBED (p.title || ' ' || p.description)
  USING 'any-embedding-model'
  FRESHNESS bounded '30s';
```

```js
// Any MongoDB driver, unchanged application code
db.customer_profile.findOne({ _id: 42 })  // one prebuilt document, one shard, no joins
```

*`CREATE PROJECTION` is documentation notation. Postgres has no parser hook, so an extension cannot add
a new statement. The same declaration is written in **native Postgres syntax**: a materialized
view whose table access method carries the freshness tier, and a unique index that carries the
key. `NEST` is written as ordinary JSON aggregation (see FR-S1 and §15):*

```sql
CREATE MATERIALIZED VIEW customer_profile USING isomer_sync AS
  SELECT c.id,
         jsonb_build_object('name', c.name, 'tier', c.tier,
           'orders', (SELECT jsonb_agg(...) FROM orders o WHERE o.customer_id = c.id)) AS doc
  FROM customers c;
CREATE UNIQUE INDEX ON customer_profile (id);   -- the projection key

-- The function API is equivalent, and the fallback where DDL is awkward:
SELECT isomer.create_projection('customer_profile', $$ SELECT … $$,
       kind => 'document', key => 'id', freshness => 'sync');
```

---

## 6. User stories

**Attach and adopt**
- US-1 As a platform engineer, I can point IsomerDB at my existing Postgres and have it replicate my
  tables without touching my write path.
- US-2 As a developer, I can move reads to IsomerDB one endpoint at a time.
- US-3 As an engineering lead, I get a cutover plan from attach to primary in my first week.
- US-4 As a platform engineer, I can cut over to primary in a short window using the replica IsomerDB
  already holds, and keep the old Postgres as a fallback for a period.

**Projections**
- US-5 As a developer, I can declare a document projection over joined tables and read it as a MongoDB
  collection with my existing driver.
- US-6 As a developer, I can declare a search projection over joined fields and query it with BM25
  ranking from SQL.
- US-7 As an AI-feature builder, I can declare a vector projection that re-embeds only rows whose source
  text changed, and query similarity from SQL.
- US-8 As a developer, I can declare a cache projection for hot keys and counters.
- US-9 As a developer, when I add a column to a table, every projection that selects it picks it up
  without me editing a pipeline.

**Freshness and correctness**
- US-10 As a developer, a write returns a token, and passing it on a later read guarantees I see my
  own write (or get an explicit "not yet").
- US-11 As an SRE, I see each projection's lag against its declared budget and am alerted when it breaks.
- US-12 As a lead, I can see a running drift counter (e.g. *drift 0 rows · 4.1M checks · 30 days*)
  for every projection.
- US-13 As a developer, a projection the compiler cannot maintain correctly is rejected with a reason,
  rather than accepted and silently wrong.

**Automation**
- US-14 As a developer, IsomerDB proposes projections for my repeated join-and-nest queries, with
  estimated write cost and read speedup, and I accept with one click.
- US-15 As a developer, queries are routed onto a projection only when it is predicted to be faster;
  otherwise they run against tables.
- US-16 As a platform engineer, the shard advisor tells me which queries cross shards, which tables
  should be reference tables, and offers an online reshard plan.

---

## 7. Functional requirements

Priority: **P0** = required for the phase it ships in, **P1** = should, **P2** = could.

### 7.1 Attach mode (Phase 0)

| ID | Requirement | Pri |
|---|---|---|
| FR-A1 | Follow an external Postgres via logical replication, read-only from IsomerDB's side. | P0 |
| FR-A2 | Land replicated rows as sharded tables in the IsomerDB cluster. | P0 |
| FR-A3 | Initial snapshot + continuous catch-up, with replication lag exposed as a metric. | P0 |
| FR-A4 | Handle source schema changes ourselves, since DDL is not replicated: poll the source catalog, apply additive changes automatically, pause and alert on destructive ones. | P0 |
| FR-A5 | All projections in attach mode are forced to `bounded`; declaring `sync` is rejected with a reason. | P0 |
| FR-A6 | Generate a per-customer cutover plan to primary mode. | P1 |
| FR-A7 | Never depend on `FOR ALL TABLES` (needs superuser on most providers). Publish an explicit `FOR TABLE` list, and add new tables with `ALTER PUBLICATION … ADD TABLE`. | P0 |
| FR-A8 | **Preflight every table before publishing.** Refuse tables with no primary key or replica identity, because publishing one makes the customer's own UPDATE/DELETE fail. Flag large objects, unlogged and foreign tables, matviews, `REPLICA IDENTITY FULL` on types without an operator class, and generated columns. | P0 |
| FR-A9 | Slot safety on the customer's primary: alert on retained WAL, honour `max_slot_wal_keep_size`, send heartbeats, use `failover = true` on PG 17+ sources, and re-snapshot automatically if the provider drops the slot (Azure under storage pressure, Neon after ~40 h idle). | P0 |
| FR-A10 | Cutover: copy sequence values (`setval`), confirm zero conflicts in `pg_stat_subscription_stats`, optional reverse replication for rollback. | P0 (Phase 2) |
| FR-A11 | Launch provider support. **Tier 1** (self-serve, CI-tested): RDS, Aurora, Cloud SQL, Azure Flexible Server, Supabase, Neon. **Tier 2** (preflight plus docs): AlloyDB, PlanetScale Postgres, Crunchy Bridge, Render, DigitalOcean (experimental). **Unsupported:** Heroku, Tiger Cloud (neither allows external logical replication). | P0 |

### 7.2 Projection DDL and compiler

| ID | Requirement | Pri |
|---|---|---|
| FR-S1 | Function API is the canonical interface: `isomer.create_projection / alter_projection / drop_projection` for `document` and `search` (Phase 0), `vector` (Phase 1), `cache` (Phase 2). It works in every migration tool's raw SQL and on stock Postgres. Option names are aligned with pg_trickle's. | P0 |
| FR-S1a | **Native DDL path:** `CREATE MATERIALIZED VIEW <name> USING isomer_sync \| isomer_bounded AS <query>`. Isomer ships these table access methods (a thin wrapper that delegates storage to heap). `ProcessUtility_hook` recognises the access method and registers the projection through FR-S1. The kind is inferred: a jsonb document column means document, a `bm25` index means search, `isomer.embed()` in the select list means vector. The key is a `UNIQUE` index. A bounded lag budget comes from a `SECURITY LABEL FOR isomer`, falling back to the cluster default. `REFRESH MATERIALIZED VIEW` triggers a full rebuild. Every piece is standard syntax that ORMs and migration tools run unchanged, and that pg_dump reproduces. | P1 (pending spike, §15) |
| FR-S1b | Projection definitions live in an extension catalog registered with `pg_extension_config_dump`, with `isomer.dump_projections()` / `isomer.restore_projections()`, so projections survive pg_dump/restore. | P0 |
| FR-S2 | Compiler validates the declared freshness tier against co-location and mode; rejects what it cannot honour, with an actionable reason. | P0 |
| FR-S3 | Incremental maintenance touches only rows affected by a change. | P0 |
| FR-S4 | Projections recompile automatically when a source table's schema changes. | P0 |
| FR-S5 | Compiler emits a correctness certificate naming the DBSP rules applied and the assumptions relied on (co-location, freshness tier). Uncertifiable projections are rejected. | P0 (Phase 3) |
| FR-S6 | Downgrade a projection from `sync` to `bounded` with one statement. | P1 |
| FR-S7 | Per-projection cost model picks the maintenance strategy. | P1 |

### 7.3 Projection types

| ID | Requirement | Pri |
|---|---|---|
| FR-D1 | **Document**: nested JSON/BSON per key, `NEST` across co-located tables, readable as a MongoDB collection. | P0 |
| FR-Q1 | **Search**: BM25 full-text index over joined fields, queried from SQL (`@@@`). | P0 |
| FR-V1 | **Vector**: embed an SQL expression with a named model. Each row stores a `source_hash` of the expression, and only rows whose hash changed are queued for re-embedding. | P0 (Phase 1) |
| FR-V1a | Embeddings run in a **separate, horizontally scaled worker service** fed by a durable queue, never as HTTP calls from Postgres backends. Batches are packed by token count; per-key token buckets enforce RPM/TPM; 429s honour `Retry-After` with jittered backoff; jobs go to a dead-letter queue after N failures. Writes are idempotent on (pk, source_hash). Backfills use provider Batch APIs. | P0 (Phase 1) |
| FR-V1b | Providers: **bring your own key** (OpenAI, Voyage, Cohere, any OpenAI-compatible endpoint), with keys referenced from a secret store and never kept in the catalog. **Bundled models** (one cheap, one quality) metered per token at list price plus a small markup, with a one-time free allowance. Optional in-cluster CPU model (gte-small / bge-small) for the free tier, dev and air-gapped use. No GPU fleet until bundled volume passes ~30B tokens/month. | P0 (Phase 1) |
| FR-V2 | Vector: estimate tokens and cost before a backfill; per-projection monthly caps that pause rather than fail; usage split into backfill / steady state / query. | P0 (Phase 1) |
| FR-V3 | Vector: the projection pins model, version and dimensions. Changing the model creates a new projection version that re-embeds in the background, then swaps atomically. | P1 |
| FR-V4 | Vector: `isomer.embed('<text>')` helper for similarity queries. | P0 (Phase 1) |
| FR-C1 | **Cache**: precomputed key → value for hot reads and counters. | P0 (Phase 2) |

### 7.4 Read and write paths

| ID | Requirement | Pri |
|---|---|---|
| FR-R1 | Postgres wire access to tables and projections (any Postgres driver/ORM). | P0 |
| FR-R2 | MongoDB wire gateway maps collection names to document projections; key reads route to exactly one shard. | P0 |
| FR-R3 | Writes return a write token: the **Citus cluster-clock HLC** of the commit, not a raw per-shard LSN. A single scalar is required because MongoDB's `afterClusterTime` holds only one Timestamp. Each shard keeps a per-projection watermark ("applied up to HLC X"), advanced by heartbeats on idle nodes. | P0 (Phase 2) |
| FR-R4 | Reads accept a token and wait until the relevant watermarks reach it, or return a "not yet" error after a timeout. SQL: `isomer.last_write_token()`, the `isomer.read_after` GUC (`SET LOCAL` under transaction-mode PgBouncer), and `isomer.wait_for(token, timeout)`. `sync` projections need no token. | P0 (Phase 2) |
| FR-R4a | **MongoDB gateway.** On writes, return `operationTime` and `$clusterTime` encoded from the HLC (`t = ms/1000`, `i = (ms%1000)<<22 \| counter`; lossless). Replace DocumentDB's current `IllegalOperation` rejection of `afterClusterTime` **in the same change**, otherwise existing causal sessions start failing. Standard causal sessions then work unchanged. With a flag, the gateway also treats a client's gossiped `$clusterTime` (advanced only by its own writes) as an implicit token, giving read-your-writes with no app changes even on implicit sessions. | P0 (Phase 2) |
| FR-R5 | Projection reads can return the log position their rows were derived from (per-read freshness). | P0 (Phase 2) |
| FR-R6 | `sync` projections update in the writing transaction, on the same shard, with no network hop. | P0 (Phase 2) |

### 7.5 Primary mode and sharding (Phase 2–3)

| ID | Requirement | Pri |
|---|---|---|
| FR-P1 | Cutover from attach to primary using the replica already held. | P0 |
| FR-P2 | `isomer.distribute(...)` to shard tables by an entity key with co-location. | P0 |
| FR-P3 | Reference tables replicated to every shard. | P0 |
| FR-P4 | Cross-shard queries run as parallel scatter-gather. | P0 |
| FR-P5 | Online shard rebalancing without downtime. | P0 (Phase 3) |
| FR-P6 | Cross-shard projections maintained by the DBSP-based projection engine, with certificates. | P0 (Phase 3) |

### 7.6 Correctness

| ID | Requirement | Pri |
|---|---|---|
| FR-K1 | **Auditor**: continuously sample projection rows, recompute from tables, diff. | P0 (Phase 2) |
| FR-K2 | Publish a per-projection drift counter in the console. | P0 (Phase 2) |
| FR-K3 | Any non-zero drift pages on-call and pins the offending input for reproduction. | P0 (Phase 2) |
| FR-K4 | Jepsen report for every release, published permanently. | P0 (first in Phase 3) |

### 7.7 Automatic projections and advisor (Phase 3)

| ID | Requirement | Pri |
|---|---|---|
| FR-M1 | **Proposer** mines the query log and groups repeated join-and-nest patterns. | P0 |
| FR-M2 | Each proposal shows estimated write cost and read speedup. | P0 |
| FR-M3 | One-click accept; optional policy to auto-accept below a cost threshold. **Auto-accept is off by default.** | P0 |
| FR-M4 | Planner routes a query onto a projection only when a model predicts a speedup; otherwise falls back to tables. | P0 |
| FR-M5 | Projections with no reads over a quiet period are flagged for removal. | P1 |
| FR-M6 | **Shard advisor** reports cross-shard hot queries and the key that would keep them local, reference-table candidates, and projections that could move `bounded` → `sync` with co-location. | P0 |
| FR-M7 | Shard advisor produces a one-click online reshard plan. | P1 |

### 7.8 Observability and console

| ID | Requirement | Pri |
|---|---|---|
| FR-O1 | Per-projection lag vs declared budget, with alerts on breach. | P0 (Phase 1) |
| FR-O2 | Per-projection write cost and embedding cost. | P0 (Phase 1) |
| FR-O3 | Console and CLI for projections, proposals, advisor output, drift and lag. | P0 |

### 7.9 Managed cloud (Phase 3)

| ID | Requirement | Pri |
|---|---|---|
| FR-CL1 | Provisioning, failover, backups, upgrades. | P0 |
| FR-CL2 | Usage metering for billing. | P0 |
| FR-CL3 | Operator built on CloudNativePG. | P0 |

---

## 8. Non-functional requirements

The numeric targets are starting points drawn from public benchmarks (`research/04`). All the
benchmark numbers are vendor or project claims, and pg_trickle is pre-1.0. **Before publishing
any target, benchmark pg_trickle IMMEDIATE ourselves** (pgbench with 0, 1, 3 and 10 projections, plus
joins), along with ParadeDB index maintenance under write load.

| Area | Requirement |
|---|---|
| **Correctness** | Zero tolerated drift. Any drift detected by the auditor is a Sev-1 defect. Correctness work (auditor, certificates, Jepsen) is funded **before** GA, not after. |
| **Sync write overhead** | Added commit latency p50 ≤ 1 ms, p99 ≤ 5 ms for single-row DML touching ≤ 3 sync projections; write amplification ≤ 2.5×. Projections that would need exclusive locks are forced to `bounded` at definition time. (Baselines: pg_trickle 20–55 µs per row; pg_ivm ~10×; Oracle ON COMMIT ~2×.) |
| **Bounded lag (primary)** | p50 ≤ 250 ms, p99 ≤ 2 s; search and vector projections p99 ≤ 5 s. (Baseline: Materialize p99 1.3–1.4 s; Debezium → Elasticsearch typically 1–5 s p50, and minutes at p99 under backfill.) |
| **Attach lag** | p99 ≤ 5 s from source commit to projection visibility; initial snapshot of 100 GB in ≤ 2 h. |
| **Freshness reporting** | Lag is measured per projection and per read, not as a cluster-wide number. |
| **Read latency** | Key reads of a document projection are single-shard with no read-time joins. |
| **Availability** | 99.95% for HA clusters at launch (matches Neon and RDS Multi-AZ); 99.99% at GA (PlanetScale, Aurora). No SLA on free or single-node tiers. |
| **Freshness SLA** | A projection counts as unavailable when its lag exceeds 60 s. No competitor offers a freshness SLA, so this is a differentiator. |
| **Compatibility** | Stock Postgres 18; any Postgres driver/ORM; MongoDB drivers for Node, Python, Go, Java. Published MongoDB compatibility matrix. |
| **Security** | Attach replication is read-only against the customer's database. Standard encryption in transit/at rest and role-based access for the managed cloud *(details TBD)*. |
| **Testing** | Jepsen + Rust property tests + deterministic simulation. The test suite must not consist only of tests written alongside the code they check. |
| **Licensing** | IsomerDB's own code is AGPL-3.0-only; the client SDK, CLI and console are Apache-2.0 (§15, decision 9). The managed control plane is not distributed and stays closed. |

---

## 9. Architecture summary

Roughly three quarters of the running system is reused open source; the new quarter — projections,
attach and the cloud — is the product's value.

| Layer | Components | Status |
|---|---|---|
| Clients | Postgres drivers; MongoDB drivers; Console & CLI (TypeScript) | Reuse / new console |
| Gateways | Postgres wire (Citus coordinator, PgBouncer); MongoDB wire (DocumentDB gateway in Rust + projection routing) | Reuse + extend |
| Coordinator | Citus distributed planner; **`isomer_planner`** (projection routing, freshness tokens; C in Citus hooks) | Reuse + **new** |
| Shard nodes | Postgres 18 + Citus worker, DocumentDB, pg_search (BM25), pgvector (HNSW), **`isomer_ext`** on pg_trickle (sync projections; Rust/pgrx) | Reuse + **new** |
| Beside the data path | **Attach ingest** (Rust), **Projection engine** (Rust/DBSP), **Auditor** (Rust), **Proposer** (Rust + model), **Shard advisor** (Rust), **Control plane** (Go, CloudNativePG operator) | **New** |

### Base choices

| Candidate | Verdict |
|---|---|
| Postgres 18 + Citus 14 | Base |
| DocumentDB (MIT, Linux Foundation) | Base |
| pg_trickle (Apache 2.0, pre-1.0) | Start from for sync projections |
| ParadeDB pg_search (AGPL / commercial) | Include |
| Feldera DBSP (MIT) | Embed as cross-shard projection engine core |
| YugabyteDB | Runner-up (would require a large C++ fork) |
| Multigres, Neki | Watch (possible Citus replacements) |
| pgai vectorizer | Archived — the gap vector projections fill |
| CockroachDB | No (proprietary license) |

**Languages:** Rust for the data path, Go for operations, TypeScript for customer-facing surfaces,
C only at the Citus boundary, Clojure (Jepsen) for correctness testing.

---

## 10. Target use cases

| Use case | Projections used | Value |
|---|---|---|
| **Commerce & marketplaces** | document, search, vector | Orders/inventory stay relational; storefront reads product documents, catalog search and "similar items" from the same tables. |
| **Multi-tenant SaaS** | cache, document (analytics later) | Shard by tenant so joins stay local; dashboards and feeds as precomputed projections; hot tenants move online. |
| **Fintech & ledgers** | document, cache | Ledger stays strictly relational; statements and balances are projections that never drift, with read-your-writes after every payment. |
| **AI features in existing apps** | vector, document, search | Embeddings and per-user context that match the database now, without a separate pipeline or vector store. |

---

## 11. Release plan

Timings are rough estimates for a small team. Each phase's exit test gates the next.

| Phase | Duration | Scope | Exit criterion |
|---|---|---|---|
| **0 · Attach** | ~2 months | One image (PG 18 + Citus + DocumentDB + pg_search + pgvector); attach ingest; bounded document & search projections; projections readable via the MongoDB gateway. | A design partner switches off one MongoDB read model or Elastic sync job. |
| **1 · Vector projections** | ~2 months | `CREATE PROJECTION … AS VECTOR`; re-embed changed rows only, with cost tracking; per-projection lag dashboards and alerts. | A partner retires an embedding pipeline or separate vector store. |
| **2 · Primary mode** | ~3 months | Attach → primary cutover; sharding-aware sync projections on pg_trickle; write tokens and per-read freshness; cache projections; auditor with public drift counter. | A partner moves production writes onto IsomerDB. |
| **3 · Scale & cloud** | ~3 months | Cross-shard projections on DBSP with certificates; proposer and predicted-benefit routing; shard advisor and online reshard; managed cloud beta; first Jepsen report. | Paying customers on the managed cloud. |
| **Later** | — | Time travel (`AS OF`), edge projections, projections as API, analytics (columnar) projections, live projections for client sync, event projections, verified-IVM research. | — |

---

## 12. Success metrics

| Metric | Measured by |
|---|---|
| Sync jobs / external stores retired per customer | Design-partner reports (Phase 0–1 exit tests) |
| Attach → primary conversion rate and time-to-cutover | Customer cohort tracking |
| Drift rows detected by the auditor | Must be 0; any non-zero is an incident |
| Projections within lag budget (% of time, p99) | Per-projection freshness telemetry |
| Proposer acceptance rate; % of accepted projections still read after 30 days | Proposer telemetry |
| Read speedup on routed queries vs table path | Planner telemetry |
| Paying managed-cloud customers; ARR | Billing / metering |

---

## 13. Risks and mitigations

| Risk | Mitigation |
|---|---|
| **Microsoft controls Citus and DocumentDB** and can bundle the same parts (Cosmos DB for MongoDB vCore). | Keep sharding behind our own interface; contribute upstream; track Multigres as a Citus replacement. The moat is the projection layer and attach mode. |
| **Incremental views are a crowded feature** (pg_trickle, Epsio, Feldera…). | Never sell projections alone; sell the four-part combination. |
| **Correctness bugs in projection maintenance** look like silent data loss to customers. | Auditor, certificates and public Jepsen — funded before GA. |
| **Automatic projections cost more than they save.** | Every proposal shows cost and benefit; auto-accept off by default; unused projections flagged for removal. |
| **Attach mode makes us look like another sync tool.** | Price and message attach as step one of the database; every attach customer gets a cutover plan in week one. |
| **AGPL on Citus and pg_search.** Extensions share a backend process, and the FSF treats plugins like that as one combined work. A proprietary extension calling Citus or pg_search internals is at risk; BSL/ELv2 code inside an AGPL combined work may conflict. No public commercial Citus license was found. | Talk only through SQL and hook APIs; counsel to confirm. Favour AGPL or Apache for Isomer-original code, with a closed control plane. ParadeDB's commercial license is available. |
| **ParadeDB Community does not replicate its search index**; HA and read-replica indexes are Enterprise-only. | Decide early: buy the ParadeDB commercial license, build our own WAL-logging (modified AGPL code, so §13 applies), or rebuild search projections from tables on replicas or at failover. |
| **Attach mode harms the customer's primary** (slot WAL bloat, or a table without replica identity breaking their UPDATEs). | FR-A8 preflight and FR-A9 slot safety. This is the fastest way to lose a design partner. |
| **Azure is retiring Cosmos DB for PostgreSQL** in favour of Flexible Server Elastic Clusters (Citus), so its Citus investment continues. | Same mitigation as the Microsoft risk above. |
| **MongoDB compatibility gaps** in DocumentDB. | Publish a compatibility matrix; position projections as the main read path, not a full drop-in replacement. |
| **Terminology.** "Shapes", the working term, is ElectricSQL's flagship concept, and **Databricks acquired Electric on 11 Aug 2026**. | Dropped. We use **projections**, a generic database and CQRS term (ClickHouse and Vertica projections, Marten, KurrentDB) that no single vendor owns. |
| **Write amplification** from too many projections. | Per-projection write and embedding cost in the console; one-statement downgrade `sync` → `bounded`. |
| **Name / trademark.** The working name "Prism DB" was dropped: an active OSS PrismDB (`HafizMMoaz/prism-db`) has nearly the same pitch, and "Prism" is one letter from Prisma (registered mark, sells Prisma Postgres). **IsomerDB** was chosen; the npm, crates.io, PyPI and GitHub names are clear. Singapore government's "Isomer" site builder exists in an unrelated market. | Lawyer's clearance search on IsomerDB (USPTO 9/42, EUIPO, WIPO, common law) before any public use. Register the domain, the package names and the GitHub org early. |

---

## 14. Dependencies

- Upstream projects: Postgres 18, Citus 14, DocumentDB, pg_trickle, ParadeDB pg_search, pgvector,
  Feldera DBSP, CloudNativePG, pgrx.
- Design partners for each phase's exit test (at least one per phase).
- Legal review of AGPL obligations before any managed-cloud offering.
- Embedding-model providers for vector projections (model-agnostic by design).

---

## 15. Open questions: researched answers

The full findings and sources are in `research/`. Items marked [unverified] there still need
confirming.

| # | Question | Answer | Status |
|---|---|---|---|
| 1a | Can we use "Prism DB"? | **No — high risk.** An active OSS "PrismDB" with nearly the same pitch exists (and holds PyPI `prismdb`), the ASPLOS '23 research system PrismDB tops search results, and Prisma (registered PRISMA mark, sells Prisma Postgres) is one letter away. Renamed to **IsomerDB** (candidates in §15.1). | **Decided: IsomerDB**; clearance search still needed |
| 1b | Keep "shapes"? | **No.** It is ElectricSQL's flagship term, and Databricks now owns Electric. Replaced with **projections** throughout this PRD. | **Decided** |
| 2 | DDL surface | Postgres 18/19 have **no parser hook**, so a literal `CREATE PROJECTION` statement needs a fork. **Fix:** declare projections in native syntax, as `CREATE MATERIALIZED VIEW … USING isomer_sync AS …` plus a `UNIQUE` index for the key (§15.2). The function API stays the canonical engine interface. | FR-S1, S1a, S1b; native path pending a spike |
| 3 | Embedding execution and billing | No one runs inference in a Postgres backend in production (pgai tried it, moved out, then was archived in May 2026). Use an **external worker service**, **bring your own key by default**, plus **bundled metered models** (MongoDB's model), an optional CPU model for free tier, and no GPU fleet below ~30B tokens/month. | **Decided** → FR-V1a, V1b, V2, V3 |
| 4 | Attach-mode sources | **Tier 1:** RDS, Aurora, Cloud SQL, Azure Flexible, Supabase, Neon. **Tier 2:** AlloyDB, PlanetScale, Crunchy, Render, DigitalOcean. **No:** Heroku, Tiger Cloud. Never rely on `FOR ALL TABLES` (superuser). Preflight tables, and watch slots. | **Decided** → FR-A7–A11; some provider details unverified |
| 5 | Write-token transport (Mongo) | Use the **Citus HLC cluster clock** encoded as MongoDB `$clusterTime` / `operationTime`. Standard causal sessions then work unchanged, and gossip gives zero-change read-your-writes. DocumentDB currently rejects `afterClusterTime`, which must be fixed at the same time. | Resolved → FR-R3, R4, R4a; driver behaviour with a zero signature untested |
| 6 | Pricing and open-core line | Price attach in the **same units as the database** (compute-hours plus GB-month), never per event. Free attach tier; attach and primary as two modes of one SKU; optional 20–30% attach discount; $0–30 entry. **Open:** projection engine, SQL and Mongo reads, single-node attach. **Commercial:** managed control plane, HA for search and vector, multi-node, cutover tooling, PITR, SSO, BYOC. | Recommended; licensing needs counsel |
| 7 | Design partners | Profile: Series A–C SaaS or marketplace, 30–300 engineers, Postgres plus Elastic/Mongo/vector DB kept in sync by Debezium, PeerDB or dual writes. 8-point qualification checklist and sourcing channels in `research/04` (next: PGConf EU, Valencia, 20–23 Oct 2026). | Profile done; **naming actual partners is yours** |
| 8 | NFR targets | Set in §8: sync overhead p99 ≤ 5 ms, bounded lag p99 ≤ 2 s, attach p99 ≤ 5 s, 99.95% → 99.99%, plus a freshness SLA. | Proposed; needs our own benchmarks |
| 9 | License for IsomerDB's own code | **AGPL-3.0-only** for the database (extensions, services, operator); **Apache-2.0** for `clients/`. Reasons: the shipped image is already an AGPL combined work (modified Citus and pg_search share the backend process), so a permissive license on our code would buy users no real freedom while letting a host resell the service without contributing; AGPL is what Citus and ParadeDB chose for the same reason. The SDK is Apache so applications that import it carry no obligations. As sole copyright holder we can still sell a commercial license to companies that ban AGPL; to keep that option, outside contributions need a CLA (inbound license grant) before any are accepted. This moves the open-core line in decision 6: everything in this repository is open, and the commercial offer is the managed cloud plus commercial licenses. | **Decided**; counsel to review the CLA |

Questions 5–8 are accepted as recommended.

### 15.1 Replacement names

Checked 11 Oct 2026: none of these exact names is taken on npm, crates.io or PyPI. The GitHub
count is the number of repositories with the name. **No trademark search has been done on any of
them.**

| Name | Idea | GitHub | Notes |
|---|---|---|---|
| **Isomer** (IsomerDB) | Chemistry: the same atoms in a different arrangement, which is exactly what a projection is | 0 | Singapore's government site builder is called Isomer (different market). **Chosen.** |
| **RefractDB** | Keeps the prism metaphor: one source of light, many outputs | 0 | Nearest name is Refact.ai (a different word) |
| **Kaleido** (KaleidoDB) | One input seen in many forms | 0 | Kaleido (kaleido.io) is an enterprise blockchain platform, so medium risk |
| **Manifold** (ManifoldDB) | One space, many local views | 0 | "Manifold" is widely used (Manifold Markets, others); medium risk |
| **ParallaxDB** | The same object seen from different angles | 1 | Tiny prototype repo plus a 2021 research system |
| **Lensbase** | Lenses over one base | 1 | Mirantis "Lens" is adjacent dev tooling |

Dropped:

- **PolyformDB**: clashes with the PolyForm software licenses.
- **OpticDB**: Optic is an API tool.
- **VantaDB**: the npm and crates names are taken, and Vanta is a large compliance company.
- **LumaDB / HelioDB**: existing repos.

### 15.2 Getting `CREATE` syntax without a fork

The problem is only the *syntax*; projections themselves are unaffected. Postgres already accepts
`CREATE MATERIALIZED VIEW <name> USING <table access method> AS <query>`, and an extension can
ship its own access methods. So projections are declared in grammar Postgres already parses:

```sql
CREATE MATERIALIZED VIEW order_search USING isomer_bounded AS
  SELECT o.id, c.name, o.status, o.placed_at FROM orders o JOIN customers c ON c.id = o.customer_id;
CREATE INDEX ON order_search USING bm25 (id, name, status);                             -- kind: search
SECURITY LABEL FOR isomer ON MATERIALIZED VIEW order_search IS 'lag=2s';             -- lag budget
```

**Spike run locally on Postgres 14.19:**

- `CREATE ACCESS METHOD isomer_document TYPE TABLE HANDLER heap_tableam_handler` followed by
  `CREATE MATERIALIZED VIEW … USING isomer_document AS …` works. The view populates, and
  `pg_class.relam` records the access method.
- `pg_dump` reproduces it: it emits the `CREATE ACCESS METHOD` and
  `SET default_table_access_method = isomer_document` before the view. **The declaration survives
  dump and restore**, which the `WITH (isomer.*)` route cannot do.
- It also confirmed the earlier finding: `WITH (isomer.key = 'id')` fails with "unrecognized
  parameter namespace", so options cannot ride on `WITH`.

**Still to verify before committing:**

- Repeat the spike on Postgres 18.
- Confirm pg_dump keeps `SECURITY LABEL`s (it should, unless `--no-security-labels` is passed).
  That needs our C/pgrx label provider loaded.
- **The Citus question:** Citus does not distribute materialized views. So the view is the
  *declaration*, and Isomer stores the projection rows in a co-located distributed table behind it.
  We need to check that this split is clean, including what `\d` and ORMs show.
- Whether `REFRESH MATERIALIZED VIEW` can be intercepted cleanly.

**Longer term:** propose an upstream extensible-DDL hook. It is not on the critical path, because
the route above needs no change to Postgres.

### Decisions still needed

1. Commission the trademark clearance search on IsomerDB, and secure the domain and package names.
2. ParadeDB HA: buy the commercial license, build our own, or rebuild search projections at failover.
3. Name the Phase 0 design partners against the checklist.

---

## 16. Research grounding

| Work | Idea taken | Where it lands |
|---|---|---|
| DBSP (VLDB 2023) | Mechanical rule turning a query into its incremental form, with a formal equivalence argument. | Projection engine; compile-time certificate. |
| Enzyme (Databricks, SIGMOD 2026) | IVM treating AI/non-deterministic calls as part of the view; cost-model strategy per view. | Vector projections; per-projection maintenance strategy. |
| Vodka (VLDB 2025) | Freshness measured against the data a query actually touched. | Per-read freshness; auditor's definition of "current". |
| Tailwind (2026) | Rewrite onto an accelerator only when a learned model predicts a win. | Predicted-benefit routing. |
| MVGPT (Alibaba, ICDE 2026) | Model forecasting which materialized views a workload needs. | Proposer reference design. |
| AMAZe (2026) | Zero-shot multi-agent index advisor. | Shard advisor structure. |
| Drift-Adapter (2025) | Embedding-model upgrades without full re-encode. | Vector-projection model upgrades. |

**Research agenda** (publish, and hire through it): machine-checked verified incremental view
maintenance; learned co-location / shard-key selection from real workloads.

---

## 17. Glossary

- **IVM** — incremental view maintenance.
- **LSN** — log sequence number; Postgres's commit position.
- **CDC** — change data capture.
- **Co-location** — tables sharing a distribution key stored on the same shards.
- **Reference table** — a small table copied to every shard so joins against it are always local.
- **BM25** — standard full-text relevance ranking function.
- **HNSW** — approximate nearest-neighbour index used for vector search.
