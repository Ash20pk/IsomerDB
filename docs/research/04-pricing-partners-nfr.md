# Research: pricing and packaging, design partners, NFR targets

Researched 11 October 2026. Prices come from vendor pages where reachable, otherwise from 2026
aggregators [2nd]. Every benchmark is a **vendor claim** unless stated. Licensing points are a
framing for counsel, not legal advice.

## 1. Pricing and packaging

### Comparable products, 2026

| Product | Billing model | Free / entry | Core license |
|---|---|---|---|
| ReadySet | Priced by cache size; self-hosted plus managed AWS; no public prices | 7-day trial | BSL 1.1, becomes Apache after 4 years |
| Epsio | From $0.488/hr | 30-day trial | Proprietary |
| Materialize | Credits at $1.50; smallest cluster ~$1.13/hr (~$820/mo minimum); storage ~$30/TB-mo | Trial; self-managed Community capped at 24 GiB | BSL |
| RisingWave Cloud | $0.227/RWU-hr; storage $0.0299/GB-mo | 7-day trial | Apache 2.0 core plus commercial Premium |
| Feldera | Sales-led enterprise license | OSS edition | MIT |
| Electric Cloud | **Writes only**: $1 per M writes plus $0.10/GB-mo; reads free | Bills under $5 waived | Apache 2.0 |
| Zero | Hosted zero-cache planned; support | Self-host free | Apache 2.0, pledged never to relicense |
| Neon | $0.106–0.222/CU-hr; storage $0.35/GB-mo; scale to zero | 100 CU-hr free [2nd] | Apache 2.0 |
| Supabase | Pro $25; Team $599 | 500 MB free [2nd] | Apache / MIT |
| PlanetScale Postgres | Per node from $5 (single) / ~$15–30 (3-node HA) | $5 entry | Proprietary |
| MongoDB Atlas | Flex ~$8–30; M10 ~$57/mo | M0 free | SSPL |
| Azure Cosmos DB for PostgreSQL | **Being retired**; replaced by Azure Database for PostgreSQL Elastic Clusters (Flexible Server + Citus) | — | Citus AGPL |
| ParadeDB | Sales-led Cloud / BYOC | Community single-node | AGPL plus commercial license |
| Tiger Data | From $30/mo plus ~$0.177/GB-mo | 30-day trial | Apache plus TSL (bans hosted resale of TSL features) |

**Patterns:**

- Databases charge compute-hours plus GB-month, with a free tier or a $5–30 entry point.
- Streaming and IVM engines also charge compute-hours, but with a $160–820/mo floor.
- Sync products charge per write.
- Nobody charges per seat.

### Key constraint: ParadeDB Community does not replicate its index

ParadeDB's docs say Community "does not physically replicate the ParadeDB index"; HA and
read-replica indexes are Enterprise-only (paradedb.com/docs/operate/deploy/enterprise). A managed
Prism DB with HA search shapes therefore needs one of:

- (a) a commercial ParadeDB license;
- (b) our own WAL-logging, which is modified AGPL code, so §13 applies;
- (c) rebuilding search shapes on replicas or at failover from the SQL source of truth.

This is a build-vs-buy decision to make early.

### AGPL implications (for counsel)

- **When §13 applies.** It is triggered by *modifying* the Program and offering it over a network.
  - Citus staff have said unmodified hosting does not trigger it (lwn.net/Articles/681312).
  - The FSF reads hosted use of modified code as triggering release.
  - There is no case law.
- **Separate extensions.** Postgres extensions are shared libraries in the same backend process,
  and the FSF's plugin FAQ treats that as one combined work. A proprietary extension that calls
  Citus or pg_search internals is at risk; one that talks only through SQL or hook APIs has a
  stronger independence argument, but it is not settled.
- **Commercial licenses.** ParadeDB sells one. No public commercial license for Citus was found
  [unverified].
- **License for our own code:**
  - AGPL: consistent with the stack (ParadeDB's choice).
  - BSL / FSL / ELv2: may conflict inside an AGPL combined work; counsel must check.
  - Apache plus a closed control plane: the Neon / Electric / Zero route, and the simplest.

### Recommendations

1. **Price attach mode as the database, not as sync.**
   - Same units as primary mode (compute-hours plus GB-month of shape storage); never per row or
     per event, which would invite comparison with Debezium, Fivetran and Electric.
   - A free attach tier (one small compute, ~5 GB of shapes, one source).
   - Present attach and primary as **two modes of one cluster SKU**. Cutover keeps the same
     cluster, shapes, endpoints and price, with no migration fee.
   - Optionally discount attach-mode compute 20–30%, so cutover has a financial reason
     ("stop paying RDS").
2. **Entry point $0–30/month**, in line with Neon, Supabase and PlanetScale. Don't anchor to
   Materialize's $800+ floor.
3. **Include shape maintenance in cluster compute**; don't meter reads.
4. **Open-core line:**
   - **Open:** the shape engine, SQL and Mongo-wire reads, single-node attach.
   - **Commercial:** managed control plane, HA/failover for search and vector shapes, multi-node
     orchestration, cutover tooling (promote, verify, rollback), PITR, SSO/audit, BYOC.
   - Charge for operational reliability, not features.

## 2. Design partners

### Public evidence of the pain

| # | Source | Pain |
|---|---|---|
| 1 | Instacart (2025) — https://tech.instacart.com/how-instacart-built-a-modern-search-infrastructure-on-postgres-c528fa601d54, https://www.infoq.com/news/2025/08/instacart-elasticsearch-postgres | Elasticsearch fed from Postgres; denormalised writes for price and inventory; moving to Postgres + pgvector gave a claimed 10× lower write load and ~80% lower storage/indexing cost |
| 2 | Zalando (Dec 2025) — https://engineering.zalando.com/posts/2025/12/contributing-to-debezium.html | Hundreds of Debezium streams; WAL bloat, slot mismatches forcing re-syncs, upgrades blocked by a regression |
| 3 | Notion — https://www.notion.com/blog/building-and-scaling-notions-data-lake | 480 Postgres shards → Debezium/Kafka/Hudi; 480 Fivetran connectors had been a burden |
| 4 | Outschool — https://engineering.outschool.com/posts/journey-to-better-search/ | Postgres → Debezium → Kafka → enrichment → Elasticsearch; keeping two stores in sync, RDS slot problems |
| 5 | Red Hat, DevConf.US 2024 — https://pretalx.com/devconf-us-2024/talk/PQ8M3U/ | "Transitioning from Debezium ElasticSearch to Postgres" |
| 6 | Delhivery via Debezium blog — https://debezium.io/blog/2020/02/25/lessons-learned-running-debezium-with-postgresql-on-rds/ | Postgres **and** MongoDB → Kafka on RDS |
| 7 | Robinhood (Hudi talk) — https://s.apache.org/hudi-robinhood-talk | Slow snapshots of large tables; slots filling disk |
| 8 | Fiber AI / PeerDB — https://www.casestudies.com/company/peerdb | Postgres → ClickHouse for real-time search |
| 9 | INSA Strasbourg / ParadeDB — https://www.paradedb.com/customers | Replaced Elasticsearch |
| 10 | Gunnar Morling — https://www.morling.dev/blog/insatiable-postgres-replication-slot | A stopped slot filled 200 GiB in under two weeks |

Public Postgres + MongoDB postmortems are scarce: the pain shows up in conversations more than in
blog posts.

### Ideal design partner

- **Company:** Series A–C SaaS or marketplace, 30–300 engineers. Below 30 they have no second
  store yet; above ~300 they have a platform team with its own pipeline.
- **Industries:** commerce and marketplaces (catalog, price and inventory search), logistics, B2B
  SaaS with in-app search, edtech, fintech activity feeds, and AI apps running Postgres plus a
  separate vector DB.
- **Stack:** Postgres (RDS, Aurora, Cloud SQL or Neon) plus Elasticsearch/OpenSearch, MongoDB or a
  vector DB, synced by Debezium/Kafka, PeerDB/Sequin or dual writes. One to three people own the
  pipeline.

### Qualification checklist (aim for at least 6 of 8)

1. PG 14+, logical replication permitted, willing to create a publication.
2. At least one secondary read store kept in sync today.
3. A named incident or ongoing cost: drift, reindex jobs, slot/WAL bloat, nightly reconciliation.
4. Read models mostly derivable in SQL.
5. Freshness need between sub-second and a few seconds.
6. About 50 GB – 2 TB of data, and under ~5k rows/s per table.
7. An engineering lead who gives 1–2 h/week and will run attach mode on production read traffic
   within 60 days.
8. A plausible cutover path (no hard dependency on Aurora-only features; acceptable residency).

### Where to find them

- PGConf.dev, PGConf EU (Valencia, 20–23 Oct 2026), POSETTE, PGConf NYC, Confluent Current.
- Debezium Zulip and GitHub issues (people filing WAL and slot bugs).
- Postgres Slack and Discord, r/PostgreSQL, Hacker News "replace Elasticsearch with Postgres"
  threads.
- The ParadeDB, Electric and Sequin communities.
- Job posts asking for "Debezium" plus "Elasticsearch".

## 3. NFR baselines

### In-transaction maintenance overhead

| System | Number |
|---|---|
| pg_trickle IMMEDIATE (pre-1.0) | 20–55 µs per row trigger cost; <5% added DML latency under 1k writes/s; ~2.0–2.3× write amplification (pgxn.org/dist/pg_trickle) |
| pg_ivm | Single-row update with a join IMMV: 15.4 ms. TPC-H lineitem update 19.6 ms vs 2.1 ms (~10×), on a laptop. Exclusive locks for multi-table views. (PGConf.EU 2024) |
| Oracle ON COMMIT fast refresh | Inserts "about a factor of two" slower |

### Freshness and CDC lag

| Path | Number |
|---|---|
| Materialize (Postgres source) | p99 freshness 1.3–1.4 s (vendor bench, materialize.com/docs/reference/performance/) |
| RisingWave | 1 s checkpoint by default |
| Debezium → Kafka → Elasticsearch | No credible public p99 found. Realistically ~1–5 s p50, and seconds to minutes at p99 under backfill (ES `refresh_interval` alone is 1 s) |

### Managed SLAs (from the vendors' SLA pages)

| Vendor | SLA |
|---|---|
| PlanetScale | 99.99% single-region, 99.999% multi-region |
| MongoDB Atlas | 99.995% (M10+) |
| Neon | 99.95% (Business/Scale) |
| RDS Multi-AZ | 99.95% |
| Supabase | 99.9% (Enterprise only) |

### Proposed targets

| Target | Value | Why |
|---|---|---|
| Sync shape commit overhead | p50 ≤ 1 ms, p99 ≤ 5 ms for single-row DML touching ≤ 3 sync shapes; write amplification ≤ 2.5×. Shapes that would need exclusive locks are forced to bounded. | pg_trickle's per-row cost makes this reachable; pg_ivm's 10× and Oracle's 2× show unbounded sync is unacceptable |
| Bounded shape lag (primary mode) | p50 ≤ 250 ms, p99 ≤ 2 s; search/vector p99 ≤ 5 s | Matches or beats Materialize's 1.4 s and clearly beats Debezium + ES |
| Attach-mode lag | p99 ≤ 5 s commit-to-visible; snapshot of 100 GB ≤ 2 h; retained-WAL alerts plus `max_slot_wal_keep_size` and heartbeat | Slot bloat on a customer's primary is the fastest way to lose a partner |
| Availability | 99.95% at launch for HA clusters; 99.99% at GA; no SLA on free or single-node | In line with Neon/RDS, then PlanetScale/Aurora |
| Freshness SLA | A shape counts as unavailable when lag exceeds 60 s | No competitor offers one: a differentiator |

**Before committing publicly:** benchmark pg_trickle IMMEDIATE ourselves (pgbench with 0, 1, 3 and
10 shapes, plus joins) and ParadeDB index maintenance under write load.

## Not verified

- List prices for ReadySet, ParadeDB, Feldera and Zero.
- A credible public p99 for Debezium → Elasticsearch.
- Whether Citus can be commercially licensed.
- Materialize trial length.
- Instacart figures (taken via InfoQ; the original blog returned 403).
