# IsomerDB

One database. Write once, read in any projection.

SQL tables stay the single source of truth. Documents, search, vectors and cache are declared
*projections* that IsomerDB builds, shards and keeps consistent incrementally, readable over the
Postgres and MongoDB wire protocols. Adoption starts by attaching to the Postgres a team already
runs, with no migration.

## Layout

| Path | What it is |
|---|---|
| [`docs/PRD.md`](docs/PRD.md) | Product requirements document |
| [`docs/research/`](docs/research/) | Research behind the PRD's open questions: naming, DDL and read-your-writes tokens, embeddings and attach-mode providers, pricing / design partners / NFR targets |
| [`docs/reference/system-design-draft-3.html`](docs/reference/system-design-draft-3.html) | The system design the PRD is based on (written under the earlier working name "Prism") |
| [`docker/`](docker/) | Base image: Postgres 18 with everything in `extensions/`, plus search and vector indexes |
| [`extensions/isomer/`](extensions/isomer/) | The `isomer` Postgres extension: projections, attach mode, advisors |
| [`extensions/isomer_router/`](extensions/isomer_router/) | C planner hook that routes keyed reads onto projections |
| [`extensions/sharding/`](extensions/sharding/) | Distributed tables, shard placement and online rebalancing |
| [`extensions/documents/`](extensions/documents/) | BSON document storage and the MongoDB wire protocol gateway |
| [`extensions/ivm/`](extensions/ivm/) | Incremental view maintenance: change capture and stream tables that keep projections fresh |
| [`services/embedder/`](services/embedder/) | Rust service that computes embeddings for vector projections: OpenAI-compatible APIs, or bge-small-en-v1.5 on its own CPU |
| [`operator/`](operator/) | Kubernetes operator (Go): `IsomerCluster` resources, online scale up/down with rebalance and drain, certificate authentication between nodes, standbys and failover, backups and point-in-time recovery, rolling upgrades. See [`operator/README.md`](operator/README.md) |
| [`services/api/`](services/api/) | Rust HTTP API: projections by key, time travel, change feeds and live streams (SSE), and the admin endpoints the console and CLI use |
| [`clients/`](clients/) | TypeScript: the API client (`sdk`), the `isomer` command line (`cli`) and the web console (`console`, served by the API at `/console`) |
| [`tests/phase0.sh`](tests/phase0.sh) … [`phase3.sh`](tests/phase3.sh), [`native.sh`](tests/native.sh), [`multinode.sh`](tests/multinode.sh), [`routing.sh`](tests/routing.sh), [`history.sh`](tests/history.sh), [`freshness.sh`](tests/freshness.sh), [`console.sh`](tests/console.sh), [`metering.sh`](tests/metering.sh), [`local_model.sh`](tests/local_model.sh), [`operator.sh`](tests/operator.sh), [`operator_backup.sh`](tests/operator_backup.sh), [`operator_upgrade.sh`](tests/operator_upgrade.sh) | Acceptance tests: Phase 0 (attach, document and search projections), 1 (vector projections), 2 (primary mode: cutover, sharding, sync, read-your-writes, auditor), 3 (proposer, shard advisor, certificates, randomized concurrency) |
| [`scripts/smoke.sh`](scripts/smoke.sh) | Checks every base component works in a running stack |
| [`scripts/test-all.sh`](scripts/test-all.sh) | Every suite on fresh stacks, single-node then the three-node cluster |
| [`docs/engineering-notes.md`](docs/engineering-notes.md) | What we learned building the stack (gateway auth, change capture and sharding quirks) |

## Run locally

```sh
docker compose up --build -d     # Postgres on :5432, MongoDB wire gateway on :10260, HTTP API on :8080
./scripts/smoke.sh

docker compose --profile attach up -d   # adds a stand-in customer Postgres (:5433) and a mock embeddings API
./tests/phase0.sh && ./tests/phase1.sh && ./tests/phase2.sh && ./tests/phase3.sh && ./tests/native.sh && ./tests/routing.sh && ./tests/history.sh && ./tests/freshness.sh && ./tests/local_model.sh
(cd clients && npm ci && npm run build) && ./tests/console.sh && ./tests/metering.sh   # console, CLI, admin API, usage

docker compose --profile cluster up -d  # adds two worker nodes
./tests/multinode.sh                    # joins them, rebalances online, tests across nodes

./scripts/test-all.sh                   # or all of it, on fresh stacks (destroys this project's volumes)

./tests/operator.sh                     # the operator on a local kind cluster (its own kubeconfig)
```

## Using it

```sql
-- Follow an existing Postgres (tables keep their names; nothing is migrated)
SELECT * FROM isomer.attach('shop', 'host=... dbname=... user=... password=...');

-- A document projection, readable as the MongoDB collection shop.customer_profile
SELECT isomer.create_projection(
  name => 'customer_profile', kind => 'document', key => 'id', database => 'shop',
  query => $$ SELECT c.id, jsonb_build_object('name', c.name,
                'orders', (SELECT jsonb_agg(jsonb_build_object('id', o.id, 'status', o.status))
                           FROM orders o WHERE o.customer_id = c.id)) AS doc
              FROM customers c $$,
  sources => '{"customers": "id", "orders": "customer_id"}');

-- Or declare it with ordinary DDL: the access method sets freshness, the unique index the key
CREATE MATERIALIZED VIEW shop.customer_card USING isomer_bounded AS
  SELECT c.id, jsonb_build_object('name', c.name) AS doc FROM shop.customers c;
CREATE UNIQUE INDEX ON shop.customer_card (id);

-- A search projection, BM25-ranked from SQL
SELECT isomer.create_projection(name => 'order_search', kind => 'search', key => 'id',
  query => $$ SELECT o.id, c.name, o.status FROM orders o JOIN customers c ON c.id = o.customer_id $$);
SELECT * FROM order_search WHERE name @@@ 'asha';

-- A vector projection: re-embeds a row only when its text changes
SELECT isomer.create_projection(name => 'product_vectors', kind => 'vector', key => 'sku',
  model => 'text-embedding-3-small', lag => '30 seconds',
  query => $$ SELECT sku, title || ' ' || description AS input FROM products $$,
  sources => '{"products": "sku"}');
SELECT sku FROM product_vectors
ORDER BY embedding <=> isomer.embed_for('product_vectors', 'quiet mechanical keyboard') LIMIT 10;
-- ...or embed on the cluster's own CPU: no API key, no network, no per-token cost
SELECT isomer.create_projection(name => 'listing_vectors', kind => 'vector', key => 'id',
  model => 'bge-small-en-v1.5', query => $$ SELECT id, title AS input FROM listings $$,
  sources => '{"listings": "id"}');

-- Move from attach to primary (copies sequences and defaults; optional rollback replication)
SELECT * FROM isomer.cutover('shop', rollback_conninfo => 'host=... user=... password=...');
SELECT isomer.alter_projection('customer_profile', freshness => 'sync');

-- Shard by entity and keep sync projections on one shard
SELECT isomer.distribute(ARRAY['customers', 'orders']::regclass[], 'customer_id');

-- Read your writes: pass a committed write's token, and reads of projections wait for it
SELECT isomer.last_write_token();                 -- after the write commits
SET isomer.read_after = '<token>';                -- or SET LOCAL, under transaction-mode PgBouncer
SELECT * FROM shop.customer_card WHERE id = 42;   -- waits until the projection has the write
SELECT isomer.wait_for(isomer.last_write_token(), 'customer_profile');   -- or wait explicitly
-- ...or from any MongoDB driver: advance a causally consistent session to the write's time
SELECT * FROM isomer.token_to_mongo_timestamp(isomer.last_write_token());   -- then
--   session.advanceOperationTime(Timestamp({t, i}))
-- MongoDB writes return operationTime and $clusterTime; projection reads return their freshness
-- as operationTime. With ISOMER_IMPLICIT_CAUSAL=1 on the gateway, a client's own gossiped
-- $clusterTime makes its reads of projections wait too, with no session changes.

SELECT * FROM isomer.projection_status;
SELECT * FROM isomer.drift_report;      -- "drift 0 rows · N checks · since ..."
SELECT isomer.certificate('customer_profile');   -- rule, assumptions, live checks
SELECT * FROM isomer.alerts;

-- Let the database suggest projections and shard keys from real traffic
SELECT isomer.propose();  SELECT * FROM isomer.proposals;  SELECT isomer.accept_proposal(1);
SELECT * FROM isomer.shard_advice();

-- Route a slow keyed read onto a projection: the application's SQL doesn't change
SELECT isomer.route_proposal(1);   -- verified on sample keys, enabled only if measurably faster
SELECT * FROM isomer.attachment_status();

-- History: time travel and a change feed (document and cache projections)
SELECT isomer.set_history('customer_profile', '7 days');
SELECT isomer.as_of('customer_profile', '42', now() - interval '1 hour');   -- one document, then
SELECT * FROM isomer.as_of('customer_profile', '2026-10-01 09:00+00');       -- the whole projection, then
SELECT * FROM isomer.history('customer_profile', '42');                      -- every version kept
SELECT * FROM isomer.changes_since('customer_profile', 0);                   -- the feed, in commit order

-- Serve projections over HTTP: an API key acts as a Postgres role, and reads only what it could
-- read from the tables behind the projection
SELECT isomer.create_api_key('storefront', 'storefront_reader');   -- shown once
```

```sh
curl -H "Authorization: Bearer $KEY" localhost:8080/v1/projections/customer_profile/docs/42
curl -H "Authorization: Bearer $KEY" -H "X-Isomer-After: $TOKEN" localhost:8080/v1/projections/customer_profile/docs/42
curl -H "Authorization: Bearer $KEY" "localhost:8080/v1/projections/customer_profile/docs/42?as_of=2026-10-01T09:00:00Z"
curl -H "Authorization: Bearer $KEY" "localhost:8080/v1/projections/customer_profile/changes?after=0&limit=100"
curl -N -H "Authorization: Bearer $KEY" localhost:8080/v1/projections/customer_profile/live   # snapshot, then changes (SSE)
```

## Console and CLI

Operators sign in with an API key for a role in `isomer_admin`:

```sql
CREATE ROLE ops; GRANT isomer_admin TO ops;
SELECT isomer.create_api_key('ops', 'ops');   -- shown once
```

- Console: http://localhost:8080/console/: every projection's lag against its budget, drift
  counters, alerts, proposals, the shard advisor, routes, attachments and events.
- CLI (`clients/cli`, Node 20+):

```sh
(cd clients && npm ci && npm run build)
alias isomer="node $PWD/clients/cli/dist/isomer.mjs"
isomer login --url http://localhost:8080 --key "$KEY"
isomer status          # exits 1 while there are alerts, so it works in scripts and health checks
isomer watch
isomer projection customer_profile
isomer proposals && isomer accept 3 --sync && isomer route 3
isomer audit --json
isomer usage --by day          # metered usage: storage, node-hours, documents written, embeddings, requests
isomer usage --csv > usage.csv # for a billing system (also GET /v1/admin/usage?format=csv)
```

- Postgres: `psql postgres://postgres:isomer@localhost:5432/postgres`
- MongoDB: `mongosh "mongodb://isomer:isomer@localhost:10260/?tls=true&tlsAllowInvalidCertificates=true"`

These are local development credentials only.

## License

Third-party components and their licenses are listed in [`NOTICE`](NOTICE).
