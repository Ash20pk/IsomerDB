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
| [`vendor/`](vendor/) | Upstream code we modify: Citus, DocumentDB, pg_trickle. See [`vendor/README.md`](vendor/README.md) for versions, licenses and the rules for changing it |
| [`docker/`](docker/) | Base image: Postgres 18 + Citus + DocumentDB + gateway + pg_search + pgvector + pg_trickle |
| [`extensions/isomer/`](extensions/isomer/) | The `isomer` Postgres extension: projections, attach mode, advisors |
| [`extensions/isomer_router/`](extensions/isomer_router/) | C planner hook that routes keyed reads onto projections |
| [`services/embedder/`](services/embedder/) | Rust service that computes embeddings for vector projections |
| [`operator/`](operator/) | Kubernetes operator (Go): `IsomerCluster` resources, online scale up/down with rebalance and drain. See [`operator/README.md`](operator/README.md) |
| [`services/api/`](services/api/) | Rust HTTP API: projections by key, time travel, change feeds and live streams (SSE) |
| [`tests/phase0.sh`](tests/phase0.sh) … [`phase3.sh`](tests/phase3.sh), [`native.sh`](tests/native.sh), [`multinode.sh`](tests/multinode.sh), [`routing.sh`](tests/routing.sh), [`history.sh`](tests/history.sh), [`operator.sh`](tests/operator.sh) | Acceptance tests: Phase 0 (attach, document and search projections), 1 (vector projections), 2 (primary mode: cutover, sharding, sync, read-your-writes, auditor), 3 (proposer, shard advisor, certificates, randomized concurrency) |
| [`scripts/smoke.sh`](scripts/smoke.sh) | Checks every base component works in a running stack |
| [`scripts/test-all.sh`](scripts/test-all.sh) | Every suite on fresh stacks, single-node then the three-node cluster |
| [`docs/engineering-notes.md`](docs/engineering-notes.md) | What we learned building the stack (gateway auth, pg_trickle and Citus quirks) |

## Run locally

```sh
docker compose up --build -d     # Postgres on :5432, MongoDB wire gateway on :10260, HTTP API on :8080
./scripts/smoke.sh

docker compose --profile attach up -d   # adds a stand-in customer Postgres (:5433) and a mock embeddings API
./tests/phase0.sh && ./tests/phase1.sh && ./tests/phase2.sh && ./tests/phase3.sh && ./tests/native.sh && ./tests/routing.sh && ./tests/history.sh

docker compose --profile cluster up -d  # adds two Citus worker nodes
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

-- Move from attach to primary (copies sequences and defaults; optional rollback replication)
SELECT * FROM isomer.cutover('shop', rollback_conninfo => 'host=... user=... password=...');
SELECT isomer.alter_projection('customer_profile', freshness => 'sync');

-- Shard by entity and keep sync projections on one shard
SELECT isomer.distribute(ARRAY['customers', 'orders']::regclass[], 'customer_id');

-- Read your writes: wait for a committed write, from SQL...
SELECT isomer.wait_for(isomer.last_write_token(), 'customer_profile');
-- ...or from any MongoDB driver: advance a causally consistent session to the write's time
SELECT * FROM isomer.token_to_mongo_timestamp(isomer.last_write_token());   -- then
--   session.advanceOperationTime(Timestamp({t, i}))

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

- Postgres: `psql postgres://postgres:isomer@localhost:5432/postgres`
- MongoDB: `mongosh "mongodb://isomer:isomer@localhost:10260/?tls=true&tlsAllowInvalidCertificates=true"`

These are local development credentials only.
