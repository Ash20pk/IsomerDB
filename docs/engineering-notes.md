# Engineering notes

Things we learned building the stack that are not obvious from the upstream docs. Newest first.

## 2026-10-11 — History, time travel, change feeds, and projections over HTTP (roadmap)

`tests/history.sh` passes (37 checks) on a single node and on the three-node cluster. Isomer 0.8.0
keeps an opt-in change log per projection (`isomer.set_history(name, retention)`), and three
roadmap items read from it:

- **Time travel.** `isomer.as_of(projection, key, at)` returns one document as it was at a moment;
  `isomer.as_of(projection, at)` returns the whole projection; `isomer.history(projection, key)`
  lists every version kept. A moment before the horizon (retention, or when history began) is
  refused with IS002 instead of answering wrongly.
- **Event feeds.** `isomer.changes_since(projection, cursor)` returns every change in commit order,
  with no gaps. `isomer.snapshot(projection)` gives a consistent starting point: the documents and
  the cursor they are as of. A cursor that retention has pruned past fails with IS003 (re-snapshot).
- **Projections as API, and live sync.** `services/api` (Rust, axum) serves key lookup, `?as_of`,
  history, the feed, and `/live` as server-sent events. `/live` opens with a snapshot event, then
  sends change events with the feed id as the SSE id, so `Last-Event-ID` resumes exactly where a
  client stopped. Live delivery is woken by `NOTIFY isomer_changes`, with a one-second poll as a
  fallback (measured in the test at about 20 ms). `X-Isomer-After: <token>` gives read-your-writes
  over HTTP (503 with `Retry-After` if not caught up).

How it is built:

- **One log, written where the projection is written.** `_apply` (the only path that writes
  document and cache projections, for sync and bounded alike) records each key's new document, or a
  delete, in `isomer.changes`. Unchanged documents are skipped, so a no-op update logs nothing.
  Search and vector projections don't keep history: their storage is pg_trickle and the embedder,
  not `_apply`.
- **Commit order is what makes the feed gap-free.** Ids come from a sequence, but are taken while
  holding a per-projection transaction lock, so within a projection they are assigned in commit
  order. Once id N is visible, nothing smaller can still appear. The obvious alternatives don't
  hold: a plain sequence lets a slow transaction commit a smaller id after a reader has passed it,
  and ordering by transaction id breaks per-key order (a transaction that started earlier can
  commit a key's later version). **Cost:** sync writers to a projection with history are
  serialised on that lock. Bounded projections are unaffected, because one drain applies them.
- **Starting history is consistent.** The baseline is taken with the source tables locked in
  SHARE mode, so no write can fall between the baseline and the log.
- **Retention keeps the version at the cutoff.** `prune_history()` (every minute) deletes versions
  superseded before the cutoff, and deletes older than it, so `as_of` still answers at the horizon.
- **The API never widens access.** An API key (`isomer.create_api_key(name, role)`, stored only as a
  SHA-256 hash) acts as a Postgres role. `isomer.can_read(role, projection)` applies the routing
  rule: the role can SELECT every table behind the projection, and none has row-level security. A
  projection the role may not read answers 404, the same as a missing one. Keys cannot act as a
  superuser.

What bit us:

- **Lost document updates on a cluster (a bug that predated history).** The concurrency test
  replayed the feed and found three keys where storage was stale while the log was right, and
  `verify` agreed. The collection's shard lives on a worker, written in a two-phase commit. The
  coordinator commits first, releasing the per-key lock, while the worker's `COMMIT PREPARED` is
  still in flight. The next writer's DocumentDB update then meets a row whose newer version is not
  yet committed, and can match nothing and lose the write. `_apply` now locks the documents
  (`SELECT … FOR UPDATE`, routed to the shard, using the `_id` index) before computing them on a
  multi-node cluster. That waits out any such commit. The single-node phase 3 concurrency test
  could never catch this, because there the collection is local.
- **`wait_for` could return early across nodes (the long-standing multinode flake).** A queued key
  moves from a worker's `node_queue` to the coordinator's `pending`: the pull commits the
  coordinator's copy, then the worker's delete. `_queued_before` checked both in one expression, so
  `pending` was read with a snapshot taken *before* the workers were asked. A key moving in between
  was seen in neither place. A loop of write, `wait_for`, read against a freshly added node missed
  9 of 50. Now the workers are asked first, then `pending` is read in a separate statement whose
  fresh snapshot covers anything the workers no longer hold: 0 of 100. This, not the router, was
  behind the intermittent "a write to a shard moved to worker2" failure.
- **Shard moves onto the coordinator failed authentication.** Citus moves a shard as its table's
  owner, and DocumentDB's collection tables belong to `documentdb_admin_role`, which had no
  password. Any rebalance moving a collection shard to the coordinator retried forever
  ("fe_sendauth: no password supplied"). The init script now gives that role the cluster password
  and adds it to `.pgpass` on every node (dev setup; production uses certificates and
  `pg_dist_authinfo`).
- **`citus_rebalance_start()` is asynchronous.** Polling `citus_rebalance_status()` right after it
  can read "finished" before the job is picked up, and shards then move mid-test.
  `citus_rebalance_wait()` is the reliable wait.
- **Not verified yet:** capture for writes made *during* a shard move. The tests write after a
  rebalance finishes. Citus keeps triggers on moved shards, and the source shard's trigger fires
  for writes before cutover, but we have not tested a write racing the cutover itself.

## 2026-10-11 — Query routing (FR-M4)

`tests/routing.sh` passes (12 checks). An application's unchanged prepared statement (a keyed join
over a 300k-row table without an index) is proposed, routed, and from then on planned as one
index lookup on a sync cache projection. Results are identical on 51 keys, inserts and updates
show up on the next read, and a read takes 0.85 ms against 7.09 ms on the tables. Routing `off`,
`shadow` mode and a disabled route all fall back to the tables.

- **`isomer_router` is a small C library preloaded right after Citus**, with a `planner_hook`.
  - For a `SELECT` whose query id (the same id `pg_stat_statements` and the proposer use) has an
    enabled route, it parses the route's replacement SQL and analyses it with the original's
    parameter types.
  - It checks that the visible output columns have the same count and types, then hands the
    replacement to the next planner (Citus).
  - Routes are cached per backend and reloaded every 2 seconds.
  - `isomer.routing` = `on` / `off` / `shadow`, and `isomer.routing_min_speedup` sets the bar.
- **`isomer.route_proposal(id)`**:
  1. Takes the parameter type from a `PREPARE`, and the output columns from a probe view with
     `$1` replaced by a typed `NULL`.
  2. Creates a **sync** cache projection holding each key's rows as a jsonb array, plus the
     replacement `SELECT x.cols FROM cache CROSS JOIN LATERAL jsonb_to_recordset(...) WHERE key = $1`.
  3. Enables the route only if the replacement returns the same multiset of rows on sample keys
     (including keys with no rows) and its measured speedup clears the bar. This one measured
     19.9×.
- **Routes target sync projections only**, so a routed read sees exactly what the tables hold.

Limits:

- **No `ORDER BY`.** Row order inside the jsonb array isn't the query's order.
- **No duplicate output column names**, which would collide as jsonb keys.
- **One parameter**, the key.
- **"Predicted to win" is a measured speedup at route time**, not a learned model that keeps
  adapting (the PRD cites Tailwind for that).
- **Creating the route's view logs a Citus warning** ("view … has dependency to table … not in
  Citus' metadata"). It is harmless: both are local by design.

### Read-your-writes across nodes: the two-phase commit gap

`multinode.sh` failed now and then at "a write to a key on worker1 is read back after wait_for",
while `pending` and every `node_queue` were already empty. The drain writes the document on the
node holding the collection's shard and deletes the queued key on the coordinator, both in one
distributed transaction. Citus commits the coordinator's side first and then sends
`COMMIT PREPARED` to the workers. In between, the queue is empty but the document update is not yet
visible on the worker. `wait_for` saw the empty queue and returned too early. In multi-node mode it
now takes a snapshot of the prepared-transaction gids (`pg_prepared_xacts` on the coordinator and
on every worker) once the queues are clear. It returns only when none of those gids is still
prepared, and fails closed with IS001 at the timeout. This had nothing to do with the router; the
router only changed the timing enough to expose it. After the fix: 4 out of 4 clean multi-node runs, with the router loaded.

## 2026-10-11 — Multi-node clusters (FR-P5, FR-P6 for key-maintained projections)

`tests/multinode.sh` passes (19 checks) on a coordinator plus two Citus workers, both fresh and on a
rerun. It covers:

- bounded projections maintained from writes on every node, with read-your-writes across nodes;
- a reference-table change re-deriving documents everywhere;
- sync refused across nodes;
- the certificate checking triggers on every shard placement;
- adding a node with Citus's online rebalancer, with capture still working on moved shards;
- three concurrent workers making 360 random transactions across nodes, after which every key
  equals recomputation.

How it works:

- **Citus runs IsomerDB's capture triggers on every shard on every node.** Shard triggers get the
  shard id appended to their name, which is why a plain `pg_trigger` count by name misses them.
  On a worker there is no projection catalog, so the trigger queues the key in
  `isomer.node_queue`, using `*` for "every key" (lookup tables, TRUNCATE).
- **`isomer.pull_node_queues()`** (a procedure, every second, coordinator only) takes each worker's
  queue inside a worker transaction, inserts it into `isomer.pending`, commits locally, then
  commits on the worker. A crash in between duplicates keys (harmless) and never loses them.
- **On a multi-node cluster `wait_for` is a barrier.** A write routed to a worker runs its
  trigger in the worker's session, so the client's session never sees that write's token, and
  tokens from different nodes' clocks don't compare safely anyway. `wait_for` reads every node's
  clock once and waits until no queue holds a change from before then. It fails closed: a worker
  that doesn't answer counts as still having work queued.
- **`isomer.add_node`** joins a node and starts `citus_rebalance_start()`. Moved shards keep their
  triggers; `isomer.rebalance_finished()` re-checks and repairs them.

What bit us:

- **Citus propagates extension DDL to every worker**, so `CREATE` and `ALTER EXTENSION isomer` ran
  IsomerDB's `cron.schedule` calls on the workers too. A worker then ran the pull job itself: it
  took keys from its own queue, found no projection in its empty catalog, and committed the
  delete, silently losing writes. Jobs are now scheduled only `WHERE citus_is_coordinator()`, and
  the pull refuses to run anywhere else.
- **Citus refuses `INSERT … SELECT` whose subquery runs queries over distributed tables**
  ("subqueries are not supported within INSERT queries"). The pull's error handler swallowed it,
  and the test's reset cleared the event log, so the failure was invisible until traced. The pull
  now fetches into a variable, expands `*` keys separately, and inserts from arrays.
- **Dropping a projection's collection with DDL propagation off orphaned Citus metadata.**
  `pg_dist_partition` kept the dropped table, and the next `citus_add_node` failed with "cache
  lookup failed for relation". `_install` and `drop_projection` now always run with propagation
  on; only purely local objects (probe views, vector and cache storage) turn it off.
- **A procedure with a `SET` clause can't `COMMIT`** ("invalid transaction termination"), so the
  pull pins nothing and qualifies every name instead. `LEAST` and `COALESCE` are syntax, not
  functions, so they can't be schema-qualified.
- **Workers must reach the coordinator by name.** `citus_add_node` refuses while the coordinator
  is registered as `localhost`; the image sets it from `ISOMER_COORDINATOR_HOST`.
- **Citus connects to workers as each client's own role**, including a MongoDB user's role, so
  every role needs inter-node auth. The dev compose trusts the private network on workers.
  Production needs certificate auth (`citus.node_conninfo`) or `pg_dist_authinfo`.

Still open:

- **Sync projections across nodes.** A write's documents would have to be written from the worker
  holding the shard.
- **Search projections over distributed tables**, which pg_trickle doesn't support.
- **The barrier `wait_for` waits for everything queued before the call on the given projection**,
  not just the caller's own write. That is correct, but it can wait longer under heavy load.

## 2026-10-11 — Native DDL for projections (FR-S1a)

`tests/native.sh` passes (19 checks). A document projection is declared with ordinary DDL:

```sql
CREATE MATERIALIZED VIEW shop.customer_card USING isomer_bounded AS SELECT c.id, jsonb_build_object(...) AS doc FROM shop.customers c;
COMMENT ON MATERIALIZED VIEW shop.customer_card IS 'isomer {"lag": "3 seconds", "sources": {"shop.order_items": "customer_id"}}';
CREATE UNIQUE INDEX ON shop.customer_card (id);
```

- **`isomer_sync` and `isomer_bounded` are table access methods** that delegate to heap
  (`heap_tableam_handler`), so this needs no C. They only mark freshness; the view is emptied
  (`REFRESH … WITH NO DATA`) after registration, and the MongoDB collection is the readable form.
- **Event triggers do the rest.**
  - `CREATE UNIQUE INDEX` registers the projection (`ddl_command_end`).
  - `COMMENT` changes options, and `ALTER MATERIALIZED VIEW … SET ACCESS METHOD` changes freshness.
  - `DROP MATERIALIZED VIEW` drops the projection (`sql_drop`). It only drops projections the view
    itself declared (`projections.native_view`).
- **It all survives `pg_dump`.** The dump sets `default_table_access_method`, and keeps the comment
  and the index. Replaying the index on a restored catalog re-installs the projection.
- **Source mappings are inferred conservatively.**
  - The key's origin is `alias.key` in the top-level select list, or an unqualified key when the
    top level reads exactly one table. The deparser drops the outer alias, so the top level is
    found by stripping parenthesised subqueries.
  - The key is propagated through equalities. Unconnected tables become lookup tables, which is
    always correct and only slower, with a notice explaining how to map them.
  - The first version fell back to the first `alias.id` it found, mapped `orders` by `orders.id`,
    and would have sent writes to the wrong documents. The fallback is gone: an ambiguous key now
    fails and asks for explicit mappings.
- **Native syntax declares document projections only.** Search, vector and cache projections are
  readable objects named after the projection, which would collide with the view; they keep the
  function API.

## 2026-10-11 — Phase 3 (single-node parts): advisors, certificates, concurrency

`tests/phase3.sh` passes (26 checks), including three back-to-back runs of its randomized
concurrency test. In each run, 4 workers make 600 random transactions with rollbacks and key moves
across a sync projection over shards and a bounded one. Every read-your-writes check holds, and
`isomer.verify` finds every projection equal to recomputation for every key.

What exists:

- **The proposer** (`isomer.propose`, hourly). It reads `pg_stat_statements`, stripping the
  `PREPARE … AS` wrapper that driver-prepared statements carry, for frequent reads of the form
  `… JOIN … WHERE alias.col = $1`.
  - It drafts a per-key document projection: it maps each joined table to the key through the join
    equalities, and canonicalises the draft.
  - It estimates benefit (calls/s × time saved over a ~0.3 ms keyed read) and cost (source
    writes/s × one run of the query, the cost of re-deriving a key).
  - Drafts that can't be mapped are `needs_review`, with the reason.
  - Auto-accept happens only under `proposer.auto_accept_max_write_ms_per_s`, and is off by
    default.
  - `isomer.unused_projections` flags projections with no reads for a quiet period, net of the
    auditor's own scans.
- **The shard advisor** (`isomer.shard_advice`) reports:
  - fan-out joins from `citus_stat_statements`, with the column to co-locate on;
  - small tables that take part in fan-out joins (reference candidates);
  - bounded projections that are sync-ready, or blocked only by co-location.

  `isomer.reshard_plan` and `isomer.reshard` change a shard key with `alter_distributed_table` and
  recreate the shard triggers.
- **Certificates** (`isomer.certificate`) state the maintenance rule and its assumptions, and check
  them live: completeness, capture triggers present and `ENABLE ALWAYS` on every source and shard,
  sync preconditions, pg_trickle health, and the auditor's record. A disabled trigger makes the
  certificate invalid.
- **`isomer.verify`** checks every key, from both sides, where the auditor samples.

Bugs the randomized test found, which every earlier test missed:

- **Sync lost update.** Transaction A renames account 82 while B inserts an event for it. B
  derives the document from a snapshot taken before A commits, waits on the document's row lock,
  then overwrites A's result with stale data. Fixed with a per-key transaction lock (in key order)
  before deriving, so every derivation sees all earlier commits to its key.
- **Bounded missed write.** A write that found its key already queued did nothing; a drain that had
  read the queue just before could then miss it. The conflict now row-locks the queued key (the
  drain skips locked keys until the write commits), and keeps the oldest real token for
  read-your-writes.

Other things that bit us:

- **Citus won't plan correlated subqueries across non-co-located tables.** IsomerDB enables
  `citus.enable_repartition_joins` so bounded projections can join them (as inner joins with
  `GROUP BY`). Sync still requires co-location.
- **Citus's `citus_stat_statements` calls `pg_stat_statements()` unqualified**, so the advisor
  runs with `public` on its search path.
- **All tables with the same key type and shard count share Citus's default co-location group**,
  so "table alone in its group" is not a usable reference-table signal; taking part in fan-out
  joins is.
- **Session statistics are flushed lazily** (about once a second), so a baseline taken in the
  same session as a bulk load doesn't include it.

Not built from Phase 3 yet:

- **Predicted-benefit query routing (FR-M4)** needs a planner hook in C.
- **Multi-node clusters.** Projections over several Citus nodes are still refused, and online
  rebalancing is untested.
- **A DBSP engine for cross-shard projections.** Key-recompute plus pg_trickle cover today's
  cases.
- **A real Jepsen run.** The randomized concurrency test is the stand-in.
- **The managed cloud and its Kubernetes operator.**

## 2026-10-11 — Phase 2: primary mode

`tests/phase2.sh` passes. It covers:

- attach, then cutover to primary, with sequences, identity columns, defaults and rollback
  replication back to the source;
- sync projections inside the writing transaction, including over Citus-sharded, co-located
  tables, with a reference table joined in;
- the co-location refusal;
- cache projections;
- read-your-writes from SQL (`wait_for`), and from an unmodified MongoDB driver through the
  gateway;
- writes to projection collections refused;
- a non-superuser application role;
- the auditor detecting injected drift, pinning it, alerting, and repairing it.

How it works:

- **Write tokens.** Every source change is stamped with the Citus node clock, packed as
  `ms << 22 | counter` (`isomer.current_token()`). `isomer.last_write_token()` returns the session's
  newest one. `isomer.wait_for(token, projection)` waits until no queued change stamped at or
  before it remains (it refreshes search projections on demand), or raises SQLSTATE `IS001`
  ("not yet"). Backfills carry token 0, so they never block a read-your-writes wait.
- **MongoDB read-your-writes needs no driver changes.** The application converts its token with
  `isomer.token_to_mongo_timestamp()` and calls `session.advanceOperationTime(Timestamp(t, i))`
  on a causally consistent session. The driver then sends `afterClusterTime` on every read, and
  the gateway waits on `isomer.wait_for` for reads of projection collections. The test proves the
  wait is real: with draining paused, the causal read fails "not yet" while a plain read returns
  the stale document.
- **Sync over shards.** Citus runs row triggers on shards when they are created with
  `citus.enable_unsafe_triggers = on` and DDL propagation on. A trigger on a shard can query the
  co-located shards and write DocumentDB documents in the same transaction. `_check_sources`
  allows sync only when every distributed source is co-located and sharded by the column that
  maps to the projection key, so every document a write touches lives on the writing shard.
- **Cutover** freezes the source (new sessions read-only), waits for open write transactions,
  waits until the subscription has applied past the source's current LSN, and refuses if
  `pg_stat_subscription_stats` recorded any conflict. It then copies sequences, defaults and
  identity columns, and stops replication. With `rollback_conninfo` it publishes IsomerDB's
  tables, and the next health check subscribes the source to them.
- **The auditor** samples keys from both the tables and the projection's storage, so orphans are
  found too, and compares what the tables produce with what is stored. A mismatch only counts as
  drift if it persists for at least twice the lag budget (minimum 2 seconds), which filters out
  lag. Drift is pinned in `isomer.drift_samples`, logged as an error, raised in `isomer.alerts`,
  and counted in `isomer.drift_report` ("drift 0 rows · N checks · since …").
  `isomer.repair()` re-derives the affected keys.

Things that bit us:

- **Source triggers inherited "no DDL propagation"** from `_install`, so they were created on the
  distributed table's coordinator definition but on no shard, and never fired. Trigger
  creation and dropping now force propagation on.
- **Cutover deadlocked creating rollback replication.** The source's `CREATE SUBSCRIPTION`
  creates a slot on IsomerDB, which waits for every open transaction, including the cutover
  waiting on it. The subscription is now created by the next health check, after cutover commits.
- **A frozen source refuses IsomerDB's own clean-up.** New sessions are read-only, and `SET
  default_transaction_read_only = off` doesn't help inside a multi-statement string (the implicit
  transaction already started read-only). `BEGIN READ WRITE; …; COMMIT` does.
- **Cutover is not atomic across the two databases.** A failure after the remote slot was dropped
  used to roll back only our side. Source clean-up failures are now logged warnings that don't
  undo a finished cutover.
- **Identity columns own their sequences.** Pre-creating them broke `ADD GENERATED AS IDENTITY`,
  so they are excluded from the sequence copy.
- **Trigger functions run as the writing role.** Every earlier test ran as a superuser, so
  ordinary application roles would have failed inside IsomerDB's trigger. `_on_source_change`,
  `wait_for`, `embed` and the gateway lookups are now `SECURITY DEFINER` with pinned search paths.
- **MongoDB drivers misread error code 20** on a write as "retryable writes not supported".
  Refused writes use 166 (`CommandNotSupportedOnView`), as MongoDB does for views.
- **DocumentDB's `collection()` only works at the top level of a query.** The auditor reads
  `documentdb_data.documents_<id>` directly and converts canonical extended JSON to plain values.
- **DocumentDB's distributed planner hook needs schema access for every role** on every Citus
  query, sharded or reference, with or without IsomerDB. Ordinary roles failed with
  `permission denied for schema documentdb_core`. The image grants `USAGE` on `documentdb_core`
  and `documentdb_api_catalog` to `PUBLIC` at first boot.
- **`wait_for` inside the writing transaction can never succeed** (the drain can't see the
  uncommitted change), and used to just time out. It now refuses immediately with a hint to
  commit first.
- **The auditor's first version flagged every document** carrying a fraction: `0.1` stored as a
  BSON double prints back as `0.10000000000000000555`. Both sides are now compared at double
  precision, and the test audits repeatedly before asserting "no drift", which would have caught
  this.
- **The local secret-redaction hook rewrites lines like `token = …`.** It silently replaced code
  in two files (a SQL `SET token = …` and a Rust `let token = …`). Grep for `REDACTED` after
  writing files, and avoid that shape.

Gaps:

- **Per-read freshness is a function** (`isomer.freshness`). The gateway does not attach it to
  responses, and it does not return `operationTime` on writes, since writes go through SQL.
- **`SET isomer.read_after` is not honoured by plain SQL reads**; call `isomer.wait_for` before
  reading. Honouring a GUC needs a planner or executor hook in C.
- **Projections over multi-node clusters are refused** (Phase 3). Everything above was tested on a
  single node whose coordinator holds the shards.

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
