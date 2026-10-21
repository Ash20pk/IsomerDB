# Engineering notes

Things we learned building the stack that are not obvious from the upstream docs. Newest first.

## Upstream code in `extensions/`

Not dated: reference for anyone changing or updating `extensions/sharding`, `documents` or `ivm`.
Public attribution is in the root `NOTICE` file.

| Directory | Project | Version | Commit | Import | License |
|---|---|---|---|---|---|
| `extensions/sharding/` | [Citus](https://github.com/citusdata/citus) | v14.2.0 | `58702e722c6efbae45e3cade885b476e4fddc7e1` | `6366877` | AGPL-3.0 |
| `extensions/documents/` | [DocumentDB](https://github.com/documentdb/documentdb) | v0.117-0 | `28cc012070ce798361ed265251ec159180b7889f` | `85f8a2e` | MIT |
| `extensions/ivm/` | [pg_trickle](https://github.com/trickle-labs/pg-trickle) | v0.108.3 | `ce709f06327b4801f79fc48934bc96107b450907` | `51c92ee` | Apache-2.0 |

DocumentDB v0.117-0 builds against Citus commit `fe25f68`, which v14.2.0 includes (41 commits
ahead, 0 behind).

### Our changes

| Project | Change | Why |
|---|---|---|
| pg_trickle | CDC triggers are created `ENABLE ALWAYS` (`cdc/mod.rs`, `cdc/rebuild.rs`, `api/helpers.rs`) | Logical-replication apply workers only fire `ALWAYS`/`REPLICA` triggers, so stream tables over attached tables never updated |
| pg_trickle | The DDL tracker treats `ENABLE [ALWAYS\|REPLICA] TRIGGER` as harmless (`hooks.rs`) | Otherwise enabling a trigger suspends every stream table on that source |
| pg_trickle | The "logical replication target" warning only fires in statement-level CDC mode (`api/helpers.rs`) | With row-level CDC and `ALWAYS` triggers, replicated changes are captured |
| DocumentDB gateway | New `isomer` module (`documentdb_gateway_core/src/isomer.rs`), called from `processor/process.rs` before each request | Reads with `readConcern.afterClusterTime` on an IsomerDB projection wait for it (`isomer.wait_for`); writes to projection collections are refused with code 166, as for a MongoDB view |
| DocumentDB gateway | `isomer::after_request`, called from `processor/process.rs` after each successful request | Writes return `operationTime` and `$clusterTime`; projection reads return their freshness as `operationTime` (FR-R4a, FR-R5) |
| DocumentDB gateway | `isomer::stamp_handshake`, called from `processor/ismaster.rs` | With `ISOMER_IMPLICIT_CAUSAL=1`, hello returns a zero `$clusterTime` so drivers gossip session cluster time on every connection |

All are marked `IsomerDB:` in the source. The pg_trickle changes are worth proposing upstream as an
opt-in setting.

### Rules

1. **Each import is its own unmodified commit.** The directories have since moved (from
   `vendor/<project>/`), so to see everything we have changed, diff across the move:
   `git diff 51c92ee:vendor/pg_trickle HEAD:extensions/ivm` (import commits are in the table above).
2. **Keep every upstream LICENSE and NOTICE file.** Never remove or edit them.
3. **Mark our changes** with a short `IsomerDB:` comment at each change site.
4. **AGPL boundary.** Changes inside `extensions/sharding` are AGPL-3.0 and must be published if we
   offer them as a service. Keep IsomerDB's own code in `isomer/` and `isomer_router/`, talking to
   the sharding layer through hooks and SQL rather than by editing it, unless there is no other
   way. See PRD §13.
5. **Updating to a new upstream version:** copy the new tag into a scratch checkout, commit it
   unmodified as its own import commit on a branch, then re-apply our changes. Update the tables
   above in the same change.

## 2026-10-21 — Subqueries in analytics projections, and in the WHERE of a query that aggregates (FR-P6)

`tests/subqueries.sh` passes on the three-node cluster (isomer 0.23.0).

0.22.0 refused a subquery outside the aggregates of a block that aggregates, which left analytics
projections with little to use: they almost always group, and their subqueries are conditions
(`WHERE EXISTS (...) GROUP BY region`). Such a condition doesn't need the fact's rows while
folding. It is not one of the fact's own filters (`only_fact` already said so), so the columns it
reads stay in the fact's partial aggregates, the correlation among them, and the condition is
applied to those partial rows when deriving. Every fact row folded into a partial row has the same
values in those columns, so the condition passes or fails the partial row as a whole, and summing
the partials that pass gives the same counts and sums as the query. The cost is state: one partial
row per distinct correlation, a customer for `EXISTS (... o.customer_id = c.id)`.

Nothing else changed for keys: a changed row of the subquery's table becomes `EXISTS (a correlated
changed row)` in the block above, as it does for a block that doesn't aggregate.

Analytics with subqueries go through the same compile. Two things read the query as written and
had to read `truth_query` instead: the base table built from the tables, and
`analytics_differences`. Over sharded tables that are not co-located, Citus can't plan the
correlated subquery once it is wrapped in `CREATE TABLE AS` or compared with `EXCEPT ALL` (see
below). `auto` looks at every block's aggregates, not only the query's, to decide whether
incremental state would keep a copy of every row.

## 2026-10-18 — Correlated subqueries maintained incrementally (FR-P6)

`tests/subqueries.sh` passes on one node and on the three-node cluster (isomer 0.22.0), and
`tests/incremental.sh` and `tests/events_analytics.sh` still pass on both.

A query with subqueries is compiled as **blocks**: the query itself and each subquery in it, in
the order `isomer._engine_blocks` walks the parse tree (`_engine_sql_blocks` walks it the same way,
so a block number means the same thing in both). Each block keeps its own state, the way a whole
projection did:

- **A block that aggregates** keeps its fact's partial aggregates, grouped by the columns its
  conditions on the outer row read. `(SELECT count(*) FROM orders o WHERE o.customer_id = c.id)`
  is one partial row per customer, however many orders each has; a filter on orders alone is
  applied while folding. An aggregate that reads the outer row, or holds a subquery, is kept as
  written, which is how nesting works: `jsonb_agg(... (SELECT jsonb_agg(...) FROM lines l WHERE
  l.order_id = o.id) ...)` runs over a copy of the orders with each order's lines block beneath it.
- **A block that doesn't aggregate** (`EXISTS`, `IN`, `ARRAY(SELECT ...)`, a lookup, `ORDER BY ...
  LIMIT 1`) keeps copies of the columns it reads. Its derivation runs it as written over them, so
  ORDER BY, LIMIT and DISTINCT are fine there.
- A table read by several blocks has one staging table and, where some block joins it without
  aggregating it, one copy holding every column any of them reads.

**Deriving** a key runs the query as written with every block reading its state: a correlated
subquery runs once per outer row, over that row's partial aggregates (indexed by the correlation
column). **Keys**: for each relation of each block, the subqueries on the way up from that block
become `EXISTS (a correlated changed row)` in their parents, in place of the conditions they were
part of; `IN (...)` keeps its comparison inside (`(c.id = o.customer_id) IS NOT FALSE`), so a big
order reaches its own customer, not all of them. Output subqueries of the query become NULL,
typed before anything below is changed. An uncorrelated subquery reaches every key, as it should.

Not maintained this way, with the reason given: a subquery in ON, GROUP BY, HAVING or LIMIT; a
subquery outside the aggregates of a block that aggregates (in its WHERE: the filter would depend
on another table while folding); subqueries in analytics projections; distributing the state of a
query with subqueries (it stays on the coordinator).

A materialized view declared with correlated subqueries (`tests/native.sh`) now gets incremental
maintenance from the cost model, where it used to fall back to key recompute; its lookup-table
notice is for key recompute only, so it is checked on a view that asks for that.

Things that bit us:

- **The auditor's reference over sharded tables.** Citus runs a correlated subquery between tables
  that are not co-located on its own, but not once the query is filtered, compared (`EXCEPT ALL`)
  or joined with anything ("complex joins are only supported when all distributed tables are
  co-located"), not even inside a `MATERIALIZED` CTE. Reading each Citus table through a view
  `SELECT ... FROM t OFFSET 0` makes it plan each one as an intermediate result on the
  coordinator, after which the query runs anywhere. That is the projection's `truth_query`.
- **An EXPLAIN poisons the transaction for documentdb.** `_key_cost` (the cost model) runs
  `EXPLAIN` over plain tables. After any EXPLAIN, even `EXPLAIN SELECT 1`, Citus sends that
  transaction's later queries on reference tables to this node over a connection, which can't see
  what the transaction wrote: `create_projection(..., strategy => 'auto')` over plain tables
  failed writing to its new collection ("Collection not found ... metadata sync issue", or
  "cannot execute a distributed query from a query on a shard"). The existing tests only used
  sharded tables there. The EXPLAIN now runs in a subtransaction that is rolled back (its plan is
  kept in a variable).
- **drop_projection left Citus metadata behind.** It runs with DDL propagation off, and a
  documentdb collection dropped that way keeps its Citus table's shard and `pg_dist_partition`
  entry, the table itself gone. The next `citus_add_node` failed ("cache lookup failed for
  relation"), so a node that had ever dropped a document projection could not grow into a
  cluster. `test-all.sh` recreates volumes between the single-node and cluster stages, so it never
  saw it. The collection is now dropped with propagation on, and the 0.22.0 upgrade removes what
  earlier drops left behind, the way Citus's drop trigger would have.
- **Every fold scanned all its state.** Emptied groups, multiset values and joined rows were
  deleted with `DELETE ... WHERE __n = 0` over the whole table. A correlated subquery's state has a
  partial row per outer row, so with 200,000 customers a one-row write folded in 170-860 ms. Only
  the groups a fold reached can empty, so the deletes are now limited to those: the same write
  folds in about 4 ms, for a customer with 8 orders or 500,000.
- **The auditor and trigger DDL deadlocked.** Creating or dropping a projection's capture
  triggers locks every shard of its tables, and the auditor's reference queries lock shards too,
  across nodes and in another order (a repartition join reads them in stages). On a cluster
  Citus's distributed deadlock detector cancelled the DDL: `drop_projection` and
  `alter_projection` failed now and then in `incremental.sh`, more often the slower the auditor
  ran (a kind cluster alongside made it 10-60 s). Trigger DDL now takes an advisory lock before it
  locks anything, waiting for a running audit, and an audit that finds the lock taken skips its
  round (`audit` and `_create/_drop_source_triggers` are wrappers over the earlier functions).
- `tests/multinode.sh` declares a projection with correlated subqueries and a lookup table, which
  the cost model now maintains incrementally; that suite tests key recompute across nodes, so it
  asks for it.

## 2026-10-14 — Incremental state distributed across the cluster (FR-P6)

`tests/incremental.sh` passes on the three-node cluster with a projection whose state is
distributed, through concurrent writers, a switch to the coordinator and back, and repair; on one
node it is refused ("no worker nodes").

`create_projection(..., state => 'distributed')` (or `alter_projection(name, state => ...)`):

- **Placement.** The aggregated table's partial aggregates, and the multisets behind min and max,
  are Citus tables distributed by the group's hash (`__h`), co-located with each other; the joined
  tables' kept columns are reference tables, on every node. The coordinator keeps only the
  staging tables a fold passes through. The certificate names the rule and checks the
  distribution.
- **Folding** writes each group on the node holding its shard (INSERT ... ON CONFLICT from the
  coordinator's staging rows). A reference table can't be updated from a table on the coordinator
  alone, so the joined tables' netted rows are read on the coordinator first and written from a
  parameter (`jsonb_to_recordset` with the column types spelled out: the staging table's row type
  doesn't exist on the workers).
- **Deriving a key** is a distributed query: the pushed-down join of partial aggregates with the
  reference tables, aggregated on each shard and combined on the coordinator. The derivation
  filtered from outside (`SELECT ... FROM (derive) q WHERE q.key = ...`) made Citus compute every
  group as an intermediate result and filter after, so a fold's few keys cost a scan of the whole
  state. The compiler now writes a keyed derivation (`_engine_sql(..., 'derive_keyed')`) with
  `key = ANY ($1)` in its WHERE clause: the key is grouped, so filtering rows before grouping gives
  the same groups, and every shard filters before it aggregates. Every aggregate kind derives the
  same through Citus (jsonb_agg with ORDER BY, DISTINCT, percentiles were checked).

Things that bit us:

- **Citus couldn't read what it had just written.** A fold writes the distributed state over
  several connections per node, then reads it in a join with the coordinator's staging tables
  ("cannot perform query with placements that were modified over multiple connections").
  Transactions that write a distributed state use one connection per node
  (`citus.multi_shard_modify_mode = sequential`), set before anything else in the transaction:
  Citus refuses to switch, or to distribute a table in sequential mode, once a parallel query
  has run.
- **Dropping the state left tables on the workers.** The engine drops its state with DDL
  propagation off (its other relations are the coordinator's alone), which for a Citus table
  dropped the coordinator's copy only; distributing again failed with "relation already exists"
  on a worker. Citus tables are now dropped with propagation on.

## 2026-10-13 — Analytics projections maintained incrementally (roadmap)

`tests/events_analytics.sh` passes on one node and on the three-node cluster (isomer 0.21.0).

An analytics projection whose query the engine can maintain (`create_analytics_projection(...,
strategy => 'incremental')`, or `'auto'`) keeps the engine's state like a document projection, and
its rows in two tables:

- **a columnar table** (the base), each row tagged with its key `__k`: the jsonb array of the
  query's grouped output columns (`[]` for a single aggregate row). A query whose rows can't be
  told apart by their columns (it groups by something it doesn't return) isn't maintained this
  way;
- **a heap of changed rows** (`<name>_changes`): each key the deltas reach is re-derived from the
  state there, or marked deleted when the query no longer returns it. The derivation is filtered
  by each key column's values, so the planner pushes the filter below the grouping and derives
  only those keys, then by the whole key.

The view is the base with its changed rows replaced (an anti-join on `__k`, hashed: the changes
are few). At most every refresh interval the two are merged into a new columnar table, without
running the query; the changes table is locked against writes for the merge, readers are not.
`refresh_analytics(name, force => true)` builds everything again from the tables. A TRUNCATE of a
source rebuilds the state and the base.

`'auto'` picks incremental maintenance when the query fits and none of its aggregates is kept as
written (a percentile would keep a copy of every row it reads, larger than the table it
produces); `'rebuild'` is how 0.20 kept every analytics projection.

`analytics_differences` checks an incremental one in one statement, so the tables, the rows and
the queues are seen at one moment: with nothing queued, every committed change is in the rows. On
a cluster, deltas still on a worker are outside that moment, so they are checked before and after.

Also: `dump_projections` wrote every projection as `create_projection(...)`, which refuses the
analytics and event kinds, so a dump of a database with either failed on restore. It now writes
them with their own functions; event projections record their rules (one created before 0.21.0
kept only its compiled form, and is listed as a comment).

## 2026-10-13 — Incremental maintenance for more queries: nested documents, DISTINCT, grouping expressions, sync (FR-P6)

`tests/incremental.sh` passes on one node and on the three-node cluster (isomer 0.21.0).

- **Aggregates kept as written.** jsonb_agg, string_agg, array_agg, ordered aggregates,
  percentiles: anything that doesn't decompose into sums, counts or a multiset. The aggregated
  table's primary key and every column the aggregate reads become grouped columns of its partial
  aggregates, so the state holds one row per row of that table, and the derivation runs the
  aggregate as written over the same rows the tables would give (`_engine_sql` keeps an aggregate
  whose template is NULL, and leaves it out of the fact statements). It may read the joined
  tables: an order's lines with each product's title and category, kept from the joined tables'
  state. The state is as large as the rows the aggregate reads, which is what the documents hold
  anyway. A table with no primary key is refused with that reason.
- **DISTINCT.** count, sum and avg (DISTINCT c) over a column of the aggregated table add c to the
  grouped columns, so each partial row is one value of c in its group, counting while any row holds
  it: `count(DISTINCT c) FILTER (WHERE __cN > 0)` over the partial rows. Over a joined table's
  column they are kept as written (which is decided once the aggregated table is known).
- **Grouping expressions** (`GROUP BY upper(status)`, `date_trunc(...)`): the columns they read are
  grouped columns of the partial aggregates, and the derivation groups by the expression. Volatile
  ones are refused.
- **Sync** (`freshness => 'sync', strategy => 'incremental'`): a statement-level trigger folds the
  statement's deltas and applies the keys they reach before the statement returns. Folds of one
  projection are serialised by a lock held to commit, so writers to its tables take turns. The
  tables must be plain tables: a statement across a sharded table's shards runs their triggers
  over parallel connections of one distributed transaction, which can't take turns on one state
  (each waits for the others to commit; Citus cancelled it as a distributed deadlock, found by the
  test's statement across every shard), and on a cluster the shards' deltas reach the coordinator
  only after they commit. Tables Citus manages on the coordinator alone are fine.

Things that bit us:

- **Grouped columns inside a grouping expression were added to GROUP BY.** The compiler adds
  columns that are functionally dependent on a grouped primary key, since the state relations have
  none. It looked at the flattened target list, where the column inside `date_trunc('day',
  placed_at)` looked ungrouped, so the derivation grouped by placed_at too and returned a row per
  timestamp. On Postgres 18 a grouped expression in the target list is a reference to the grouping
  step; only bare table columns are read without being grouped.

Not changed: correlated subqueries (`(SELECT jsonb_agg(...) FROM orders o WHERE o.customer_id =
c.id)`) are re-derived by key. Re-deriving one customer's orders reads only that customer's rows,
so key recompute is already the cheap strategy for them; the same document written with a join
and GROUP BY is maintained incrementally.

## 2026-10-13 — Edge replicas: a copy on disk, and MongoDB (README roadmap)

`tests/edge.sh` passes, with the new checks below.

- **A copy on disk** (`ISOMER_EDGE_DATA_DIR`, `services/edge/src/store.rs`). Each projection has
  `snapshot.json` (every document and the cursor they are as of) and `log.jsonl` (each change
  applied since). A change is appended as it is applied and the log is synced once per batch the
  origin sends. A crash that loses the log's tail only moves the cursor back, and changes are
  keyed upserts and deletes, so applying them again is harmless; a torn last line is cut off on
  restore. Once the log holds 10,000 changes and more than the snapshot has documents, it is
  folded into a new snapshot (written aside, then renamed over the old). A restarted edge serves
  the copy at once and resumes the feed from its cursor: the test stops it, writes, starts it, and
  sees the write arrive without a new snapshot.
- **Which copy is valid.** A feed cursor means nothing across incarnations of a projection: one
  dropped and created again keeps its name but starts a new history, and the old cursor would
  replay the new feed on top of stale documents that are never deleted. The snapshot records the
  origin and when the projection's history was turned on (`history_since`, now in the API's
  projection listing, with the MongoDB database); a copy from anything else is discarded. The
  test recreates the projection with fewer rows while the edge is down, and the rows that are
  gone are gone at the edge too.
- **MongoDB** (`ISOMER_EDGE_MONGO_LISTEN`, `services/edge/src/mongo.rs`, `query.rs`). The edge
  speaks the wire protocol (OP_MSG, and OP_QUERY for the legacy handshake) as a read-only
  standalone server. find (filters, sort, skip, limit, projection, batches and getMore), count,
  distinct, aggregate ($match, $sort, $skip, $limit, $project, $count, $group with $sum, $avg,
  $min, $max, $first, $last, $push, $addToSet), listCollections, listDatabases. Filters follow
  MongoDB's semantics over the JSON documents: arrays match by element, numbers by value whatever
  their width, comparisons only within a type, `{f: null}` matches a missing field; $eq, $ne, $gt,
  $gte, $lt, $lte, $in, $nin, $exists, $regex, $size, $all, $elemMatch, $not, $and, $or, $nor. A
  filter on `_id` is a lookup by key. Anything else is refused with "send this query to the
  origin" rather than answered differently; writes are refused naming the origin.
- **Authentication is the origin's.** The password is an IsomerDB API key, sent with SASL PLAIN
  (`authMechanism=PLAIN&authSource=$external`), checked against the origin like a bearer token;
  a collection is visible only if the key's role may read the projection. SCRAM can't work: the
  edge never holds a password's verifier, and a client that tries it is told what to use. PLAIN
  sends the key as it is, so the listener takes TLS and refuses to start without it unless
  `ISOMER_EDGE_MONGO_PLAINTEXT=1` (compose sets it for local development).
- **Read your writes over MongoDB.** A causally consistent session sends
  `readConcern.afterClusterTime`. The edge turns the timestamp back into the write token
  (the inverse of `isomer.token_to_mongo_timestamp`) and waits as `X-Isomer-After` does. The test
  writes at the origin, advances a session to the write's timestamp, and reads it back at the
  edge three times in a row.
- Events (event projections) in the feed only move the cursor; they were stored as documents
  before.

Not built: the edge keeps no key's access on disk, so after a restart it serves only once the
origin's API answers who a key is; that is deliberate (a revoked key must stop working).

## 2026-10-13 — The sign-up page (FR-CL1)

The control plane serves a page at `/`: a team signs up or logs in, creates a cluster in a region
on a plan (the plan's size and its monthly price as created are shown before creating), reads its
connection strings once it is ready, scales it, deletes it after typing its name, and sees this
month's invoice. It is plain JavaScript in `controlplane/web`, compiled into the binary with
`go:embed`, on the API's own origin, so it calls the same public API and needs no CORS. It builds
the page from text nodes only, never HTML from the server, under a content security policy that
allows nothing but its own origin.

- **The token** is kept in `sessionStorage` and revoked on log out (`DELETE /v1/tokens/current`,
  new), so closing the tab or logging out ends it.
- **Sign-up and log-in are rate limited** per client address: a burst of 10, then one every 6
  seconds. Behind a proxy, `CP_CLIENT_IP_HEADER` names the header carrying the client's address.
- Checked in Chrome through Playwright against the control plane on kind: sign up, create and
  delete a cluster, log out, a wrong password, log back in, at desktop and phone widths, light and
  dark. `tests/controlplane.sh` checks the page is served under its policy, that log out revokes,
  and that a burst of failed log-ins is cut off.

What bit us: **a `pattern` attribute is compiled with the `v` flag,** under which an unescaped `-`
at the end of a character class is a syntax error; the name field's pattern escapes it.

Not built: email verification (it needs a mail provider), and the console's operator views are
still a separate app (`clients/console`) run against a cluster's own API.

## 2026-10-12 — Jepsen: coordinator faults, clock skew, MongoDB, incremental projections (FR-K4)

`tests/jepsen.sh` now covers what the first Jepsen entry below left out:

- **Faults on any node.** `--fault-nodes` (or `FAULT_NODES=db,worker1`) picks where the nemesis
  pauses, kills and cuts off; the coordinator is one of them. Clients reconnect and retry, so a
  kill of the coordinator shows up as indeterminate operations, not as a crashed run.
- **Clock skew.** `SKEW=1` runs the cluster on an image with libfaketime preloaded for the
  `postgres` user. The nemesis writes an offset of 5 to 60 seconds, forwards or backwards, to a
  file the library rereads every second, and clears it on heal.
- **MongoDB.** The `mongo` workload is read-your-writes through the gateway with the MongoDB Java
  driver: a causally consistent session increments, then reads the projection and must see it.
- **Incremental projections.** The `incremental` workload writes order lines under sales by
  category, kept by the incremental engine across a reference table, a distributed table and one
  not co-located with it: inserts, updates, voids, deletes, products moved between categories, and
  statements across every shard. At the end the projection must equal recomputation, no deltas
  may be left on the workers, no pull left prepared, and the certificate must hold.
- **A report per release.** `./tests/jepsen.sh report` runs every workload with faults on the
  workers, on the coordinator, and with skew on every node, and writes the matrix to
  `docs/jepsen/isomer-<version>.md`. Elle checks append for G0, G1a, G1b and G1c, with 30
  seconds to search for cycles (the default ran out and answered "unknown"). It checked internal
  consistency too, until the 0.21.0 report flagged it in every run, clock skew alone included:
  Elle's internal check expects a transaction's later read to equal its earlier one plus its own
  appends, but the client runs READ COMMITTED, where each statement rightly sees what others
  committed meanwhile (`[1]`, then `[1 2 … 15 8]` with its own 8 last). The database did what READ
  COMMITTED promises; the check asked for more. All 12 runs of the 0.21.0 report are valid.
  One earlier report run hung for 40 minutes in the incremental workload's setup, dropping the
  previous run's projection after the coordinator had been killed in the run before; it did not
  happen again in that sequence or in the full matrix, with lock waits watched for.

It found a third bug, fixed:

- **Read-your-writes on a cluster assumed no clock steps back.** `wait_for` took every node's
  `clock_timestamp()` as the barrier and waited for queued changes enqueued before it. A worker
  whose clock was ahead when a write queued, then stepped back, left that write stamped after the
  barrier, so the read didn't wait for it: stale reads under skew only. Queued changes already
  carry the Citus cluster clock of the node that queued them, which never goes back (the larger of
  a logical clock and the wall clock, persisted across restarts). The barrier is now that token:
  the largest of every node's cluster clock and every token already queued. `wait_for` waits
  until nothing at or below it is queued anywhere, asking the workers before the coordinator so a
  change moving between them is seen in one place or the other, and counting a worker that
  doesn't answer as still busy.

Things that bit us: libfaketime keeps its state in shared-memory semaphores that only the user who
created them can open, so the server must run as `postgres` from the start (`user: postgres` in
`jepsen/compose.skew.yml`). A `FAKETIME` variable overrides the timestamp file, so the image sets
none. MongoDB counters come back as 32-bit integers.

## 2026-10-12 — Event and analytics projections (roadmap)

`tests/events_analytics.sh` passes on one node and on the cluster (isomer 0.20.0).

- **Event projections** turn row changes into domain events:
  `isomer.create_event_projection('order_events', 'shop.orders', events => '{"order_paid": {"when":
  "new.status = ''paid'' AND old.status IS DISTINCT FROM ''paid''", "payload": "..."}}')`. Each
  rule is a condition and a payload over the old and new row. Capture is the incremental engine's:
  the old and new images are recorded together in the writing transaction, on the node holding the
  row, and reach the coordinator exactly once. The drain turns them into events and appends them to
  the projection's feed under its feed lock, so ids follow commit order and one row's events keep
  theirs. Consumers read them with `isomer.changes_since`, or over HTTP from `/changes` and
  `/live`, as `op: event`. Past retention, events go (a document's history keeps each key's version
  at the cutoff; an event log keeps nothing older). A TRUNCATE makes no events.
- **Analytics projections** keep a query as a columnar table (Citus columnar),
  `isomer.create_analytics_projection(name, query, refresh => '5 minutes')`, read through a view.
  It is rebuilt into a new table and swapped in, in one transaction, when the sources have changed,
  at most once per refresh. Changes are noticed from the tables' own statistics on every shard
  (rows inserted, updated, deleted, and the file a TRUNCATE replaces), so it needs no triggers and
  follows attached tables. `isomer.analytics_differences` compares it with the query while the
  sources are unchanged; the certificate runs it.

Things that bit us:

- **A missing row is a row of nulls,** not NULL, once it comes from `jsonb_populate_record`, so
  `coalesce(new, old)` picked an empty `new` for a delete. `CASE WHEN new IS NULL` is right: a row
  of nulls IS NULL.
- **`run_command_on_shards` fills in one `%s`.** A command using the shard's name twice needs
  `%1$s`.
- **Columnar storage is its own extension** (`citus_columnar`), which `CREATE EXTENSION citus` no
  longer creates. New nodes create it; `isomer.control` requires it; an older cluster is told to.

## 2026-10-12 — Managed Postgres providers (FR-A11)

`tests/managed_source.sh` passes. Attach and cutover now run, and are tested, as an ordinary role
holding only what managed providers grant: it owns its database and tables and may replicate, and
is not a superuser. `isomer.provider_check(conninfo)` (isomer 0.19.0) recognises the provider from
the roles each one creates (RDS and Aurora, Cloud SQL, Azure Flexible Server, Supabase, Neon,
AlloyDB, Crunchy Bridge, and Tiger Cloud, which it refuses), and checks what attach needs there:
logical decoding on, a role that may replicate (on RDS, membership of `rds_replication`), a free
slot and WAL sender, the right to create the publication, and, for cutover, ownership of the
database. A failed check says what to change in that provider's terms (the parameter group on RDS,
the database flag on Cloud SQL, the project setting on Neon). `isomer.attach` runs it first and
refuses before creating anything.

Not done, because it needs accounts: running attach and cutover against each real provider in CI
(Tier 1 is "CI-tested" in the PRD). The test recognises providers by creating their roles on plain
Postgres; their real limits (RDS's parameter groups, Neon's suspended computes dropping slots) are
only known from running there.

## 2026-10-12 — Edge replicas (README roadmap)

`tests/edge.sh` passes. `services/edge` (Rust) serves projections from memory, close to users,
fed by the origin's change feed: it follows `/v1/projections/{name}/live` (the snapshot, then each
change in commit order), resumes with `Last-Event-ID` after a disconnect, and takes a new snapshot
if the origin has pruned past its cursor.

- **Access is the origin's.** A client's API key is checked against the origin (the projections
  its role may read) and cached for 30 seconds, so the edge never widens what a key can read.
- **Read your writes at the edge.** With `X-Isomer-After: <token>` the edge asks the origin for the
  feed cursor that reflects the write (`/v1/projections/{name}/cursor`, new: it waits for the token,
  then returns the newest change id, and the write's change is at or before it because changes are
  logged in the transaction that applies them), and answers once it has applied the feed that far.
  The test writes and reads back through the edge five times in a row.
- The projections need history on: the feed is what the edge follows.

Persistence across restarts and MongoDB were added on 2026-10-13 (below).

## 2026-10-12 — Embedding backfills through provider batch APIs, and bundled models (FR-V1a, FR-V1b)

`tests/embed_batch.sh` passes, against the mock provider's new Batch API (isomer 0.18.0).

- **Batches.** A model registered with `batch_api => true` sends a backfill of at least
  `batch_min_items` jobs to the provider's Batch API (`/v1/files`, `/v1/batches`): one JSONL file
  of embedding requests, processed within 24 hours at half price at OpenAI and outside the rate
  limits. `isomer.embed_batch_lease` claims the jobs into a batch and numbers them; the embedder
  uploads, polls, and hands each result back through `embed_batch_result`; items the provider
  rejects, and items a failed or expired batch never returned, go back to ordinary requests.
  Steady-state work never waits for a batch, and the ordinary workers leave a backfill big enough
  for a batch to it.
- **A row that changes while its old text is in a batch** must not get the old text's embedding.
  The job records the hash it was batched with; a result for a job whose hash has moved on is
  dropped, and the new text goes by ordinary request (it was made ready again by the change).
- **Bundled models** run on IsomerDB's own key: usage is billed at list price times
  (1 + `markup`), after the cluster's one-time free allowance (`bundled.free_tokens`).
  `embedding_usage.billed_usd` is what the customer owes, `cost_usd` what the provider charges;
  monthly caps count what the customer pays. Metered hourly as `embedding_billed_usd`, which the
  control plane passes onto invoices.

What bit us: **Postgres takes no row locks in a query with window functions** ("FOR UPDATE is not
allowed with window functions"), which the first lease did to number the items: it locks first
now, and numbers after.

## 2026-10-12 — The cloud control plane (FR-CL1, FR-CL2)

`tests/controlplane.sh` passes on kind. `controlplane/` is the managed service on top of the
operator, in Go: a team signs up, creates a cluster in a region on a plan, reads its credentials,
scales it within the plan, sees its usage and its invoice, and deletes it.

- **Clusters are IsomerCluster resources** in a namespace per organisation
  (`isomer-org-<id>`), in the Kubernetes cluster that serves the region. The operator does the
  rest: provisioning, failover, backups, upgrades. A region is a kubeconfig (empty: the cluster the
  control plane runs in); a plan is a size (workers, standbys, storage, CPU, memory, the most
  workers it may scale to) and two prices.
- **Pricing follows the PRD:** compute by the node-hour and storage by the GB-month, the same
  units as the database (decision 6). Standbys, which the database can't see, are billed one per
  node on plans that have them. Embeddings on bundled models pass through at what the cluster's
  own metering bills; embeddings on the customer's own keys bill nothing here.
- **Usage comes from each cluster's own metering:** every five minutes the collector reads
  `isomer.usage_report` for the last two days, hour by hour, and replaces those hours, so
  collecting twice never counts twice. A deleted cluster is read one last time first, and the time
  it ran stays on the invoice.
- **Accounts:** passwords under bcrypt, API tokens stored only as SHA-256 hashes. Every query is
  scoped to the caller's organisation: another team's cluster is a 404, as one that doesn't exist.
  `/v1/admin/collect` takes a separate admin token.
- **Its own Postgres** holds organisations, users, tokens, clusters and collected usage
  (`controlplane/config`, with development secrets to replace).

What bit us: **macOS's `/bin/bash` 3.2 mangles escaped quotes nested inside `"$( )"`.** The test's
second sign-up arrived as a JSON string instead of an object. Request bodies are now built with
`printf` first.

Not built: payment collection (invoices are computed; charging a card needs a payment provider
account), and regions in other clouds (any kubeconfig works, none has been run).

## 2026-10-12 — Incremental maintenance across shards (FR-P6, FR-S5) and the strategy chooser (FR-S7)

`tests/incremental.sh` passes on one node and on the three-node cluster. A projection that
aggregates a sharded table through joins (sales per category from order lines, through products
and a reference table of categories) is kept from row deltas instead of by re-deriving keys from
the tables, with no key mappings declared (isomer 0.17.0).

Why: re-deriving a key re-reads its whole group, and for a key the table only reaches through a
join (lines reach a category only through products) every write re-derived every key. On the
cluster, re-deriving one category's sales is a repartition join across every shard. Folding a
write costs the same however large its group is.

How it works:

- **Capture.** Each write records row images, -1 for the old row and +1 for the new, in the
  writing transaction, on the node holding the row (`isomer.deltas` on the coordinator,
  `isomer.node_deltas` on workers). An update that changes none of the columns the projection
  reads records nothing. A reference table's change runs on every node holding a copy; only the
  copy in the lowest node group records it.
- **Exactly once.** Deltas are counted, not just noted, so a duplicate is wrong where a duplicated
  key was harmless. `isomer.pull_node_deltas` takes a worker's deltas in a prepared transaction
  there, commits them on the coordinator together with the pull's id (`isomer.delta_pulls`), then
  commits the worker's. A pull left prepared is finished the way the ledger says: recorded means
  commit, absent means roll back. Pulls are serialised; a second puller could otherwise roll back a
  pull the first has not recorded yet. Worker rows are inserted in the worker's order, so changes
  to one row keep theirs.
- **The compiler** works on Postgres's own parse tree (`isomer_router/isomer_engine.c`):
  `isomer._engine_analyze` reports relations, joins, conjuncts, aggregates and grouping;
  `isomer._engine_sql` rewrites the analysed query and deparses it back with `pg_get_querydef`.
  State relations line up with the source tables attribute by attribute (dropped columns
  included), so the rewrite only swaps a relation's oid and every Var stays valid.
- **The rule it checks** (`isomer._engine_shape`): one table, the fact, holds every aggregate's
  input, and every join leading away from it lands on a unique key. Each fact row then joins at
  most one row of the rest, so its aggregates can be kept per group of the columns it is joined
  and grouped by, before the join. sum, count, avg, min, max, bool_and and bool_or, with FILTER;
  inner joins, and a LEFT JOIN with the fact alone on the optional side; HAVING; a global
  aggregate under a constant key. Anything else is refused with the reason.
- **State,** in `isomer_state`: the fact's partial aggregates per group (count, and a sum and
  count per aggregate; min and max as multisets of values with multiplicities), and for each
  joined table the columns the query reads, keyed like the table. Folding is commutative, so
  deltas from different nodes can arrive in any order and the state still converges.
- **Which keys to re-derive.** A key's value can change only if a delta of one input reaches it
  through the state of the others, before or after the fold. For each relation with deltas, the
  query with that relation swapped for its staged rows (and everything else for state) gives the
  keys it reaches; that runs before and after folding, and the union is queued like any other
  key. The drain then derives those keys from the state (`projections.derive_query`), through
  the same apply path as before, so history, the change feed, MongoDB and HTTP reads are
  unchanged.
- **Proof that it holds.** At creation the derivation from the state must equal the query over
  the tables row for row, or creation fails. The auditor keeps recomputing samples from the
  tables. `isomer.certificate` names the rules relied on and re-checks the unique keys, the
  triggers on every placement and the state.
- **Strategy (FR-S7).** `create_projection(strategy => 'auto')`, now the default, asks
  `isomer.choose_strategy`: incremental when the query fits the rule and the cost of re-deriving
  one key is high, or the query maps sources to nothing, or the cluster can't run it at all;
  key recompute otherwise (a key on its own shard costs one small read and keeps no state).
  The cost comes from where Citus keeps each table and how the joins connect them: a key on one
  shard, every shard, or a repartition. `isomer.strategy_advice()` says what it would pick for
  each projection now; `alter_projection(name, strategy => ...)` switches, in one transaction.
- **Queries the cluster can't run as written.** Citus refuses an outer join between tables that
  are not co-located (customers with their order counts, sharded differently). Neither key
  recompute nor the auditor could run that query, but the engine reads one table at a time, so it
  maintains it. The auditor then checks against `truth_query`: the same derivation over partial
  aggregates computed afresh from the table (forced to be computed first with `OFFSET 0`), joined
  with the tables themselves.

Measured on the dev stack: folding 2,450 deltas and applying the keys they reach takes about 70
ms; a write is applied about 0.7 s after it commits (the drain runs every second), within the 2 s
budget. The checks in the test take longer only because `verify` re-runs the original query, a
repartition join, each time.

Things that bit us:

- **A functionally dependent column broke the derivation.** `GROUP BY c.id` with `c.name` in the
  output is valid because `c.id` is the table's primary key; the state relation has none, so the
  rewritten query failed. The compiler now groups such columns explicitly, which changes nothing.
- **Planning a repartition join changes the rest of the transaction.** After Citus has planned
  one (even for EXPLAIN), later statements that mix local tables with distributed subqueries go
  over its own connections, which can't see the state tables just created in the transaction. So
  nothing in the create path plans the original query: whether the cluster can run it, and what a
  key costs, are worked out from metadata (`isomer._shard_groups`, `_runnable`, `_key_cost`).
- **Explaining over a second connection deadlocks itself.** An early version asked the shards to
  explain through a separate connection; inside `alter_projection`, which had already locked the
  sources, that connection waited for our own locks forever.
- **`count(t.*)` counts rows.** IS NULL on a whole row tests every field, so a row with any null
  field looked absent. A whole-row argument is counted as a constant.
- **`CREATE OR REPLACE FUNCTION` resets SECURITY DEFINER.** `freshness` had been made one with
  ALTER FUNCTION in 0.3.0; redefining it without the clause made the MongoDB gateway's
  `isomer.freshness` call fail with "permission denied". Everything this version adds is revoked
  from PUBLIC.
- **Citus refuses volatile functions inside writes to distributed tables,** so the test's random
  writers pick their values first.
- **A lock race in `alter_projection`, older than this work, found by the full suite.** It took
  `FOR UPDATE` on the projection's row, then needed an exclusive lock on the source table to swap
  triggers. The auditor runs every projection in one transaction: having read the table, it
  inserted a suspect row whose foreign key takes KEY SHARE on the same projection row, and waited.
  `alter_projection` never changes the key, so it now takes `FOR NO KEY UPDATE`, which foreign-key
  checks don't wait for.
- **A statement that wrote several shards could deadlock key recompute** (older than this work,
  found by this test). Each shard's trigger upserted the keys it touched into `isomer.pending`;
  Citus runs the shards over parallel connections of one distributed transaction, so two of them
  inserting the same key waited on each other until Citus's deadlock detector cancelled the
  statement. Capture now only appends (`isomer.key_log`, and `node_queue` on workers without its
  unique key), and the drain merges what has committed into `pending` in one session. A write
  still uncommitted when the drain merges is merged by the next drain, so nothing is missed;
  `wait_for`, `freshness`, the auditor and the status view count the log too.

Limits:

- The state lives on the coordinator. It is small (one row per group of the fact's join and
  grouping columns, plus the joined tables' columns), but it is not sharded.
- Incremental projections are bounded; a sync projection is re-derived by key in the writing
  transaction.
- `jsonb_agg` (nested documents), `DISTINCT` aggregates and expressions in GROUP BY stay on key
  recompute.
- Floating-point sums can differ from the query's in the last bits (addition order), which the
  auditor would report.

## 2026-10-12 — Cutover plan (FR-A6)

`tests/cutover_plan.sh` passes. `isomer.cutover_plan('crm')` (isomer 0.16.0) reads the team's
database and IsomerDB's copy of it and returns the plan in order. Each step is ok, blocked, to do or
for information, and comes with its SQL. `isomer.cutover_plan_markdown` renders the same plan as a
document. The admin API (`/v1/admin/attachments/{name}/plan`), `isomer plan` and the console's
Attachments page all show it.

- **Before cutover:** the copy has finished, replication is healthy (slot, WAL, errors,
  conflicts), the auditor has checked the projections over these tables and found no drift,
  every table in the attached schemas is copied, and nothing outside them is written to. Cutover
  makes the whole source database read-only, so writes to other schemas would start failing.
- **What the copy does not carry yet.** Attach copies columns and primary keys. The plan
  compares the source with the copy and returns the SQL for whatever is missing: extensions,
  functions, indexes, unique, check and exclusion constraints, foreign keys, triggers, views and
  materialized views, and roles and grants. The source prints every name schema-qualified
  (`search_path` is `pg_catalog` on that connection), so the SQL runs as it is. Foreign keys are
  added `NOT VALID`, then validated. Stored generated columns are flagged, since they arrive as
  plain values.
- **Cutover:** the applications connected to the source (role, address, application name), and
  the write rate and quietest hour seen so far. `isomer.sample_attachments()` records writes to the
  attached tables per hour, every minute. Then the cutover command.
- **After cutover:** the bounded projections that can now be `sync`, tables over a size threshold
  with a shard key, and how to stop rollback replication.

The test builds a source with each of these gaps, applies the plan's own SQL, cuts over, and
checks that the copy enforces the foreign key, unique and check constraints, fires the trigger, and
gives the application's role exactly its privileges.

- **Cutover failed on stored generated columns.** It copied every column default, and a
  generated column's expression is stored as one; Postgres refuses it as a default because it
  refers to other columns. Cutover now skips them. The column keeps its replicated values, and the
  plan says what to do. The test found this.

## 2026-10-12 — Alert delivery (FR-K3)

`tests/alerts.sh` passes. Alerts were raised and shown before; now they reach on-call (isomer
0.15.0 and the HTTP API).

- **Each alert is tracked from firing to resolved** in `isomer.alert_state`, keyed by kind and
  subject. `isomer.pending_notifications()` brings it in line with `isomer.alerts` and returns the
  transitions not yet delivered; an alert that clears and comes back is a new incident.
- **Channels:** `webhook` (JSON, optional bearer token), `slack` (incoming webhook) and `pagerduty`
  (Events API v2: trigger on firing, resolve on resolved, one dedup key per alert and subject).
  Each has a minimum severity: drift and broken attachments are critical, lag-budget breaches,
  embedding lag and dead letters are errors, staleness and paused budgets are warnings. "Unused"
  projections are never paged.
- **The HTTP API delivers** every `ISOMER_ALERT_INTERVAL_SECS` (15 by default) and records every
  attempt in `isomer.alert_deliveries`. A failed send is retried on the next pass, to the channels
  that have not accepted it yet. The first version retried to all of them, so one failing channel
  made the others hear the same alert three times; the test caught it.
- **Secrets stay out of the database:** a channel names the environment variable on the API that
  holds its routing key or token.

The test drives a mock receiver (`tests/mockhooks/`) with a channel that fails twice before
accepting.

## 2026-10-12 — Jepsen (FR-K4)

`tests/jepsen.sh` runs Jepsen (0.3.14) against the three-node compose cluster from a control
container, with a nemesis that pauses, kills and network-isolates the workers through the Docker
socket, a randomly chosen fault every 20 seconds, healed 10 seconds later:

- **append**: Elle list-append over a table distributed in 8 shards, 10 clients, 2–4 operations per
  transaction (most span shards, so commit is two-phase). Checked against READ COMMITTED (G0, G1a,
  G1b, G1c) and for lost or phantom appends. Valid over 120s with 2 pauses, 2 kills and 2
  partitions: 3,344 transactions (2,994 ok, 54 failed, 296 indeterminate), no anomalies.
- **ryw**: read your writes through a document projection. A client increments a counter, takes
  `isomer.last_write_token()`, and reads the projection with `SET isomer.read_after`. An answer must
  show its increment or a later one; "not yet" (IS001) is the allowed alternative. At the end every
  acknowledged increment must be in the table and the projection must equal recomputation.
  Valid over 120s with a pause, 4 kills and a partition: 550 increments (7 indeterminate), 497
  reads answered, 0 stale, 44 explicit "not yet", none lost, no drift.

It found two bugs, both fixed:

- **A cluster hung while a worker was paused or cut off.** Citus caches a connection per worker,
  and without keepalives a query waiting on one to a node that stopped answering waits until the
  node returns: ten minutes in the first run, the length of the pause. Node connections now carry
  `keepalives=1 keepalives_idle=10 keepalives_interval=5 keepalives_count=3 tcp_user_timeout=20000`
  (image config, and the operator's `citus.node_conninfo`), so such a query fails within about 20s.
- **Read-your-writes did not wait for writes to a worker's shards.** `isomer.last_write_token()` is
  set by the capture trigger, which on a multi-node cluster runs in the worker's backend, so the
  client's session never had a token and `read_after` did nothing: 11 stale reads in the first run.
  `isomer_router` now sets the session's token from the coordinator's clock after every top-level
  write to a table with capture triggers. `tests/multinode.sh` covers it.

Not covered yet: faults on the coordinator itself (the operator's failover is tested on kind
instead), clock skew, and the MongoDB wire path.

## 2026-10-12 — Usage metering (FR-CL2)

`tests/metering.sh` passes. Every minute `isomer.meter()` (isomer 0.14.0) samples into
`isomer.usage`, one row per hour, metric and dimension:

| Metric | Kind | By |
|---|---|---|
| `storage_bytes` | gauge (peak) | (total of every node) |
| `node_hours` | counter | coordinator / worker |
| `documents_written` | counter | projection |
| `embedding_tokens`, `embedding_cost_usd` | counter | model |
| `sql_statements` | counter | (coordinator) |
| `api_requests` | counter | API key's role |

`isomer.usage_report(from, to, 'hour'|'day'|'month')` rolls it up; the admin API serves it as JSON
or CSV (`/v1/admin/usage`), `isomer usage` prints or exports it, and the console has a Usage page.

- **Counters are differences from the last sample** (`isomer.usage_cursor`), so a restart, a second
  sampler (they serialise on an advisory lock) or a statistics reset never counts anything twice.
  The meter's very first sample takes a baseline; a source that appears later (a new projection,
  model or month) counts in full. The first version treated every new source as a baseline, which
  would have dropped each month's first minute of embedding tokens; the test caught it.
- **The HTTP API counts its own requests** in memory and adds them every 15 seconds through
  `isomer.record_usage`, keeping counts it could not write. Only data requests count: the admin
  API (the console polls it) does not.
- **Node-hours accrue between samples, capped at two minutes**: a gap means the sampler was not
  running, and is not billed.

Not metered: standbys (the database does not see them; the operator does), MongoDB gateway
requests, and network transfer.

## 2026-10-12 — Operator: version upgrades (FR-CL1)

`tests/operator_upgrade.sh` passes on kind. A cluster with standbys moves to an image carrying a
newer isomer extension while a writer commits once a second through the client Service: every pod
reaches the new image (standbys first, then a planned switchover per group), the extension is
updated on every node, no acknowledged write is lost (137 of 137), and the writer saw 21 failed
attempts over the run, about seven seconds per group switched.

- **The operator rolls pods itself** (`OnDelete` StatefulSets): it knows which pod is the primary,
  which the StatefulSet controller does not, and would restart a primary while its standby was
  still on the old image. Any template change goes through the same path.
- **A planned switchover loses nothing:** the old primary's fast shutdown sends all WAL to the
  standby before exiting, and the operator waits for the standby to have replayed it before
  promoting.
- **Extensions are updated per primary, in dependency order,** only where the image has a newer
  version; standbys get the change through WAL.

Limit: write availability during a switchover is a few seconds per node; clients must retry. A
major Postgres upgrade is not covered.

## 2026-10-12 — Operator: backups and point-in-time recovery (FR-CL1)

`tests/operator_backup.sh` passes on kind, against SeaweedFS as the S3 store. A cluster archives
every node's WAL and takes base backups; a restore point is created; later writes follow; a second
cluster restored to the point has exactly the data before it (and the projection, equal to
recomputation, still maintained), with Citus pointed at its own nodes and the source's passwords.

- **WAL-G** (3.0.9, pinned and checksummed in the image) does the archiving and base backups.
  `isomer-walg` picks the node group's area of the store from the pod's name; `isomer-backup-agent`
  is a sidecar that takes the base backups while its pod is the primary.
- **Consistency across nodes comes from restore points**, not timestamps:
  `isomer.create_restore_point()` (isomer 0.13.0) wraps `citus_create_restore_point`, which blocks
  distributed commits for the instant it writes the point on every node. Names carry the UTC time,
  which a restore uses to choose each node's base backup (the newest that finished before it).
- **Restore is a new cluster**, built by each pod's init container (`isomer-node-init`): fetch,
  replay to the point, promote. Citus's metadata then still names the source's hosts; the operator
  repoints the workers first and the coordinator last, syncing metadata between steps (a sync
  that tries the source's hosts would fail).

Things that bit us:

- **A bug from query routing, found by this test:** a fresh session whose first statement was a
  multi-shard `INSERT ... SELECT` into a table behind a projection failed with "another command is
  already in progress". The capture trigger on the coordinator's own shards ran mid-COPY, and its
  first planned statement made `isomer_router` load its routes in an internal subtransaction,
  which Citus mirrors as `SAVEPOINT` on every open worker connection, busy with COPY. Routing now
  ignores statements nested inside another (only top-level application queries are routed);
  `tests/multinode.sh` covers the case.
- **`.pgpass` lives outside the data directory,** so pods cloned as standbys or restored never had
  one, and shard moves to a promoted coordinator would have failed. The init container writes it.
- **A restore's `restore_command` must not outlive the restore:** a restored node that later
  becomes a standby would otherwise fetch WAL from the source's archive. The init container
  removes the recovery settings once recovery has finished.
- **MinIO no longer publishes images**; the test uses SeaweedFS's S3 gateway, and creates the
  bucket with `weed shell` (a short-lived `kubectl run -i` loses its output to an attach race).

## 2026-10-12 — Operator: certificates between nodes, standbys and failover (FR-CL1)

`tests/operator.sh` passes on kind. It now also checks that workers refuse anything but the node
certificate, that every connection between nodes is encrypted and certificate-authenticated, that
a reissued certificate is picked up without a restart, and that a standby takes over when a
worker's or the coordinator's postmaster is frozen: Citus is repointed, every row stays readable,
SQL, MongoDB and HTTP clients carry on through the same Services, projections keep being
maintained, the old primary rejoins as a standby, and turning standbys off moves the primaries
back with nothing lost. See `operator/README.md` for how it works.

Things that bit us:

- **Kubernetes stops containers with SIGTERM, Postgres's smart shutdown.** It waits for every
  client to leave; Citus caches connections, so every pod deletion ran into the 60s grace period
  and was killed uncleanly. A `preStop` hook sends SIGINT (fast shutdown: well under a second,
  and a standby is sent all WAL first). The hook must only send the signal: one that waits for
  PID 1 to exit hangs itself until the grace period ends.
- **A rolling restart looked like a failure.** The pod is missing (or terminating) for a moment,
  and "missing" first triggered an immediate failover. Missing now gets the same
  `failoverAfter` grace as unready.
- **Turning standbys on restarted every primary** while the standbys were being cloned, because
  the replication settings were server arguments. The operator now sets
  `max_slot_wal_keep_size` and `synchronous_standby_names` with `ALTER SYSTEM` on each primary.
- **`citus_set_coordinator_host` refuses while a metadata node is out of sync,** which a worker
  failover a minute earlier leaves behind. Promotion now does only the promotion; every pass
  brings Citus's metadata in line with the topology and syncs it, so a refused step is retried.
- **A container's PID 1 ignores SIGSTOP sent from inside its namespace** (it is the namespace's
  init). The test freezes the postmaster from the kind node instead, via `crictl`, and must pick
  the ready sandbox: a recreated pod has two.
- **`pg_ident.conf` maps the node certificate to every role** with `isomer-nodes isomer-node all`
  (Postgres 16+), which is what Citus needs: it connects to workers as each client's own role.

Limits: one standby per node; a failover is decided by the operator's view of pod readiness, so
an operator that is down delays failover (the database keeps running). Logical replication slots
on the coordinator (cutover rollback) are not carried over to its standby.

## 2026-10-12 — Embeddings on the cluster's own CPU (FR-V1b)

`tests/local_model.sh` passes. A vector projection can now use `model => 'bge-small-en-v1.5'`:
the embedder computes it on its own CPU, with no API key, no network and no per-token cost. That
covers the free tier, development, and air-gapped clusters, which until now had only
`local-hash-64` (word hashing, no meaning).

- **Model:** BAAI/bge-small-en-v1.5 (MIT, 384 dimensions, ~130 MB), one of the two the PRD names.
  It is downloaded at a pinned revision during the image build, checked against SHA-256 sums, and
  shipped at `/usr/share/isomer/models/bge-small-en-v1.5` with its license. `ISOMER_MODEL_DIR`
  points the embedder at another directory of Hugging Face-layout models (`config.json`,
  `tokenizer.json`, `model.safetensors`); register one with provider `'local'`.
- **Runtime:** candle (pure Rust) and the `tokenizers` crate with its pure-Rust regex backend, so
  the image gains no native libraries. Inference runs on a blocking thread so a batch doesn't stall
  the async workers. The model loads once per process.
- **Checked against the reference implementation:** our vectors match sentence-transformers' to
  cosine > 0.9999 (the ignored unit test takes `ISOMER_REFERENCE_EMBEDDINGS`). The acceptance test
  ranks five listings by meaning with queries that share no content word with them.

Things that bit us:

- **candle 0.11 needs Rust 1.95** on aarch64 (it uses NEON f16 intrinsics that 1.89 calls
  unstable). The embedder now pins 1.95 in `rust-toolchain.toml` and its Dockerfile.
- **BGE pools the [CLS] token, not the mean.** A reference built from just the three weight files
  silently used mean pooling, because sentence-transformers reads pooling from `modules.json`;
  the comparison needs the repo's `1_Pooling/config.json`.

Limits: English only, 512 tokens per input (longer text is truncated), and throughput is whatever
the embedder's CPUs give: scale the embedder out for large backfills.

## 2026-10-12 — Console and CLI (FR-O3)

`tests/console.sh` passes. Operators get three things, all over the same admin API:

- **`/v1/admin/*` on the HTTP API** (`services/api/src/admin.rs`): overview, one projection in
  full, audit, repair, proposals (mine, accept, reject, route), routes on/off, the shard advisor,
  attachments and the event log. The SQL behind it lives in the extension
  (`isomer.admin_overview()`, `admin_projection()`, `admin_projections()`, isomer 0.10.0), so it
  can be checked from psql and the API stays a thin layer.
- **`isomer`, the CLI** (`clients/cli`, Node 20+, no runtime dependencies): `status`, `watch`,
  `projection`, `drift`, `alerts`, `audit`, `proposals`, `accept`, `route`, `routes`, `advice`,
  `attachments`, `events`, `get`. `--json` everywhere. Exit codes carry the outcome (`status` and
  `drift` exit 1 while something is wrong, 75 for "not yet"), so it slots into scripts and health
  checks.
- **The console** (`clients/console`, Preact, served at `/console`): every projection as a row
  whose lag travels toward its budget mark, drift counters, grouped alerts, a lag sparkline per
  projection, proposals with accept/reject/route, the advisor, routes, attachments and events.
  It loads no fonts or scripts from elsewhere, so it works air-gapped.

Access: a key may use the admin API only if its role is a member of the new `isomer_admin` role.
API keys never act as a superuser (that was already enforced by `create_api_key`), so membership
is the only way in. Keys for other roles get 403 from `/v1/admin/*` and keep their projection reads.

Things that bit us:

- **`isomer.repair` re-derives only the keys the auditor found drifted,** not the whole
  projection. The first labels said "re-derive every key"; the console and CLI now say "repair
  drifted keys".
- **`coalesce($1, 0)` makes Postgres type `$1` as `integer`,** and tokio-postgres then refuses to
  bind an `i64`, which the API reported as "database unavailable". Cast parameters explicitly.
- **Routing a proposal builds its own cache projection** (`route_<id>`) and points the proposal at
  it, and keeps the route disabled when the measured speedup is under
  `isomer.routing_min_speedup`. The console's routes page shows those and lets an operator turn
  them on anyway.
- **The event log repeats.** `isomer.alerts` includes every warning from the last 15 minutes, so a
  projection that breached its budget a few times listed each one. The console groups alerts by
  kind and subject with a count.

## 2026-10-12 — Read-your-writes without calling wait_for (FR-R4, FR-R4a, FR-R5)

`tests/freshness.sh` passes, five runs in a row. It closes the read-your-writes gaps the Phase 2
notes left open:

- **`SET isomer.read_after = <token>` works on plain SQL reads.** `isomer_router` now also hooks
  the executor. A top-level SELECT that reads a projection waits, through `isomer.wait_for`, until
  that projection reflects the token, or fails with IS001 ("not yet") after
  `isomer.read_after_timeout` (5s). Reads that touch no projection never wait. `SET LOCAL` works,
  so transaction-mode PgBouncer is fine. `isomer.get` and `isomer.cache_get` honour it as well.
- **MongoDB responses carry cluster time.** Writes return `operationTime` and `$clusterTime`
  (the cluster clock after the write). Reads of a projection return `operationTime` set to
  `isomer.freshness`: the newest token the projection reflects, which is the read's freshness.
- **Implicit causal reads** (`ISOMER_IMPLICIT_CAUSAL=1` on the gateway). A projection read that
  carries `$clusterTime` but no `afterClusterTime` waits for it. `$clusterTime` is only returned on
  writes, so a client's gossiped time is advanced by its own writes and nothing else.

Things that bit us:

- **Under READ COMMITTED the statement snapshot is taken before the executor starts.** Waiting in
  ExecutorStart and then reading with that snapshot returns the stale row: the test that resumes
  draining two seconds into a read caught exactly that when the fix was disabled. After a wait the
  hook unregisters the query's snapshot and registers a fresh one; `PortalRunSelect` pushes
  `queryDesc->snapshot`, so the run uses it. Only top-level statements wait (executor nesting is
  tracked), and never in REPEATABLE READ, which `wait_for` refuses with a reason.
- **Which relations are a projection.** Plan permission entries cover views and the tables under
  them, so the map (`isomer._projection_relations()`, loaded as the bootstrap superuser and cached
  for two seconds) lists each projection's view, its storage table and any native view.
  `documentdb_api.collection(db, name)` has no relation; the hook matches its constant arguments,
  and waits for every projection when they aren't constants.
- **`isomer.get` and `cache_get` were STABLE,** so their reads ran on the caller's snapshot.
  They are VOLATILE now and wait inside, before reading.
- **The Node driver only gossips a session's cluster time on a connection that holds one
  itself** (`prepareCommand`: `let a = this.clusterTime`, then the session's time if larger). A
  pooled connection that never saw a write sent nothing, so implicit reads passed or failed
  depending on the connection. In implicit mode the hello handshake now returns a zero
  `$clusterTime`, which starts every connection's clock without making anything wait.
- **The gateway's error-code proc macro bakes in `CARGO_MANIFEST_DIR`.** After the move out of
  `vendor/`, a cached build looked for the CSV at the old path; `cargo clean -p documentdb_macros`.

Limits:

- Implicit causal mode guarantees read-your-writes within a driver session. A read on an implicit
  session that lands on a connection which never carried the write's cluster time does not wait.
- The token is a single scalar, so a read waits for every change up to it on that projection, not
  only the caller's own.

## 2026-10-11 — Kubernetes operator on kind

`tests/operator.sh` passes on a local kind cluster (Kubernetes 1.36), with nothing spent on cloud
resources. One `IsomerCluster` resource brings up a coordinator, two workers and the HTTP API, all
Ready in about 25 seconds. Over the run, the test:

- serves SQL, MongoDB (through the gateway) and HTTP;
- scales to three workers online and back to two (drain, then remove) without losing a row;
- keeps the projection equal to recomputation throughout;
- survives coordinator and worker pod restarts on their volumes;
- cleans up everything on delete.

Design choices:

- **StatefulSets, not CloudNativePG.** CloudNativePG runs one Postgres primary with replicas. It
  has no model for Citus workers, and it replaces the image's entrypoint, while our first-boot
  setup lives in the official image's init scripts. The operator manages a coordinator
  StatefulSet and a worker StatefulSet directly. Each pod gets a stable DNS name from a headless
  Service that publishes addresses before the pod is ready. Citus and the init scripts must
  resolve nodes that are still starting.
- **The gateway is a sidecar.** It only connects to Postgres over the Unix socket, so it shares
  the coordinator pod and an `emptyDir` mounted at `/var/run/postgresql`.
- **Readiness means "isomer is installed"**, checked over TCP. The image's first-boot setup runs
  against a socket-only server, so a plain `pg_isready` over the socket would pass too early. A
  startup probe allows up to ten minutes for first boot.
- **Membership follows `spec.workers`, never the other way round.** Workers register only once
  their pod is ready, and one rebalance runs for a batch of new nodes, since Citus allows only one
  background rebalance at a time. A worker above the desired count is first marked
  `shouldhaveshards = false`, then drained by a drain-only rebalance and removed from Citus. Only
  then does the StatefulSet shrink: the worker StatefulSet keeps every pod still registered.
  Removed workers' volumes are deleted (`WhenScaled: Delete`), so a node added later starts empty
  instead of carrying stale Citus metadata.
- **`isomer.rebalance_finished()` runs after every finished rebalance job** (tracked in
  `status.lastRepairedJob`).
- **Server-side apply** for every child object, so defaults the API server adds don't cause
  update loops.
- **`kubectl scale isomer`** works through the scale subresource (`.spec.workers`).
- **Credentials** are generated once into `<name>-credentials` and never rewritten.

What bit us:

- **The current kubectl context on this machine is a production EKS cluster.** `kind create
  cluster` normally switches the global context to the new cluster. Everything here uses a
  separate kubeconfig (`operator/.kubeconfig-kind`, git-ignored), and the test exports it, so
  nothing can reach another cluster by accident.
- **`kind load docker-image` fails for multi-platform images** such as `mongo:8` ("content digest
  … not found"). The test lets the node pull that one from the registry instead.
- **A status can still read Ready just after a pod is deleted.** The restart checks wait for the
  replacement pod (a new uid) to be ready, then run a query over every shard.

Not done yet (also in `operator/README.md`): HA with standbys and failover, backups, certificate
authentication between nodes, and version upgrades.

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
  (see "Upstream code in `extensions/`" above).
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
- **DocumentDB version choice.** We imported the latest stable, v0.117-0. v1.0-RC2 was published
  on 9 Oct 2026. Re-import before we start modifying the gateway, so our changes sit on 1.0.
- **Disk.** A cold build needs roughly 10–15 GB of Docker space (Rust toolchains plus three
  compiles). The first attempt failed because the Mac's disk was full, which left Docker's VM
  read-only until Docker Desktop was force-restarted.
