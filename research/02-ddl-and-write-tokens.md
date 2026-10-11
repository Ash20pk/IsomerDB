# Research: DDL surface and read-your-writes token transport

Researched 11 October 2026. Key claims were checked in source: postgres `REL_18_STABLE`,
`REL_19_STABLE` and `master`, and `documentdb/documentdb` at commit 76cc6cc (8 Oct 2026). Items
marked [unverified] come from memory or search snippets.

## Q1. Can an extension add `CREATE SHAPE` syntax?

**No. Postgres 18, 19 and master have no parser hook** (verified in `parser.c`).

- `exec_simple_query` calls `raw_parser()` directly. The only hooks run after parsing:
  `post_parse_analyze_hook` and `ProcessUtility_hook`. A new leading keyword is a syntax error
  before any extension code runs.
- Upstream attempts were never committed: "Hook for extensible parsing" (Rouhaud, 2021) and
  "Parser Hook" (Mlodgenski, 2021). Tom Lane objected that the hook swaps only the grammar and would
  force plugins to copy `gram.y`. No PG 19 or 20 revival found.
  - https://www.postgresql.org/message-id/20210501072458.adqjoaqnmhg4l34l@nol
  - https://www.postgresql.org/message-id/1023941.1631725020%40sss.pgh.pa.us

**Reloptions can't carry extension options either** (verified in `reloptions.c`). A namespaced
option such as `prism.x` on a matview errors with "unrecognized parameter namespace", and table
access methods have no reloptions callback in 18 or 19. Any `WITH (prism.*)` options must be
stripped in `ProcessUtility_hook` and stored in our own catalog, so pg_dump would not reproduce
them.

### What existing extensions do

| Extension | Approach |
|---|---|
| Citus | Function API (`create_distributed_table()`); also intercepts DDL in ProcessUtility to propagate it |
| pg_ivm | Function API (`create_immv()`) [from memory] |
| pg_trickle | Function API: `pgtrickle.create_stream_table(name, query, schedule, refresh_mode)` |
| TimescaleDB | `CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous)` intercepted in ProcessUtility; dump/restore needs pre/post-restore functions [from memory] |
| ParadeDB | `CREATE INDEX … USING bm25` — native syntax, because index AMs have an `amoptions` callback |

pg_trickle's own native-syntax plan (2026-02-25, PROPOSED) is the direct precedent:

- It rejects a parser fork, a table access method and an FDW.
- It picks three tiers: function API, then `CALL` wrappers, then matview `WITH (pgtrickle.stream =
  true)` via ProcessUtility.
- It adds `generate_dump()` / `restore_stream_tables()` for pg_dump.
- https://pgxn.org/dist/pg_trickle/0.9.0/plans/sql/PLAN_NATIVE_SYNTAX.html

### Options

| Option | ORMs / migration tools | pg_dump | Cost |
|---|---|---|---|
| **A. Function API** | Works in every raw-SQL migration | Needs catalog tables marked with `pg_extension_config_dump` plus a restore function | Lowest; stock Postgres |
| **B. Intercept `CREATE MATERIALIZED VIEW … WITH (prism.*)`** | Looks like DDL; schema-diff tools may "fix" the stripped options | Silently restores as a plain matview unless a restore step re-attaches it | Medium; must chain with Citus's hook |
| C. Proxy that rewrites `CREATE SHAPE` | Breaks direct psql / migration connections | Never round-trips | A second SQL parser to maintain |
| D. Fork Postgres | Native | Needs pg_dump changes | Defeats the stock-Postgres premise. Not viable. |

Hook order: Citus must be first in `shared_preload_libraries`. A library loaded later runs
outermost, so Prism sees statements before Citus does. Prism must push shapes to workers itself.
[reasoned, not tested]

### Recommendation

1. Make the **function API the canonical interface**:
   `SELECT prism.create_shape('customer_profile', $$SELECT …$$, kind => 'document', key => 'id', freshness => 'sync')`.
2. Add **matview interception** as optional sugar that translates into the function API.
3. Express `NEST` inside the SQL query (`jsonb_build_object`, `json_agg`, `JSON_ARRAYAGG`), not as
   new grammar.
4. Keep `CREATE SHAPE` as documentation notation only. No pgwire proxy.
5. From day one, keep shape definitions in an extension catalog registered with
   `pg_extension_config_dump`, plus `prism.dump_shapes()` / `prism.restore_shapes()`. This covers
   pg_dump and the fact that DDL isn't replicated.
6. Align option names with pg_trickle's planned `pgtrickle.*` options.

## Q2. Read-your-writes token through MongoDB drivers, without driver changes

### MongoDB driver behaviour (from the specs)

- **Cluster time gossip** (https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.md):
  - Drivers keep the highest `$clusterTime` seen in any non-SDAM response and send it on every
    command, at MongoClient and session level, with or without explicit sessions.
  - `lsid` is attached whenever the handshake reports `logicalSessionTimeoutMinutes`.
- **Causal consistency** (https://github.com/mongodb/specifications/blob/master/source/causal-consistency/causal-consistency.md):
  - On by default for **explicit** sessions only; it must be off for implicit sessions.
  - Drivers store `operationTime` and send `readConcern.afterClusterTime` on later reads, and since
    2026-05 on writes too.
  - Not sent by `runCommand` or after unacknowledged writes.
- So causal sessions need app code to use explicit sessions. **Gossip is the only fully automatic
  channel.**

### DocumentDB gateway today (verified in source)

- The handshake reports `isdbgrid` (a mongos-like topology) and `logicalSessionTimeoutMinutes: 30`,
  so drivers do send `lsid`.
- `operationTime` is returned only on hello, only when the change-streams GUC is on, and it is
  built from wall time. No normal response includes `$clusterTime` or `operationTime`; an incoming
  `$clusterTime` is ignored.
- In `requests/read_concern.rs`, a non-zero `afterClusterTime` returns `IllegalOperation` "not
  supported on this deployment", and `atClusterTime` returns `InvalidOptions`.
- **Trap:** once Prism returns `operationTime`, causal sessions start sending `afterClusterTime`
  and will fail, unless that check is replaced **in the same change**.

### Precedents

- **PG 19 `WAIT FOR LSN`** (beta; modes standby_replay / write / flush, primary_flush; `TIMEOUT`;
  `NO_THROW`). https://www.postgresql.org/docs/19/sql-wait.html. Before 19: poll
  `pg_last_wal_replay_lsn()`.
- **ReadySet:** no token; a per-cache policy plus an "opportunistic RYW" time window.
  https://readyset.io/docs/concepts/read-your-writes
- **CockroachDB** HLC `AS OF SYSTEM TIME`; **Aurora** `aurora_replica_read_consistency` [from
  memory].
- **Citus cluster clock:** HLC in 64 bits (42-bit ms + 22-bit counter), `citus_get_cluster_clock()`,
  `citus.enable_cluster_clock`.
  https://pgxn.org/dist/citus/src/backend/distributed/clock/README.html

### Recommended design

1. **Token = Citus cluster-clock HLC, not a raw LSN.** `afterClusterTime` is a single Timestamp
   (u32 seconds + u32 increment), so it can't hold a per-shard LSN vector. Map
   `t = ms / 1000`, `i = (ms % 1000) << 22 | counter`. The maximum `i` is 4,194,303,999 < 2^32,
   so the mapping is lossless and keeps order.
2. **Per-shard watermark.** Each shard records "shape S has applied everything up to HLC X". Idle
   nodes need a heartbeat (like MongoDB's no-op writer) so reads don't wait on shards with no
   writes. A per-node HLC → LSN map lets replicas use `WAIT FOR LSN` (PG 19) or polling (PG 18).
3. **Gateway:**
   - On writes, return `operationTime` and
     `$clusterTime = {clusterTime: <commit HLC>, signature: {hash: zeros, keyId: 0}}`.
   - Replace the IllegalOperation check: on bounded-shape reads, wait for the watermarks to reach
     `afterClusterTime`, with a timeout. On writes, accept it as a no-op.
4. **Zero-app-change mode via gossip.** Advance `$clusterTime` only from **that client's own
   writes**. The driver echoes it automatically, even with implicit sessions, and the gateway
   treats it as an implicit `afterClusterTime` on bounded-shape reads (behind a flag).
   - The token is self-contained, so gateways behind a load balancer need no shared state.
   - Caveat: a MongoClient shared across tenants waits longer than needed, but is never incorrect.
5. **`sync` shapes need no token.**
6. **SQL side:**
   - `prism.last_write_token()`, captured at or after commit.
   - GUC `prism.read_after`, plus `prism.wait_for(token, timeout)`.
   - Under transaction-mode PgBouncer, use `SET LOCAL` or a per-transaction call.

## Not verified

- Whether Citus advances the cluster clock for single-shard and non-2PC transactions, and what it
  costs.
- Whether every official driver (Java, C#, Go) echoes a zero signature without validating it. Not
  tested.
- PG 19 `WAIT FOR` syntax may change before release; pg_trickle is pre-1.0.
