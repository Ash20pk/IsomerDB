# Contributing to IsomerDB

Thanks for wanting to help. Bug reports, fixes, tests and docs are all welcome. For anything
bigger than a fix, open an issue first so we can agree on the approach before you write it.

## Before your first pull request

You'll be asked to accept the [Contributor License Agreement](CLA.md), once, by commenting on
your pull request. You keep the copyright in your work. The agreement lets us keep distributing
it, including under other licenses, and it promises your contributions stay available as open
source.

## Where things are

| Directory | What | License |
|---|---|---|
| `extensions/isomer/` | The `isomer` extension: SQL, one file per release (`isomer--X--Y.sql`) | AGPL-3.0 |
| `extensions/isomer_router/` | The C side: the planner hook, query analysis, the incremental engine | AGPL-3.0 |
| `extensions/sharding/`, `documents/`, `ivm/` | Citus, DocumentDB and pg_trickle, built from source, lightly changed | their own |
| `services/` | The HTTP API and console, the edge replica, the embedder (Rust) | AGPL-3.0 |
| `operator/` | The Kubernetes operator (Go) | AGPL-3.0 |
| `controlplane/` | The cloud control plane (Go), on hold for now | AGPL-3.0 |
| `clients/` | The SDK, CLI and console front end (TypeScript) | Apache-2.0 |
| `jepsen/` | The Jepsen tests (Clojure) | Apache-2.0 |
| `tests/`, `scripts/` | Acceptance suites, one per area | AGPL-3.0 |

`docs/PRD.md` is the plan, and `docs/engineering-notes.md` records what we learned building each
part. Read the notes for the area you're changing: they explain decisions the code alone doesn't.

## Building and testing

You need Docker with Compose v2, plus Go 1.24 and Node 22 for the operator and the clients.

```sh
docker compose up --build -d         # the first build compiles Citus, DocumentDB and pg_trickle
./scripts/smoke.sh
./tests/subqueries.sh                # any one suite, against whatever stack is up
./scripts/test-all.sh single         # every single-node suite, on a fresh stack
./scripts/test-all.sh cluster        # the three-node cluster suites
./tests/jepsen.sh subqueries         # a Jepsen workload, faults on the workers
```

`test-all.sh` deletes this Compose project's volumes. If ports 5432, 10260 or 8080 are taken on
your machine, set `ISOMER_PG_PORT`, `ISOMER_MONGO_PORT` and `ISOMER_API_PORT`, and point the
suites at the API with `ISOMER_API=http://localhost:<port>`.

On a slow machine, `ISOMER_TIMEOUT_SCALE=3` makes every suite wait three times as long for
projections to catch up. It never changes what a check expects. CI runs the Go and TypeScript
tests and every single-node suite on each pull request, with that scale. The cluster suites and
Jepsen run nightly.

## Changing the extension

- **A released upgrade script is never edited.** Changes go in a new
  `extensions/isomer/isomer--<last>--<next>.sql`, with `default_version` in `isomer.control`
  bumped to match. Existing databases reach the new version with `ALTER EXTENSION isomer UPDATE`,
  so the script must work on top of every earlier one.
- **Test the upgrade, not only a fresh install:** run the suite on a database created at the
  previous version, after `ALTER EXTENSION isomer UPDATE`.
- **Add the test with the change**, in the suite for that area. Each check says what it shows
  (`ok   an order moving to another customer takes its lines with it`).
- **Anything that can't be supported is refused, with the reason,** rather than maintained
  wrongly. A refusal gets a test too.

## Changing upstream code

Changes inside `extensions/sharding/`, `documents/` and `ivm/` follow the rules in
[engineering notes, Upstream code](docs/engineering-notes.md#upstream-code-in-extensions): keep
every upstream LICENSE and NOTICE, mark each change with an `IsomerDB:` comment, and prefer hooks
and SQL to editing the code. Add the change to the table there.

## Commits and pull requests

- One change per pull request, with its tests and docs.
- A commit message says what the change does, in a sentence, the way the history reads: "Anchored
  subqueries: an order's lines distributed with the customer of their order".
- For a change in behaviour, add an entry to `docs/engineering-notes.md`: what it does, why, and
  what you found along the way.

## Reporting bugs

Open an issue with the version (`SELECT extversion FROM pg_extension WHERE extname = 'isomer'`),
what you ran, what you expected and what happened. A query and a few rows that reproduce it help
most. Security problems go through [SECURITY.md](SECURITY.md) instead.
