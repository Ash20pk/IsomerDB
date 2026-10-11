# Vendored upstream code

We copy (not fork, not submodule) the upstream projects we modify, so their source lives in this
repo and our changes show up as ordinary diffs. Projects we use unmodified are pinned by version in
`docker/Dockerfile` instead.

## Copied into `vendor/`

| Directory | Upstream | Version | Commit | License |
|---|---|---|---|---|
| `citus/` | https://github.com/citusdata/citus | v14.2.0 | `58702e722c6efbae45e3cade885b476e4fddc7e1` | AGPL-3.0 |
| `documentdb/` | https://github.com/documentdb/documentdb | v0.117-0 | `28cc012070ce798361ed265251ec159180b7889f` | MIT |
| `pg_trickle/` | https://github.com/trickle-labs/pg-trickle | v0.108.3 | `ce709f06327b4801f79fc48934bc96107b450907` | Apache-2.0 |

DocumentDB v0.117-0 builds against Citus commit `fe25f68`, which v14.2.0 includes (41 commits
ahead, 0 behind).

## Our changes

| Project | Change | Why |
|---|---|---|
| pg_trickle | CDC triggers are created `ENABLE ALWAYS` (`cdc/mod.rs`, `cdc/rebuild.rs`, `api/helpers.rs`) | Logical-replication apply workers only fire `ALWAYS`/`REPLICA` triggers, so stream tables over attached tables never updated |
| pg_trickle | The DDL tracker treats `ENABLE [ALWAYS\|REPLICA] TRIGGER` as harmless (`hooks.rs`) | Otherwise enabling a trigger suspends every stream table on that source |
| pg_trickle | The "logical replication target" warning only fires in statement-level CDC mode (`api/helpers.rs`) | With row-level CDC and `ALWAYS` triggers, replicated changes are captured |

| DocumentDB gateway | New `isomer` module (`documentdb_gateway_core/src/isomer.rs`), called from `processor/process.rs` before each request | Reads with `readConcern.afterClusterTime` on an IsomerDB projection wait for it (`isomer.wait_for`); writes to projection collections are refused with code 166, as for a MongoDB view |

All are marked `IsomerDB:` in the source. The pg_trickle changes are worth proposing upstream as an
opt-in setting.

## Pinned, not copied

| Component | Version | Source | License |
|---|---|---|---|
| PostgreSQL | 18.6 | `postgres:18.6-bookworm` image | PostgreSQL |
| pg_search (ParadeDB) | 0.26.1 | release `.deb` | AGPL-3.0 |
| pgvector | PGDG `postgresql-18-pgvector` | apt | PostgreSQL |
| pg_cron, PostGIS | PGDG packages | apt (DocumentDB dependencies) | PostgreSQL / GPL-2.0 |

## Rules

1. **Each import is its own unmodified commit** ("Vendor <name> <tag> …"). To see everything we
   have changed in a project: `git diff <import-commit> -- vendor/<name>`.
2. **Keep every upstream LICENSE and NOTICE file.** Never remove or edit them.
3. **Mark our changes** in vendored files with a short `IsomerDB:` comment at each change site.
4. **AGPL boundary.** Changes inside `vendor/citus` are AGPL-3.0 and must be published if we offer
   them as a service. Keep IsomerDB's own code in its own directories, talking to Citus through
   hooks and SQL rather than by editing Citus, unless there is no other way. See PRD §13.
5. **Updating to a new upstream version:** copy the new tag into a scratch checkout, commit it as a
   new "Vendor …" commit on a branch, then re-apply our changes. Update the table above in the
   same change.
