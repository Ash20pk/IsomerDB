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
| [`scripts/smoke.sh`](scripts/smoke.sh) | Checks every base component works in a running stack |
| [`docs/engineering-notes.md`](docs/engineering-notes.md) | What we learned building the stack (gateway auth, pg_trickle and Citus quirks) |

## Run locally

```sh
docker compose up --build -d     # Postgres on :5432, MongoDB wire gateway on :10260
./scripts/smoke.sh
```

- Postgres: `psql postgres://postgres:isomer@localhost:5432/postgres`
- MongoDB: `mongosh "mongodb://isomer:isomer@localhost:10260/?tls=true&tlsAllowInvalidCertificates=true"`

These are local development credentials only.
