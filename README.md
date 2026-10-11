# IsomerDB

One database. Write once, read in any projection.

SQL tables stay the single source of truth. Documents, search, vectors and cache are declared
*projections* that IsomerDB builds, shards and keeps consistent incrementally, readable over the
Postgres and MongoDB wire protocols. Adoption starts by attaching to the Postgres a team already
runs, with no migration.

## Contents

| Path | What it is |
|---|---|
| [`PRD.md`](PRD.md) | Product requirements document |
| [`research/`](research/) | Research behind the PRD's open questions: naming, DDL and read-your-writes tokens, embeddings and attach-mode providers, pricing / design partners / NFR targets |
| [`reference/system-design-draft-3.html`](reference/system-design-draft-3.html) | The system design the PRD is based on (written under the earlier working name "Prism") |
