# Research: name clearance and the term "shapes"

Researched 11 October 2026. This is web research, not a legal trademark search. Trademark data
comes from third-party aggregators; GitHub and package-registry checks were run live.

## Q1. Is "Prism DB" / "PrismDB" usable?

**Risk: HIGH for "PrismDB" / "Prism DB"; medium-high for bare "Prism" in software.**

### Direct conflicts

- **An active open-source database called PrismDB with nearly our pitch.**
  `github.com/HafizMMoaz/prism-db`:
  - Apache-2.0, created June 2026, last push 11 Sep 2026, about 10 stars.
  - It describes itself as "a single-node, multi-model database engine: relational tables (SQL),
    JSON-like documents, and ordered key-value pairs… atomically".
  - Distributed as PyPI `prismdb` 0.2.0, Homebrew tap `HafizMMoaz/prism/prismdb`, apt/dnf repos, a
    PHP SDK, and binaries `prismd` / `prism-shell`. Site: https://hafizmmoaz.github.io/prism-db/
  - It is small, but it has the same name in the same category and holds the PyPI name.
- **PrismDB research system (Princeton/Columbia).** Published at ASPLOS '23 as "Efficient
  Compactions between Storage Tiers with PrismDB" (arXiv 2008.02352). It is the top search result
  in database circles. https://www.cs.princeton.edu/~mfreed/docs/prismdb-asplos23.pdf
- Minor uses:
  - RubyGem `prismdb` (prickathon, a Japanese fan-data platform).
  - npm `prism.db` (deprecated).
  - Aprismatic "Prisma/DB", a Postgres encryption proxy, stale since about 2020.

### Prisma (the most serious commercial risk)

- Prisma Data, Inc. owns the PRISMA word mark, US Reg. No. 6245908, Class 9 software development
  tools.
- It sells **Prisma Postgres**, a managed Postgres service.
- "Prism" vs "Prisma" differ by one letter, in the same goods and the same Postgres buyers: a
  classic likelihood-of-confusion argument.
- Source: https://uploads5.craft.co/uploads/operating_source/document/982769/ec991bfcdb7aafab.pdf

### Other PRISM marks and noise

- Northrop Grumman PRISM, US Serial 98723781 (pending, computer products).
- PRISMML (2026, AI software).
- Prism Data (fintech), Stoplight Prism (API mock server).
- The NSA PRISM programme is a poor association for a database.

### Fallback names

npm, crates.io and PyPI were all free for these names; none has been trademark-checked.

| Name | Findings | Risk |
|---|---|---|
| **RefractDB** (top pick: keeps the prism/light metaphor) | No repos, packages or products; nearest is Refact.ai, a different word | Low |
| Lensbase | One parked repo; Mirantis "Lens" is adjacent dev tooling | Low–medium |
| ParallaxDB | One tiny C++ prototype; "Parallax" ACM 2021 research system | Medium-low |
| FacetDB | Dormant .NET repo; "facet" implies faceted search, which clashes with our search shapes | Medium |
| TesseraDB | Two tiny Rust DB repos; ConsenSys Tessera | Medium |

### What a lawyer must still check

- USPTO knock-out and clearance search in Classes 9 and 42 (and 35 if we sell services), including
  phonetic variants.
- EUIPO, TMview, WIPO Global Brand Database, plus UK, India and Singapore if they are target
  markets.
- Common-law use, which is what protects the prism-db project.
- Live status of Prisma's registrations and whether Prisma has opposed similar marks.
- Domains and social handles.

## Q2. Should we use the term "shapes"?

**Risk: HIGH confusion.**

- A "Shape" is ElectricSQL's flagship concept: a client-requested, single-table sync subset of
  Postgres data.
  - https://electric-sql.com/docs/guides/shapes
  - `electric-sql/electric` has about 10.4k stars and is active.
  - TanStack DB builds on Electric shapes.
- Competitors define their own terms against it: PowerSync maps "Electric Shapes → Sync Streams";
  Zero says "queries".
- **Databricks acquired Electric on 11 Aug 2026.** The team joins Databricks' Neon/Lakebase group,
  so "shapes" is set to become a Databricks Postgres term in our direct market.
  - https://thenewstack.io/databricks-electric-wasm-agentic-postgres/
- The meaning differs: Electric's shapes are client sync subsets; ours are server-maintained,
  multi-model read models. Buyers who know Electric would assume we are a sync engine.

### Alternatives

| Term | Notes |
|---|---|
| **Projections** | The CQRS / event-sourcing term for derived, maintained read models of a source of truth. Used for Postgres-backed read models: Marten (https://martendb.io/tutorials/read-model-projections), KurrentDB. Not owned by any one vendor. |
| Materialized views | Collides with native Postgres `MATERIALIZED VIEW` (full refresh) and MongoDB views over the wire. |
| Sync streams / queries / caches | Vendor-specific (PowerSync, Zero, ReadySet) or too narrow. |
| Lenses | Implies bidirectional, writable transforms; misleading for read-only derived data. |

**Recommendation:** use **projections**, with typed variants ("document projection", "search
projection", and so on).

## Not verified

- Live USPTO/EUIPO status of every mark above.
- Whether the prism-db author has filed a trademark.
- Trademark status of the fallback names.
