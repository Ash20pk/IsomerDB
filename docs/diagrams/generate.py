"""Writes the README's C4 diagrams: docs/diagrams/<name>.svg and <name>-dark.svg.

    python3 docs/diagrams/generate.py

Status follows the code: "built" means implemented and covered by an acceptance test; "partial"
means part of it is; "planned" is in the PRD and not built yet. Keep it honest when things change.
"""

from pathlib import Path

from c4 import Diagram, Element as E

OUT = Path(__file__).parent


def context() -> Diagram:
    d = Diagram("IsomerDB: system context (C4 level 1)",
                "Who uses IsomerDB, and the systems it works with.", 1320, 850)
    d.add(E("dev", "person", "Application developer", 90, 100, 300,
            desc="Builds products on SQL tables, and reads them as documents, search, vectors and cache."))
    d.add(E("ai", "person", "AI feature builder", 510, 100, 300,
            desc="Adds similarity search and retrieval to existing apps."))
    d.add(E("sre", "person", "Platform engineer", 930, 100, 300,
            desc="Runs clusters, watches lag, drift and usage, and handles upgrades."))
    d.add(E("isomer", "system", "IsomerDB", 430, 400, 460,
            desc="One database. SQL tables are the source of truth; documents, search, vectors and cache are "
                 "projections it keeps consistent across shards, readable over the Postgres and MongoDB "
                 "wire protocols and HTTP."))
    d.add(E("pg", "external", "Existing Postgres", 32, 410, 250,
            desc="The team's current database (RDS, Cloud SQL, Neon, Supabase...). Followed with no migration."))
    d.add(E("llm", "external", "Embedding providers", 1038, 410, 250,
            desc="OpenAI-compatible embedding APIs, used for vector projections when a hosted model is chosen."))
    d.add(E("s3", "external", "Object storage", 60, 690, 280,
            desc="S3-compatible storage for continuous WAL archives and base backups."))
    d.add(E("billing", "external", "Billing system", 520, 690, 280,
            desc="Pulls metered usage: storage, node-hours, projection writes, embedding tokens, requests."))
    d.add(E("oncall", "external", "On-call alerting", 980, 690, 280,
            desc="PagerDuty, Slack or a webhook. Paged when a projection drifts or breaks its lag budget."))

    d.rel(("dev", "bottom", 0.5), ("isomer", "top", 0.15), "Reads and writes data, declares projections",
          "SQL, MongoDB wire, HTTP", label_at=0.42)
    d.rel(("ai", "bottom", 0.5), ("isomer", "top", 0.5), "Declares vector projections, searches by meaning",
          "SQL", label_at=0.42)
    d.rel(("sre", "bottom", 0.5), ("isomer", "top", 0.85), "Operates and observes",
          "console, CLI, Kubernetes", label_at=0.42)
    d.rel(("isomer", "left", 0.5), ("pg", "right", 0.5), "Follows it, then cuts over",
          "logical replication", label_at=0.5, label_dy=-36, label_w=120)
    d.rel(("isomer", "right", 0.5), ("llm", "left", 0.5), "Embeds changed text", "HTTPS",
          label_at=0.5, label_dy=-30, label_w=120)
    d.rel(("isomer", "bottom", 0.15), ("s3", "top", 0.6), "Archives WAL, base backups", "S3 API", label_at=0.5)
    d.rel(("billing", "top", 0.5), ("isomer", "bottom", 0.5), "Pulls usage", "HTTPS, CSV", label_at=0.5)
    d.rel(("isomer", "bottom", 0.85), ("oncall", "top", 0.4), "Pages on drift and lag", "HTTPS", label_at=0.5)
    return d


def containers() -> Diagram:
    d = Diagram("IsomerDB: containers (C4 level 2)",
                "What IsomerDB is made of, and how the parts talk to each other.", 1560, 1280)
    d.add(E("dev", "person", "Application", 90, 96, 300,
            desc="An app using Postgres or MongoDB drivers, or HTTP."))
    d.add(E("sre", "person", "Platform engineer", 990, 96, 300, desc="Operates the cluster."))
    d.boundary("IsomerDB", 30, 290, 1500, 790)

    # Front doors.
    d.add(E("gw", "container", "MongoDB gateway", 70, 340, 270, "Rust",
            desc="MongoDB wire protocol: projections read as collections, causal reads wait for writes."))
    d.add(E("api", "container", "HTTP API", 380, 340, 280, "Rust, axum",
            desc="Documents by key, time travel, change feeds, live streams; admin and usage."))
    d.add(E("edge", "container", "Edge replicas", 740, 340, 220, status="planned",
            desc="Projections served close to users, fed by the change feed."))
    d.add(E("ui", "container", "Console and CLI", 990, 340, 300, "TypeScript",
            desc="Lag against budget, drift, alerts, proposals, routes, usage."))

    # Data plane.
    d.add(E("coord", "container", "Coordinator", 70, 600, 430, "PostgreSQL 18, Citus, isomer extension",
            desc="Plans and routes SQL, holds the catalog, attaches to existing Postgres, maintains projections "
                 "(by key, or incrementally across shards), issues write tokens, audits for drift, proposes "
                 "projections, meters usage."))
    d.add(E("workers", "container", "Worker nodes", 600, 600, 280, "PostgreSQL 18, Citus",
            desc="Hold shards of tables and projections; capture each change (its keys, or its row deltas) "
                 "where the row is written."))
    d.add(E("embedder", "container", "Embedder", 1250, 600, 250, "Rust, candle",
            desc="Embeddings for vector projections: a hosted API, or bge-small on its own CPU."))

    # Control plane.
    d.add(E("operator", "container", "Kubernetes operator", 70, 870, 430, "Go",
            desc="Provisions and scales clusters, fails over to standbys, backs up and restores to a point in "
                 "time, rolls upgrades, issues node certificates."))
    d.add(E("cloud", "container", "Cloud control plane", 600, 870, 280, "Go",
            desc="Managed IsomerDB: sign-up, regions, plans, provisioning, usage and invoices."))

    # Outside.
    d.add(E("pg", "external", "Existing Postgres", 70, 1140, 260, desc="Followed in attach mode until cutover."))
    d.add(E("s3", "external", "Object storage", 380, 1140, 260, desc="WAL archive and base backups."))
    d.add(E("billing", "external", "Billing system", 700, 1140, 260, desc="Pulls metered usage."))
    d.add(E("llm", "external", "Embedding providers", 1250, 1140, 250, desc="OpenAI-compatible APIs."))

    x = lambda el, px: (px - d.elements[el].x) / d.elements[el].w   # the fraction of a box's width at px

    d.rel(("dev", "bottom", x("dev", 205)), ("gw", "top", x("gw", 205)), "Documents", "MongoDB wire", label_at=0.55)
    d.rel(("dev", "bottom", x("dev", 380)), ("api", "top", x("api", 520)), "Documents, feeds", "HTTPS",
          via=[(380, 312), (520, 312)], label_at=0.62)
    d.rel(("dev", "bottom", x("dev", 360)), ("coord", "top", x("coord", 360)), "SQL", "Postgres wire", label_at=0.82)
    d.rel(("sre", "bottom", 0.5), ("ui", "top", x("ui", 1140)), "Uses", label_at=0.5)
    d.rel(("gw", "bottom", 0.5), ("coord", "top", x("coord", 205)), "Queries; waits for write tokens", "SQL",
          label_at=0.45, label_w=130)
    d.rel(("api", "bottom", x("api", 470)), ("coord", "top", x("coord", 470)), "Queries", "SQL", label_at=0.45)
    d.rel(("ui", "bottom", 0.5), ("api", "bottom", x("api", 610)), "Admin API", "HTTPS",
          via=[(1140, 545), (610, 545)], label_at=0.3)
    d.rel(("edge", "left", 0.5), ("api", "right", 0.5), "Change feed", status="planned", label_at=0.5,
          label_dy=-24, label_w=50)
    d.rel(("coord", "right", 0.5), ("workers", "left", 0.5), "Queries, change pulls", "Citus, TLS",
          label_at=0.5, label_dy=-30, label_w=96)
    d.rel(("embedder", "bottom", x("embedder", 1300)), ("coord", "bottom", x("coord", 420)),
          "Claims jobs, writes vectors", "SQL", via=[(1300, 825), (420, 825)], label_at=0.62)
    d.rel(("embedder", "bottom", x("embedder", 1430)), ("llm", "top", x("llm", 1430)), "Embeds", "HTTPS", label_at=0.5)
    d.rel(("operator", "top", x("operator", 200)), ("coord", "bottom", x("coord", 200)),
          "Manages every node", "Kubernetes API", label_at=0.5)
    d.rel(("cloud", "left", 0.5), ("operator", "right", 0.5), "Creates clusters", "IsomerCluster",
          label_at=0.5, label_dy=-20, label_w=90)
    coord_y = d.elements["coord"].anchor("left", 0.85)[1]
    pg_y = d.elements["pg"].anchor("left", 0.5)[1]
    d.rel(("coord", "left", 0.85), ("pg", "left", 0.5), "Logical replication", "pgoutput",
          via=[(48, coord_y), (48, pg_y)], label_at=0.64, label_dx=64)
    d.rel(("operator", "bottom", x("operator", 450)), ("s3", "top", x("s3", 450)), "Node sidecars archive WAL, back up",
          "WAL-G, S3 API", label_at=0.5, label_w=150)
    d.rel(("billing", "top", x("billing", 905)), ("api", "bottom", x("api", 640)), "Usage", "HTTPS, CSV",
          via=[(905, 565), (640, 565)], label_at=0.07, label_dx=0)
    return d


def components() -> Diagram:
    d = Diagram("IsomerDB: inside the coordinator (C4 level 3)",
                "Components of the isomer extension and the router, inside each coordinator.", 1640, 1355)
    cols = {"adopt": 60, "proj": 370, "read": 680, "trust": 990, "auto": 1300}
    W = 270
    rows = [370, 545, 720, 895]

    d.add(E("pg", "external", "Existing Postgres", cols["adopt"], 96, W, desc="The team's current database."))
    d.add(E("clients", "container", "Applications, gateway, HTTP API", cols["read"], 96, W,
            desc="Everything that reads and writes through SQL."))
    d.boundary("Coordinator", 30, 300, 1580, 765, subtitle="[PostgreSQL 18, Citus, isomer extension, isomer_router]")

    # Adoption.
    d.add(E("attach", "component", "Attach", cols["adopt"], rows[0], W, "SQL, logical replication",
            desc="Preflights tables, follows an existing Postgres, applies schema changes, watches slots."))
    d.add(E("cutover", "component", "Cutover", cols["adopt"], rows[1], W, "SQL",
            desc="Makes IsomerDB the primary: sequences, defaults, checks; optional rollback replication."))
    d.add(E("plan", "component", "Cutover planner", cols["adopt"], rows[2], W, "SQL",
            desc="A per-team plan to primary: what the copy lacks, with SQL; who to repoint; the quietest hour."))
    # Projections.
    d.add(E("catalog", "component", "Catalog and compiler", cols["proj"], rows[0], W, "SQL",
            desc="create_projection and native DDL; checks freshness against co-location; certifies or rejects."))
    d.add(E("capture", "component", "Change capture", cols["proj"], rows[1], W, "Row triggers",
            desc="Queues the keys every write touches, or its row deltas, on whichever node holds the row."))
    d.add(E("maint", "component", "Maintenance", cols["proj"], rows[2], W, "SQL, IVM",
            desc="Re-derives documents and cache by key, refreshes search stream tables, queues embeddings."))
    # Reads.
    d.add(E("router", "component", "Query router", cols["read"], rows[0], W, "C planner hook",
            desc="Routes a keyed read onto a projection when it was measured faster and returns the same rows."))
    d.add(E("fresh", "component", "Freshness", cols["read"], rows[1], W, "SQL, C executor hook",
            desc="Write tokens from the cluster clock; wait_for and read_after make reads see their writes."))
    d.add(E("history", "component", "History and change feed", cols["read"], rows[2], W, "SQL",
            desc="Every version kept for a retention period; time travel and a gap-free feed in commit order."))
    # Correctness.
    d.add(E("audit", "component", "Auditor", cols["trust"], rows[0], W, "SQL",
            desc="Samples projections, recomputes them from the tables, counts drift, pins what differed."))
    d.add(E("cert", "component", "Certificates", cols["trust"], rows[1], W, "SQL",
            desc="Names the rules a projection relies on and checks them live."))
    d.add(E("alerting", "component", "Alert delivery", cols["trust"], rows[2], W, "SQL, Rust",
            desc="Tracks each alert from firing to resolved; sends both to PagerDuty, Slack or a webhook."))
    # Automation and operations.
    d.add(E("propose", "component", "Proposer and shard advisor", cols["auto"], rows[0], W, "SQL",
            desc="Mines the query log for repeated joins to project, and for keys that keep queries on one shard."))
    d.add(E("meter", "component", "Metering and admin", cols["auto"], rows[1], W, "SQL",
            desc="Usage per hour for billing; the views the console, CLI and admin API read."))
    d.add(E("cost", "component", "Strategy chooser", cols["auto"], rows[2], W, "SQL, EXPLAIN",
            desc="Picks key recompute or incremental maintenance per projection, from the plan of re-deriving one key."))
    d.add(E("engine", "component", "Incremental engine", cols["adopt"], rows[3], W, "C on the parse tree, SQL",
            desc="Folds row deltas into partial aggregates and the joined columns a query reads; re-derives "
                 "only the keys a change reaches, from that state."))

    # Neighbouring containers and systems.
    d.add(E("workers", "container", "Worker nodes", cols["proj"], 1135, W, "PostgreSQL 18, Citus",
            desc="Shards of tables and projections."))
    d.add(E("embedder", "container", "Embedder", cols["read"], 1135, W, "Rust", desc="Computes embeddings."))
    d.add(E("oncall", "external", "On-call alerting", cols["trust"], 1135, W, desc="Pages people."))

    x = lambda el, px: (px - d.elements[el].x) / d.elements[el].w
    mid = lambda el: d.elements[el].x + d.elements[el].w / 2
    y_of = lambda el, side, f: d.elements[el].anchor(side, f)[1]

    d.rel(("pg", "bottom", 0.5), ("attach", "top", 0.5), "Logical replication", "pgoutput", label_at=0.42)
    d.rel(("clients", "bottom", 0.5), ("router", "top", 0.5), "SQL", label_at=0.42)
    d.rel(("catalog", "bottom", 0.5), ("capture", "top", 0.5), "Installs triggers", label_at=0.5)
    d.rel(("capture", "bottom", 0.5), ("maint", "top", 0.5), "Keys to refresh", label_at=0.5)
    d.rel(("attach", "right", 0.5), ("capture", "left", 0.5), "Replicated rows", via=[(350, y_of("attach", "right", 0.5)),
          (350, y_of("capture", "left", 0.5))], label_at=0.5, label_dx=0, label_w=70)
    d.rel(("router", "bottom", 0.5), ("fresh", "top", 0.5), "Waits for write tokens", label_at=0.5)
    d.rel(("maint", "bottom", 0.85), ("workers", "top", 0.85), "Writes projection rows", "Citus", label_at=0.5)
    m_y = y_of("maint", "right", 0.7)
    d.rel(("maint", "right", 0.7), ("embedder", "top", 0.2), "Vector jobs", "queue",
          via=[(660, m_y), (660, 1100), (cols["read"] + 0.2 * W, 1100)], label_at=0.66, label_dy=-2, label_w=70)
    cl_y = y_of("capture", "left", 0.85)
    d.rel(("capture", "left", 0.85), ("engine", "top", 0.85), "Row deltas",
          via=[(345, cl_y), (345, 870), (cols["adopt"] + 0.85 * W, 870)], label_at=0.3, label_w=60)
    e_y = y_of("engine", "right", 0.5)
    d.rel(("engine", "right", 0.5), ("maint", "bottom", 0.3), "Keys a change reaches",
          via=[(cols["proj"] + 0.3 * W, e_y)], label_at=0.3, label_dy=-14, label_w=90)
    d.rel(("propose", "top", 0.5), ("catalog", "top", 0.5), "Accepted proposals become projections",
          via=[(mid("propose"), 335), (mid("catalog"), 335)], label_at=0.6, label_w=260)
    d.rel(("alerting", "bottom", 0.5), ("oncall", "top", 0.5), "Pages", "HTTPS", label_at=0.5)
    return d


def deployment() -> Diagram:
    d = Diagram("IsomerDB on Kubernetes (deployment)",
                "One IsomerCluster resource, as the operator runs it with standbys and backups.", 1600, 1040)
    d.boundary("Kubernetes cluster", 30, 96, 1540, 750)
    d.boundary("namespace isomerdb-system", 60, 140, 330, 330)
    d.add(E("operator", "container", "isomerdb-operator", 85, 185, 280, "Deployment, Go",
            desc="Reconciles IsomerCluster resources: pods, services, certificates, topology, failover, "
                 "backups, upgrades."))
    d.boundary("namespace of the cluster", 420, 140, 1120, 680)

    d.add(E("svc", "container", "Service <name>", 450, 185, 300, "ClusterIP: 5432, 10260",
            desc="Follows the primary coordinator (isomerdb.io/primary=true)."))
    d.add(E("apisvc", "container", "HTTP API", 790, 185, 330, "Deployment, Service <name>-api",
            desc="Stateless; scales out. Serves the console."))
    d.add(E("emb", "container", "Embedder", 1160, 185, 350, "Deployment",
            desc="Stateless; holds the CPU model; scales with embedding backlog."))

    d.boundary("node group: coordinator", 450, 400, 1060, 150)
    d.add(E("c0", "container", "<name>-coordinator-0", 470, 428, 480, "Pod: postgres, gateway, backup",
            desc="Primary. Its own volume; archives WAL; mTLS to every node."))
    d.add(E("c1", "container", "<name>-coordinator-replica-0", 1010, 428, 480, "Pod: postgres, gateway, backup",
            desc="Hot standby, streaming. Promoted if the primary stays down."))

    d.boundary("node group: worker i, for each worker", 450, 620, 1060, 150)
    d.add(E("w0", "container", "<name>-worker-i", 470, 648, 480, "Pod: postgres, backup",
            desc="Primary for its shards. Its own volume; archives WAL."))
    d.add(E("w1", "container", "<name>-worker-replica-i", 1010, 648, 480, "Pod: postgres, backup",
            desc="Hot standby, streaming."))

    d.add(E("secrets", "container", "Secrets and ConfigMaps", 85, 560, 280, "Kubernetes",
            desc="Credentials, the cluster CA and node certificates, authentication rules, and the topology "
                 "(which pod is each group's primary)."))

    d.add(E("clients", "person", "Applications", 85, 890, 280, desc="Inside or outside Kubernetes."))
    d.add(E("s3", "external", "Object storage", 1170, 905, 320, desc="Each node group's WAL archive and base backups."))

    x = lambda el, px: (px - d.elements[el].x) / d.elements[el].w
    d.rel(("clients", "top", 0.85), ("svc", "left", 0.5), "Postgres wire, MongoDB wire",
          via=[(323, 870), (405, 870), (405, d.elements["svc"].anchor("left", 0.5)[1])], label_at=0.08, label_dx=0, label_dy=0, label_w=170)
    d.rel(("svc", "bottom", x("svc", 700)), ("c0", "top", x("c0", 700)), "", label_at=0.5)
    d.rel(("c0", "right", 0.5), ("c1", "left", 0.5), "WAL stream", label_at=0.5, label_dy=-20, label_w=50)
    d.rel(("w0", "right", 0.5), ("w1", "left", 0.5), "WAL stream", label_at=0.5, label_dy=-20, label_w=50)
    d.rel(("c0", "bottom", x("c0", 780)), ("w0", "top", x("w0", 780)), "Citus over mTLS", label_at=0.45)
    d.rel(("apisvc", "bottom", 0.2), ("svc", "right", 0.5), "SQL", via=[(856, 360), (780, 360),
          (780, d.elements["svc"].anchor("right", 0.5)[1])], label_at=0.3)
    d.rel(("operator", "bottom", 0.5), ("secrets", "top", 0.5), "Issues and updates", label_at=0.5)
    d.rel(("w1", "bottom", 0.6), ("s3", "top", x("s3", 1300)), "WAL-G", "S3 API", label_at=0.5)
    return d


def main() -> None:
    for name, build in [("c4-1-context", context), ("c4-2-containers", containers),
                        ("c4-3-coordinator", components), ("c4-deployment", deployment)]:
        d = build()
        (OUT / f"{name}.svg").write_text(d.render("light"))
        (OUT / f"{name}-dark.svg").write_text(d.render("dark"))
        print("wrote", name)


if __name__ == "__main__":
    main()
