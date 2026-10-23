# Self-hosting IsomerDB

IsomerDB is free to run, including commercially, under the AGPL-3.0 (see [LICENSE](../LICENSE)).
There are two ways to run it: Docker Compose on one machine, to try it or for development, and
the Kubernetes operator, for production.

Every release publishes these images for amd64 and arm64:

| Image | What it is |
|---|---|
| `ghcr.io/ash20pk/isomerdb` | Postgres 18 with Citus, DocumentDB, pg_trickle and the `isomer` extension; also the MongoDB gateway |
| `ghcr.io/ash20pk/isomer-api` | The HTTP API and the web console |
| `ghcr.io/ash20pk/isomer-embedder` | Embeddings on your own CPU (bge-small-en-v1.5), optional |
| `ghcr.io/ash20pk/isomer-edge` | An edge replica serving projections from memory, optional |
| `ghcr.io/ash20pk/isomerdb-operator` | The Kubernetes operator |

## Try it on one machine

You need Docker with Compose v2.

```sh
git clone https://github.com/Ash20pk/IsomerDB.git && cd IsomerDB
ISOMER_REGISTRY=ghcr.io/ash20pk/ ISOMER_TAG=0.28.0 docker compose up -d --no-build
./scripts/smoke.sh
```

That pulls the released images. To build from source instead, run `docker compose up --build -d`.
The first build takes a while, since Citus, DocumentDB and pg_trickle all compile.

What you get:

| | Address | Credentials (local only) |
|---|---|---|
| Postgres | `localhost:5432` | `postgres` / `isomer` |
| MongoDB wire protocol | `localhost:10260` | `isomer` / `isomer` |
| HTTP API and console | `http://localhost:8080` | an API key from `SELECT isomer.create_api_key(...)` |

If something on your machine already uses one of those ports, move it:
`ISOMER_PG_PORT=55432 ISOMER_MONGO_PORT=20260 ISOMER_API_PORT=58080 docker compose up -d`.

To try a cluster, add two Citus workers and register them:

```sh
docker compose --profile cluster up -d
psql postgres://postgres:isomer@localhost:5432/postgres \
  -c "SELECT isomer.add_node('worker1')" -c "SELECT isomer.add_node('worker2')"
```

From here, [Using it](../README.md#using-it) in the README walks through attaching an existing
Postgres and creating projections.

The Compose setup is for trying IsomerDB out. Its passwords are fixed, the workers accept
connections without one, and nothing is backed up. Don't put real data in it.

## Production on Kubernetes

The operator runs an `IsomerCluster`: a coordinator with the MongoDB gateway beside it, Citus
workers, the HTTP API and, if you want it, the embedder. Install it from a release:

```sh
kubectl apply --server-side -f https://github.com/Ash20pk/IsomerDB/releases/download/v0.28.0/isomerdb-operator.yaml
```

That installs the CRD, RBAC and the operator in the `isomerdb-system` namespace, with every image
pointing at that release. Then create a cluster:

```yaml
apiVersion: isomerdb.io/v1alpha1
kind: IsomerCluster
metadata:
  name: orders
spec:
  workers: 2
  storage: 100Gi
  ha:
    standbys: 1
  backup:
    s3:
      bucket: my-backups
      credentialsSecret: s3        # keys access-key-id and secret-access-key
  api:
    enabled: true
```

```sh
kubectl get isomer                                      # PHASE  WORKERS  READY  REBALANCE
kubectl get secret orders-credentials -o jsonpath='{.data.postgres-password}' | base64 -d
```

Applications connect to the `orders` Service: Postgres on 5432, MongoDB on 10260. The HTTP API
is the `orders-api` Service on 8080.

The [operator's README](../operator/README.md) covers the rest in detail:

- **Scaling:** `kubectl scale isomer orders --replicas=3` adds a worker and rebalances online.
  Scaling down drains a worker's shards before its pod goes.
- **High availability:** a streaming standby per node, failover after `failoverAfter`, and the
  old primary rewound and brought back as the standby.
- **Backups and point-in-time recovery:** WAL archiving and base backups to S3 or any
  S3-compatible store, with cluster-consistent restore points. A restore creates a new cluster.
- **Security between nodes:** TLS everywhere, nodes authenticated by certificate, a NetworkPolicy
  around the workers.

## Upgrading

**On Kubernetes**, set `spec.image` to the new release's `ghcr.io/ash20pk/isomerdb` tag. The
operator moves one node group at a time, standbys first with a clean switchover where there are
any, then runs `ALTER EXTENSION ... UPDATE` for every extension the new image has a newer version
of. `status.isomerVersion` shows where it got to.

**With Compose**, pull the new tag and restart. A new image doesn't update the extensions in an
existing database, so update them yourself:

```sh
ISOMER_REGISTRY=ghcr.io/ash20pk/ ISOMER_TAG=<new version> docker compose up -d --no-build
psql postgres://postgres:isomer@localhost:5432/postgres \
  -c "SELECT name, installed_version, default_version FROM pg_available_extensions WHERE installed_version <> default_version"
psql postgres://postgres:isomer@localhost:5432/postgres -c "ALTER EXTENSION isomer UPDATE"
```

Update Citus before `isomer` if both changed. Major Postgres versions (18 to 19) need
`pg_upgrade`, which neither path handles yet.

## Help

Bugs and questions go in [GitHub issues](https://github.com/Ash20pk/IsomerDB/issues). Security
problems don't: see [SECURITY.md](../SECURITY.md).
