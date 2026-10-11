# IsomerDB operator

Runs `IsomerCluster` resources on Kubernetes: a Citus coordinator with the MongoDB gateway beside
it, Citus worker nodes, the HTTP API, and (optionally) the embedder. Keeps Citus's node membership
in step with `spec.workers`:

- **Scale up:** new worker pods start, are registered once ready (`isomer.add_node`), then one
  online rebalance spreads shards onto them.
- **Scale down:** the node is marked to hold no shards, a drain-only rebalance moves its shards
  off, the node is removed from Citus, and only then does its pod go (its volume is deleted with it).
- **After every rebalance:** IsomerDB's capture triggers are re-checked on moved shards
  (`isomer.rebalance_finished`).

```sh
kubectl apply -k operator/config                       # CRD, RBAC, operator (namespace isomerdb-system)
kubectl apply -f operator/config/samples/isomercluster.yaml
kubectl get isomer                                     # PHASE  WORKERS  READY  REBALANCE
kubectl scale isomer demo --replicas=3                 # add a worker online
kubectl get secret demo-credentials -o jsonpath='{.data.postgres-password}' | base64 -d
```

Clients connect to the `demo` Service: Postgres on 5432 and MongoDB on 10260. The HTTP API is
the `demo-api` Service on 8080.

## Local development on kind

`tests/operator.sh` creates a kind cluster named `isomer` with its **own kubeconfig**
(`operator/.kubeconfig-kind`), loads the images, installs the operator and runs the acceptance
test. Your current kubectl context is never used. To work with that cluster by hand:

```sh
export KUBECONFIG=$PWD/operator/.kubeconfig-kind
```

Regenerate the CRD, RBAC and deepcopy code after changing `api/` or the RBAC markers:

```sh
cd operator
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 object paths=./api/...
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 crd rbac:roleName=isomerdb-operator paths=./... \
  output:crd:dir=config/crd output:rbac:dir=config/rbac
```

## Not yet

- **High availability.** There is one coordinator and one pod per worker, with no standbys. A
  node's pod restarts on its own volume, but there is no failover while it is down.
- **Backups and point-in-time recovery.**
- **Production authentication between nodes.** Workers trust connections (as in docker-compose),
  and a NetworkPolicy admits only the cluster's own database pods. Production needs certificate
  authentication (`citus.node_conninfo`) or `pg_dist_authinfo`.
- **Version upgrades** of the image or the isomer extension (`ALTER EXTENSION isomer UPDATE` on
  every node).
