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

## Security between nodes

Every node serves TLS, and nodes authenticate to each other with a certificate rather than a
password. The operator creates a CA for the cluster (`<name>-ca`) and issues the nodes' server
certificate and the node client certificate (`<name>-tls`), renewing them a month before they
expire; nodes reload them without a restart. Citus (`citus.node_conninfo`), IsomerDB's own
connections between nodes, and replication all use `verify-full` and the node certificate, which
may act as any role (Citus connects to workers as each client's own role). Workers accept nothing
else; applications reach the coordinator with a password, as before. A NetworkPolicy also admits
connections to workers only from the cluster's own database pods and the operator.

## High availability

```yaml
spec:
  ha:
    standbys: 1          # a streaming hot standby beside the coordinator and every worker
    failoverAfter: 30s   # how long a primary may be unready before its standby takes over
    synchronous: false   # true: commits wait for the standby while it is streaming
    maxRetainedWAL: 4GB  # WAL a primary keeps for a disconnected standby
```

Each node is a group of two pods: pod *i* of `<name>-worker` and pod *i* of
`<name>-worker-replica` (and the two coordinator pods). Either may be the group's primary; the
`<name>-topology` ConfigMap records which, and `status.groups` shows each group's primary, standby,
whether it is streaming, and its replay lag.

- **Failover.** When a primary has been unready (or missing) for `failoverAfter` and its standby
  is ready, the operator records the standby as primary, deletes the old primary's pod (so it
  cannot keep serving), and promotes the standby. Citus is then pointed at it: a worker's entry
  in `pg_dist_node`, or the coordinator's own host. The client Service follows the primary
  coordinator, so applications, the MongoDB gateway and the HTTP API keep their addresses.
- **The old primary comes back as the standby.** Its pod's init container rewinds it onto the
  new primary (`pg_rewind`), or clones it afresh if it cannot.
- **Turning standbys off** moves every primary back to the main pods (a clean switchover, nothing
  lost), then removes the replicas. Turning them on or off never restarts a primary.
- **Asynchronous by default:** a failover can lose the last moments of commits. With
  `synchronous: true` a commit waits for the standby while it streams, so a failover loses
  nothing; while a standby is down, commits do not wait (rather than stopping writes).
- A standby that falls further behind than `maxRetainedWAL` loses its slot and is re-cloned.

`status.lastFailover` records the last promotion and why.

## Not yet

- **Backups and point-in-time recovery.**
- **Version upgrades** of the image or the isomer extension (`ALTER EXTENSION isomer UPDATE` on
  every node).
