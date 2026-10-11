#!/bin/bash
# Backups and point-in-time recovery with the Kubernetes operator, on kind: a cluster archives
# every node's WAL and takes base backups to S3-compatible storage (SeaweedFS here), creates a
# cluster-consistent restore point, and a second cluster is restored to exactly that point.
#
#   ./tests/operator_backup.sh   # creates the kind cluster "isomer" if needed, builds and loads images
#
# Uses its own kubeconfig (operator/.kubeconfig-kind); the current kubectl context is never touched.
set -euo pipefail
cd "$(dirname "$0")/.."

export KUBECONFIG=$PWD/operator/.kubeconfig-kind
NS=isomer-backup
K="kubectl -n $NS"
S3_KEY=isomer-test-key
S3_SECRET=isomer-test-secret-0123456789
BUCKET=isomer-backups

pass() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"
  echo "-- state at failure --"
  $K get isomer,pods -o wide 2>&1 | sed 's/^/       /'
  kubectl -n isomerdb-system logs deploy/isomerdb-operator --tail=15 2>&1 | cut -c1-240 | sed 's/^/       /'
  exit 1
}
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=${4:-300} got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 3
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}
primary_of() { $K get pod -l "app.kubernetes.io/instance=$1,isomerdb.io/node=coordinator,isomerdb.io/primary=true" -o name | head -1 | cut -d/ -f2; }
sql_on() { local cl=$1; shift; $K exec -i "$(primary_of "$cl")" -c postgres -- psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1 "$@"; }

echo "== kind cluster, operator and an S3 store"
kind get clusters 2>/dev/null | grep -qx isomer || kind create cluster --name isomer --kubeconfig "$KUBECONFIG"
docker image inspect isomerdb:dev >/dev/null
docker build -q -t isomerdb-operator:dev operator >/dev/null
for img in isomerdb:dev isomerdb-operator:dev; do kind load docker-image "$img" --name isomer >/dev/null; done
kubectl apply -k operator/config >/dev/null
kubectl -n isomerdb-system rollout restart deploy/isomerdb-operator >/dev/null
kubectl -n isomerdb-system rollout status deploy/isomerdb-operator --timeout=180s >/dev/null
kubectl delete namespace $NS --ignore-not-found --wait=true >/dev/null
kubectl create namespace $NS >/dev/null
$K apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Secret
metadata: { name: s3 }
stringData:
  access-key-id: $S3_KEY
  secret-access-key: $S3_SECRET
  s3.json: |
    {"identities": [{"name": "isomer", "credentials": [{"accessKey": "$S3_KEY", "secretKey": "$S3_SECRET"}],
                     "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: s3 }
spec:
  selector: { matchLabels: { app: s3 } }
  template:
    metadata: { labels: { app: s3 } }
    spec:
      containers:
        - name: seaweedfs
          image: chrislusf/seaweedfs:4.48
          imagePullPolicy: IfNotPresent
          args: ["server", "-dir=/data", "-s3", "-s3.port=8333", "-s3.config=/etc/s3/s3.json", "-master.volumeSizeLimitMB=256"]
          ports: [{ containerPort: 8333 }]
          readinessProbe: { tcpSocket: { port: 8333 }, periodSeconds: 3 }
          volumeMounts: [{ name: config, mountPath: /etc/s3 }]
      volumes: [{ name: config, secret: { secretName: s3 } }]
---
apiVersion: v1
kind: Service
metadata: { name: s3 }
spec: { selector: { app: s3 }, ports: [{ port: 8333 }] }
EOF
$K rollout status deploy/s3 --timeout=180s >/dev/null
bucket() { $K exec deploy/s3 -- sh -c "echo '$1' | weed shell -master=localhost:9333" 2>/dev/null; }
eventually "the bucket exists" "yes" \
  "bucket 's3.bucket.create -name $BUCKET' >/dev/null; bucket s3.bucket.list | grep -q '^ *$BUCKET\b' && echo yes" 120

echo "== a cluster that backs itself up"
$K apply -f - >/dev/null <<EOF
apiVersion: isomerdb.io/v1alpha1
kind: IsomerCluster
metadata: { name: b1 }
spec:
  image: isomerdb:dev
  workers: 2
  storage: 2Gi
  deleteVolumes: true
  resources: { requests: { cpu: 200m, memory: 512Mi } }
  api: { enabled: false }
  backup:
    s3: { bucket: $BUCKET, endpoint: "http://s3:8333", forcePathStyle: true, credentialsSecret: s3 }
    fullEvery: 1h
    restorePointEvery: 1h
EOF
eventually "the cluster is Ready" "Ready" "$K get isomer b1 -o jsonpath='{.status.phase}'" 600
sql_on b1 >/dev/null <<'SQL'
CREATE SCHEMA shop;
CREATE TABLE shop.customers (customer_id bigint PRIMARY KEY, name text NOT NULL);
CREATE TABLE shop.orders (order_id bigint, customer_id bigint NOT NULL, status text, PRIMARY KEY (customer_id, order_id));
SELECT isomer.distribute(ARRAY['shop.customers', 'shop.orders']::regclass[], 'customer_id');
INSERT INTO shop.customers SELECT g, 'customer ' || g FROM generate_series(1, 300) g;
INSERT INTO shop.orders SELECT g, 1 + g % 300, 'open' FROM generate_series(1, 3000) g;
SELECT isomer.create_projection(
  name => 'customer_card', kind => 'document', key => 'customer_id', database => 'shop', lag => '2 seconds',
  query => $q$ SELECT c.customer_id, jsonb_build_object('name', c.name,
              'open', (SELECT count(*) FROM shop.orders o WHERE o.customer_id = c.customer_id AND o.status = 'open')) AS doc
              FROM shop.customers c $q$,
  sources => '{"shop.customers": "customer_id", "shop.orders": "customer_id"}');
SQL
backups() { $K exec "$1" -c backup -- isomer-walg backup-list 2>/dev/null | grep -c base_ || true; }
eventually "every node has a base backup" "1 1 1" \
  "echo \$(backups b1-coordinator-0) \$(backups b1-worker-0) \$(backups b1-worker-1)" 300
expect "and archives its WAL" "0" \
  "$(sql_on b1 -c "SELECT sum(failed_count) + (SELECT sum(r.result::bigint) FROM run_command_on_workers('SELECT failed_count FROM pg_stat_archiver') r) FROM pg_stat_archiver")"

echo "== a restore point, then more writes"
sql_on b1 >/dev/null <<'SQL'
INSERT INTO shop.customers SELECT g, 'customer ' || g FROM generate_series(301, 400) g;
UPDATE shop.customers SET name = 'before the point' WHERE customer_id = 7;
SELECT isomer.wait_for(isomer.last_write_token(), 'customer_card', '30 seconds');
SQL
POINT=$(sql_on b1 -c "SELECT isomer.create_restore_point('test')")
expect "isomer.create_restore_point names it by time" "isomer-" "${POINT:0:7}"
sql_on b1 >/dev/null <<'SQL'
INSERT INTO shop.customers SELECT g, 'customer ' || g FROM generate_series(401, 500) g;
UPDATE shop.customers SET name = 'after the point' WHERE customer_id = 7;
DELETE FROM shop.orders WHERE customer_id = 9;
SQL
POINT_AT=$(sql_on b1 -c "SELECT created_at FROM isomer.restore_points WHERE name = '$POINT'")
# Close the WAL segment holding the point on every node, so it is archived.
sql_on b1 -c "SELECT pg_switch_wal(); SELECT run_command_on_workers('SELECT pg_switch_wal()')" >/dev/null
archived_past() {
  sql_on b1 -c "SELECT (SELECT last_archived_time > '$POINT_AT' FROM pg_stat_archiver)
                 AND (SELECT bool_and(r.result::boolean) FROM run_command_on_workers(
                        \$q\$SELECT last_archived_time > '$POINT_AT' FROM pg_stat_archiver\$q\$) r)"
}
eventually "every node has archived past the point" "t" "archived_past" 180
SOURCE_ROWS=$(sql_on b1 -c "SELECT (SELECT count(*) FROM shop.customers) || '/' || (SELECT count(*) FROM shop.orders)")
expect "the source has the later writes" "500/2990" "$SOURCE_ROWS"

echo "== restore a new cluster to the point"
$K apply -f - >/dev/null <<EOF
apiVersion: isomerdb.io/v1alpha1
kind: IsomerCluster
metadata: { name: b2 }
spec:
  image: isomerdb:dev
  workers: 2
  storage: 2Gi
  deleteVolumes: true
  resources: { requests: { cpu: 200m, memory: 512Mi } }
  api: { enabled: false }
  restore:
    from: { bucket: $BUCKET, prefix: "$NS/b1", endpoint: "http://s3:8333", forcePathStyle: true, credentialsSecret: s3 }
    restorePoint: "$POINT"
    credentialsSecret: b1-credentials
EOF
eventually "the restored cluster is Ready, with Citus pointed at its own nodes" "Ready true" \
  "$K get isomer b2 -o jsonpath='{.status.phase} {.status.restored}'" 900
expect "it holds what the source held at the point" "400/3000" \
  "$(sql_on b2 -c "SELECT (SELECT count(*) FROM shop.customers) || '/' || (SELECT count(*) FROM shop.orders)")"
expect "including the write just before it" "before the point" "$(sql_on b2 -c "SELECT name FROM shop.customers WHERE customer_id = 7")"
expect "and knows the point it was restored to" "1" "$(sql_on b2 -c "SELECT count(*) FROM isomer.restore_points WHERE name = '$POINT'")"
expect "Citus names only the restored cluster's nodes" "0" "$(sql_on b2 -c "SELECT count(*) FROM pg_dist_node WHERE nodename NOT LIKE 'b2-%'")"
expect "the projection came back too, equal to recomputation" "0" "$(sql_on b2 -c "SELECT count(*) FROM isomer.verify('customer_card')")"
sql_on b2 >/dev/null <<'SQL'
UPDATE shop.customers SET name = 'written after the restore' WHERE customer_id = 8;
SELECT isomer.wait_for(isomer.last_write_token(), 'customer_card', '30 seconds');
SQL
expect "and keeps being maintained" "written after the restore" "$(sql_on b2 -c "SELECT isomer.get('customer_card', '8') ->> 'name'")"
MONGO_PW=$($K get secret b2-credentials -o jsonpath='{.data.mongo-password}' | base64 -d)
expect "MongoDB clients sign in with the source's credentials" "written after the restore" \
  "$($K run "mongosh-$RANDOM" --rm -i --restart=Never --quiet --image=mongo:8 --image-pull-policy=IfNotPresent -- \
     mongosh "mongodb://isomer:$MONGO_PW@b2:10260/?tls=true&tlsAllowInvalidCertificates=true" --quiet \
     --eval 'print(db.getSiblingDB("shop").customer_card.findOne({_id: 8}).name)' 2>/dev/null | tail -1)"
expect "the source is untouched" "$SOURCE_ROWS" \
  "$(sql_on b1 -c "SELECT (SELECT count(*) FROM shop.customers) || '/' || (SELECT count(*) FROM shop.orders)")"

echo "== cleaning up"
$K delete isomer b1 b2 --wait=true >/dev/null
kubectl delete namespace $NS --wait=false >/dev/null
echo "== operator backup passed"
