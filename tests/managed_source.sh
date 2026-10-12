#!/bin/bash
# Managed Postgres providers (FR-A11): attach and cutover as an ordinary role holding only what
# managed providers grant (owner of its database and tables, REPLICATION; no superuser), and the
# provider check that recognises each provider and says what to change there.
#
# The stand-in source is plain Postgres. Providers are recognised by the roles they create, so the
# test creates those roles to see each one recognised; real providers still need their own run
# (credentials needed: see docs/engineering-notes.md).
#
#   docker compose --profile attach --profile test up -d && ./tests/managed_source.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
SRC="docker compose exec -T source psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
SRCDB="docker compose exec -T source psql -U postgres -d appdb -X -q -t -A -v ON_ERROR_STOP=1"
APP="host=source port=5432 dbname=appdb user=app password=app-password"

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=${4:-30} got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 1
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}
check() { $ISO -c "SELECT ok || '|' || coalesce(hint, '') FROM isomer.provider_check('$1') WHERE item = '$2'"; }

echo "== reset"
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'ms\_%';
SELECT isomer.detach(name, drop_tables => true) FROM isomer.attachments WHERE name LIKE 'ms\_%';
DROP SCHEMA IF EXISTS app CASCADE;
SQL
$SRC <<'SQL' >/dev/null
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'isomer_ms\_%';
DROP DATABASE IF EXISTS appdb WITH (FORCE);
DROP ROLE IF EXISTS app; DROP ROLE IF EXISTS app_norepl;
DROP ROLE IF EXISTS rds_superuser; DROP ROLE IF EXISTS rds_replication; DROP ROLE IF EXISTS cloudsqlsuperuser;
DROP ROLE IF EXISTS neon_superuser; DROP ROLE IF EXISTS tsdbadmin;
-- What a managed provider gives its customer: a login that owns its database and tables and may
-- replicate. Not a superuser.
CREATE ROLE app LOGIN REPLICATION PASSWORD 'app-password';
CREATE ROLE app_norepl LOGIN PASSWORD 'app-password';
CREATE DATABASE appdb OWNER app;
SQL
docker compose exec -T -e PGPASSWORD=app-password source psql -h localhost -U app -d appdb -X -q -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE SCHEMA app;
CREATE TABLE app.customers (id bigint PRIMARY KEY, name text NOT NULL, tier text);
CREATE TABLE app.orders (id bigserial PRIMARY KEY, customer_id bigint NOT NULL REFERENCES app.customers (id), total numeric NOT NULL);
INSERT INTO app.customers SELECT g, 'customer ' || g, CASE WHEN g % 3 = 0 THEN 'gold' ELSE 'std' END FROM generate_series(1, 50) g;
INSERT INTO app.orders (customer_id, total) SELECT 1 + g % 50, g FROM generate_series(1, 300) g;
GRANT CONNECT ON DATABASE appdb TO app_norepl;
SQL

echo "== the provider check, as the provider's customer"
expect "plain Postgres is recognised as such" "postgres" "$($ISO -c "SELECT DISTINCT provider FROM isomer.provider_check('$APP')")"
expect "nothing blocks an ordinary role that owns its database and may replicate" "0" \
  "$($ISO -c "SELECT count(*) FROM isomer.provider_check('$APP') WHERE blocking")"
expect "it may cut over: it owns the database" "true|" "$(check "$APP" cutover)"
contains "a role that may not replicate is told how to fix it" "false|As a superuser: ALTER ROLE app_norepl WITH REPLICATION" \
  "$(check "host=source port=5432 dbname=appdb user=app_norepl password=app-password" 'replication role')"
contains "and attach refuses before creating anything" "replication role" \
  "$($ISO -c "SELECT * FROM isomer.attach('ms_norepl', 'host=source port=5432 dbname=appdb user=app_norepl password=app-password', schemas => '{app}')" 2>&1 || true)"
expect "nothing was created on the source" "0" "$($SRC -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'isomer_ms_norepl'")"

echo "== each provider is recognised by the roles it creates"
for p in "rds_superuser|rds|GRANT rds_replication TO app_norepl" "cloudsqlsuperuser|cloud-sql|ALTER USER app_norepl WITH REPLICATION" \
         "neon_superuser|neon|Neon console" "tsdbadmin|tiger-cloud|"; do
  IFS='|' read -r role name hint <<< "$p"
  $SRC -c "CREATE ROLE $role NOLOGIN" >/dev/null
  [ "$role" = rds_superuser ] && $SRC -c "CREATE ROLE rds_replication NOLOGIN" >/dev/null
  expect "$role means $name" "$name" \
    "$($ISO -c "SELECT DISTINCT provider FROM isomer.provider_check('host=source port=5432 dbname=appdb user=app_norepl password=app-password')")"
  if [ -n "$hint" ]; then
    contains "and the replication hint is the provider's" "$hint" \
      "$(check "host=source port=5432 dbname=appdb user=app_norepl password=app-password" 'replication role')"
  else
    expect "Tiger Cloud is refused: it allows no logical replication out" "t" \
      "$($ISO -c "SELECT blocking FROM isomer.provider_check('host=source port=5432 dbname=appdb user=app_norepl password=app-password') WHERE item = 'provider'")"
  fi
  $SRC -c "DROP ROLE $role" >/dev/null
  [ "$role" = rds_superuser ] && $SRC -c "DROP ROLE rds_replication" >/dev/null
done
$SRC -c "CREATE ROLE rds_superuser NOLOGIN; CREATE ROLE rds_replication NOLOGIN; GRANT rds_replication TO app_norepl" >/dev/null
expect "on RDS, membership of rds_replication is the right to replicate" "true|" \
  "$(check "host=source port=5432 dbname=appdb user=app_norepl password=app-password" 'replication role')"
$SRC -c "DROP ROLE rds_replication; DROP ROLE rds_superuser" >/dev/null

echo "== attach, follow and cut over as the provider's customer"
contains "attach as the ordinary role" "attached=2" \
  "$($ISO -c "SELECT string_agg(status || '=' || n, ' ') FROM (SELECT status, count(*) n FROM isomer.attach('ms_app', '$APP', schemas => '{app}') GROUP BY status) s")"
eventually "the copy finishes" "300" "$ISO -c 'SELECT count(*) FROM app.orders'" 60
$ISO -c "SELECT isomer.create_projection(name => 'ms_card', kind => 'document', key => 'id', database => 'ms',
  query => 'SELECT c.id, jsonb_build_object(''name'', c.name, ''orders'', (SELECT count(*) FROM app.orders o WHERE o.customer_id = c.id)) AS doc FROM app.customers c',
  sources => '{\"app.customers\": \"id\", \"app.orders\": \"customer_id\"}')" >/dev/null
docker compose exec -T -e PGPASSWORD=app-password source psql -h localhost -U app -d appdb -X -q \
  -c "UPDATE app.customers SET name = 'renamed at the source' WHERE id = 7" -c "INSERT INTO app.orders (customer_id, total) VALUES (7, 1)" >/dev/null
eventually "writes at the source reach the projection" "renamed at the source 7" \
  "$ISO -c \"SELECT (isomer.get('ms_card', '7') ->> 'name') || ' ' || (isomer.get('ms_card', '7') ->> 'orders')\"" 30
steps=$($ISO -c "SELECT string_agg(step, ' > ' ORDER BY ord) FROM isomer.cutover('ms_app') WITH ORDINALITY AS c(step, detail, ord)" 2>&1 || true)
expect "cutover runs as the ordinary role" "freeze source > catch up > sequences > defaults > stop replication > done" "$steps"
expect "the source is now read-only for its applications" "on" \
  "$(docker compose exec -T -e PGPASSWORD=app-password source psql -h localhost -U app -d appdb -X -t -A -c 'SHOW default_transaction_read_only')"
expect "IsomerDB takes the writes" "1" "$($ISO -c "INSERT INTO app.customers VALUES (51, 'after cutover', 'std') RETURNING 1")"
echo "managed_source: all checks passed"
