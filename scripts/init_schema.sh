#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}/.."
CQL_FILE="${REPO_ROOT}/infra/schema.cql"
HOST="${CASS_HOST:-cassandra1}"
RETRIES=60
SLEEP=5

echo "[schema] Waiting for Cassandra at ${HOST}..."
for i in $(seq 1 $RETRIES); do
  if docker exec -t "${HOST}" cqlsh -e "DESCRIBE cluster" >/dev/null 2>&1; then
    echo "[schema] Cassandra ready."; break
  fi
  if [ "$i" -eq "$RETRIES" ]; then echo "[schema] Cassandra not ready after retries"; exit 1; fi
  sleep $SLEEP
done

echo "[schema] Applying ${CQL_FILE}"
docker exec -t "${HOST}" cqlsh -f /workspace/infra/schema.cql || {
  # Fallback: copy then apply if not mounted
  docker cp "${CQL_FILE}" "${HOST}:/tmp/schema.cql"
  docker exec -t "${HOST}" cqlsh -f /tmp/schema.cql
}

echo "[schema] Done"
