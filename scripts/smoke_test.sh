#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${SCRIPT_DIR}/.."



echo "[smoke] Waiting for scheduler HTTP..."
for i in $(seq 1 60); do
  if curl -s http://localhost:8082/healthz >/dev/null 2>&1; then break; fi
  sleep 2
  if [ "$i" -eq 60 ]; then echo "[smoke] scheduler not ready"; exit 1; fi
done

echo "[smoke] Waiting for worker HTTP..."
for i in $(seq 1 60); do
  if curl -s http://localhost:8080/healthz >/dev/null 2>&1; then break; fi
  sleep 2
  if [ "$i" -eq 60 ]; then echo "[smoke] worker not ready"; exit 1; fi
done

echo "[smoke] Creating job"
JOB_JSON='{"schedule":"now","payload":{},"max_duration_seconds":60}'
JOB_ID=$(curl -s -X POST http://localhost:8082/jobs -d "$JOB_JSON" | sed -E 's/.*"(job_id|jobId)":"([^"]+)".*/\2/')
[ -n "$JOB_ID" ] || { echo "[smoke] Failed to parse job id"; exit 1; }
echo "[smoke] Created job $JOB_ID"

echo "[smoke] Enqueue run"
curl -s -X POST http://localhost:8082/jobs/$JOB_ID/run || true

echo "[smoke] Waiting for completion"
for i in $(seq 1 40); do
  HIST_COUNT=$(docker exec -t cassandra1 cqlsh -e "SELECT COUNT(*) FROM scheduler.job_history WHERE job_id='${JOB_ID}';" | grep -E "[0-9]+" | tail -1 | tr -d '\r')
  if [ "${HIST_COUNT}" != "0" ]; then echo "[smoke] History row present"; break; fi
  sleep 2
  if [ "$i" -eq 40 ]; then echo "[smoke] No history row found"; exit 1; fi
done

echo "[smoke] Metrics sample (worker):"; curl -s http://localhost:8080/metrics | head -n 10
echo "[smoke] Metrics sample (scheduler):"; curl -s http://localhost:8082/metrics | head -n 10

echo "[smoke] SUCCESS"
