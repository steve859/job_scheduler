# Job Scheduler (Cassandra LWT + Distributed Locks)

## Overview
This project provides a distributed job scheduler backed by Apache Cassandra using lightweight transactions (LWT) for safe, fencing-token based locking. A worker service exposes REST endpoints to create and run jobs and a background loop processes pending jobs while emitting Prometheus metrics.

## Components
- `client-lib/`: Java lock client (acquire / renew / release) using `locks`, `lock_seq`.
- `worker/`: Worker HTTP service + polling loop.
- `infra/`: Docker Compose stack (3-node Cassandra, Prometheus, Grafana, worker) and schema.
- `scripts/`: Helper scripts for starting stack, applying schema, smoke test.
 - `orders/`: Phase 3 Orders checkout + idempotency + optimistic inventory locking (Postgres)

## Quick Start
Prerequisites: Docker, Bash (Git Bash on Windows or WSL), Java 17, Maven.

```bash
./scripts/start.sh
./scripts/init_schema.sh
```

Verify containers:
```bash
docker ps
```

### Create & Run a Job
```bash
curl -X POST http://localhost:8080/jobs -d '{"schedule":"now","payload":"{}","max_duration_seconds":60}' -H 'Content-Type: application/json'
# => {"job_id":"<UUID>"}
curl -X POST http://localhost:8080/jobs/<UUID>/run
```

Check metrics:
```bash
curl http://localhost:8080/metrics
```

Smoke test end-to-end:
```bash
./scripts/smoke_test.sh
```

Stop stack:
```bash
./scripts/stop.sh
```

## REST Endpoints
- `POST /jobs` -> create job (returns job_id)
- `GET /jobs/{id}` -> fetch job (minimal response)
- `POST /jobs/{id}/run` -> enqueue job into `jobs_sharded`
- `GET /metrics` -> Prometheus metrics
- `GET /healthz` -> health check (queries Cassandra; returns 503 if unhealthy)
- `GET /readyz` -> readiness gate (returns READY once server and poller initialized)
 - `POST /checkout` (orders service): create or return existing order via idempotency key; performs inventory optimistic lock.

## Locking Semantics
Fencing tokens are generated via CAS on `lock_seq`. Lock acquisition stores `fencing_token` and `expire_at`. Expired locks are taken over by incrementing `epoch` (CAS on `epoch`). Clients use their token to ensure ordered execution.

## Schema Application
`scripts/init_schema.sh` waits for Cassandra readiness and applies `infra/schema.cql` idempotently.

## Prometheus & Grafana
Prometheus scrapes:
- Worker metrics (`worker1:8080/metrics`)
- Cassandra exporters (placeholder ports)

Worker metrics (Phase 2/3):
- `jobs_processed_total{status}`: total jobs processed by status (success/failed)
- `jobs_in_progress`: current running jobs
- `job_duration_seconds`: histogram of job execution time
- Lock metrics include LWT latency histogram and operation counters
- `jobs_dlq_total`: total jobs moved to DLQ

Grafana:
- Container starts with persistent volume `grafana_data`.
- Import the starter dashboard JSON at `infra/grafana/dashboards/job_scheduler_overview.json` to visualize throughput, durations, lock LWT latency, DLQ insert rate, and in-progress cap.

Suggested alerts:
- DLQ spike: `sum(rate(jobs_dlq_total[5m])) > 0.5` for 10m
- Lock LWT latency: `histogram_quantile(0.99, sum(rate(lock_lwt_latency_seconds_bucket[5m])) by (le)) > 0.5`s for 10m
- Saturation: `jobs_in_progress` near `WORKER_MAX_IN_PROGRESS` for sustained 10m

## Common Issues
| Issue | Cause | Fix |
|-------|-------|-----|
| Prometheus mount error | Wrong relative path | Ensure compose run from `infra/` so path `./prometheus/prometheus.yml` resolves |
| Lock takeover not occurring | `expire_at` still in future | Lower TTL or adjust client clock |
| RF=3 warning | Fewer Cassandra nodes up | Start all 3 nodes or lower RF for dev |

## Development
Build all modules:
```bash
mvn -q -DskipTests install
```

Run worker locally (without Docker):
```bash
java -jar worker/target/worker-1.0-SNAPSHOT.jar
```

### Phase 3 Runtime Hardening
Endpoints:
- `/readyz` indicates readiness after session, HTTP server, and poller init.
- `/healthz` performs a lightweight Cassandra query; returns 503 if not healthy.

Graceful shutdown:
- The worker stops polling and drains in-flight jobs (up to 30s), then closes the session and HTTP server.

Backpressure:
- Global cap via `WORKER_MAX_IN_PROGRESS` (default `32`). Poller pauses when in-progress reaches the cap.

Env vars:
- `WORKER_MAX_ATTEMPTS` (default `3`): max retry attempts before DLQ.
- `WORKER_BACKOFF_BASE_MS` (default `1000`): base backoff in ms.
- `WORKER_BACKOFF_MAX_MS` (default `60000`): max backoff cap in ms.
- `WORKER_MAX_IN_PROGRESS` (default `32`): max concurrently running jobs.

### Phase 3 Orders & Postgres
Postgres (service `postgres`) added via Compose with auto-init script `infra/postgres/init.sql` providing:
- Tables: `inventory`, `orders`, `order_items`, `payments`, `idempotency_keys`.
- Seed SKUs: `SKU-RED-TSHIRT`, `SKU-BLUE-TSHIRT`.

Orders service (`orders/`):
- Endpoint `POST /checkout` expects JSON:
	`{ "idempotencyKey": "KEY-123", "userId": "u1", "items": [{"sku":"SKU-RED-TSHIRT","qty":1,"priceCents":1999}] }`
- Idempotency: first request inserts key; subsequent identical requests return the existing `orderId`.
- Inventory reservation uses optimistic locking (`version` column) preventing oversell.
- Failure responses: `409 insufficient_inventory`, `409 inventory_conflict`, `409 idempotency_conflict`.

Orders env vars:
- `PG_HOST` (default `localhost`)
- `PG_PORT` (default `5432`)
- `PG_DB` (default `orders`)
- `PG_USER` (default `app`)
- `PG_PASSWORD` (default `app`)
- `ORDERS_HTTP_PORT` (default `8081`)

Run Orders locally:
```bash
mvn -q -pl orders -am package
java -cp orders/target/orders-1.0-SNAPSHOT.jar com.example.orders.OrdersMain
```
Sample checkout:
```bash
curl -X POST http://localhost:8081/checkout \
	-H 'Content-Type: application/json' \
	-d '{"idempotencyKey":"KEY-123","userId":"u1","items":[{"sku":"SKU-RED-TSHIRT","qty":1,"priceCents":1999}]}'
```

### Simple Lock Benchmark (Phase 2)
Run a minimal concurrent benchmark against a Cassandra endpoint:
```bash
mvn -q -pl client-lib -am -DskipTests package
java -cp client-lib/target/client-lib-1.0-SNAPSHOT.jar com.example.lock.ClientBench
```
Env vars (optional): `CASS_CONTACT`, `CASS_PORT`, `CASS_KEYSPACE`, `THREADS`, `DURATION_SECONDS`, `TTL_SECONDS`, `RESOURCE_PREFIX`.

## Smoke Test Validation
`scripts/smoke_test.sh` ensures:
1. Stack starts
2. Schema applied
3. Job created & enqueued
4. `job_history` populated
5. Metrics accessible

## Notes on Production Readiness
- Clock skew influences `expire_at`; keep nodes NTP-synchronized.
- Consider NetworkTopologyStrategy + replication per DC for multi-DC.
- Run `nodetool repair` after topology changes.
- Add retry & exponential backoff for transient Cassandra timeouts before production.

## License
Internal project (no license header added).
