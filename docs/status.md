# Project Status (Checkpoint)

This file is a short “where am I?” map for the repo.

See also: `docs/phase_tracker.md` (tracker theo Phase 0–7).

## Where you are now
- Current phase in the roadmap: **PHASE 5** (Observability + Alerting + AIOps MVP).
- What’s solid already: distributed locks (Phase 1), worker queue semantics + metrics (Phase 2/3), scheduler API + delayed/cron (Phase 4), and now Alertmanager + initial alert rules + AIOps webhook (Phase 5).

## Roadmap (phases)
- Phase 0: Infra & foundation (Compose, schemas, CI).
- Phase 1: Cassandra LWT distributed lock client (fencing tokens, takeover, renew/release) + IT.
- Phase 2: Job scheduler queue + worker (polling, retries/backoff+jitter, DLQ, REST API, metrics).
- Phase 3: Domain flow extensions (e.g., orders/inventory) if you choose to demo it.
- Phase 4: Scheduler API & system integration (separate scheduler service, delayed/cron, compose wiring).
- Phase 5: Observability + alerting + AIOps (Alertmanager + rules + RCA loop).
- Phase 6: Benchmarking + chaos testing + optimization.
- Phase 7: Documentation + hardening/security/scale.

## What exists in this repo (mental model)
- `client-lib/`: Cassandra lock client + integration tests.
- `worker/`: scheduler worker HTTP API + poller + retries/DLQ + Prometheus metrics.
- `orders/`: Postgres-backed Orders service (checkout + idempotency + optimistic inventory locking) + Testcontainers IT.
- `infra/`: Docker Compose (Cassandra x3, Prometheus, Grafana, Postgres, worker, orders) + schema files.
- `scripts/`: helper scripts to start/stop stack and apply Cassandra schema.

## What is done vs. remaining (high signal)
### Done (✅)
- Phase 0: Compose + schemas + CI pipeline.
- Phase 1: `LockClient` with LWT + fencing tokens + takeover, with Testcontainers IT.
- Phase 2: worker queueing/polling + retry/backoff + DLQ + metrics/dashboard.
- Phase 4 (done): Scheduler service + API submit/query/run + delayed + cron + Prometheus scrape.
- Phase 5 (partial): Alertmanager wiring + initial alerts + AIOps webhook (MVP).

### Still missing (🟡 / next)
- Phase 5: log aggregation (Loki/ELK) and/or tracing (OpenTelemetry).
- Phase 5: richer RCA: link to dashboards, attach runbooks, correlate with deploy/version.
- Phase 6: benchmark suite + chaos scripts + report artifacts.

## Quick “confidence checks” (run these)
All commands assume repo root: `d:\Code\da1\job_scheduler`.

### Orders integration tests (Postgres Testcontainers)
```bat
cd /d d:\Code\da1\job_scheduler
mvn -q -pl orders -Dtest=OrdersIT test
```

### Lock client integration tests (Cassandra Testcontainers)
```bat
cd /d d:\Code\da1\job_scheduler
mvn -q -pl client-lib verify
```

### Start local stack (requires Bash: Git Bash/WSL)
```bash
./scripts/start.sh
./scripts/init_schema.sh
./scripts/smoke_test.sh
```

## Primary endpoints (today)
- Worker: `http://localhost:8080`
  - `GET /healthz`, `GET /readyz`, `GET /metrics`
  - Executes jobs from Cassandra queue
- Scheduler: `http://localhost:8082`
  - `GET /healthz`, `GET /readyz`, `GET /metrics`
  - `POST /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/run`
- Prometheus: `http://localhost:9090`
- Alertmanager: `http://localhost:9093`
- AIOps (MVP): `http://localhost:8085`
- Orders: `http://localhost:8081`
  - `POST /checkout`

## Recommended next step (pick one)
1) **Complete Phase 5**: add Loki (logs) and wire Prometheus alerts → AIOps with richer context.
2) **Start Phase 6**: add benchmark harness + chaos scripts (kill cassandra/worker) + report.
