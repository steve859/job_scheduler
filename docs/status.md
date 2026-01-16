# Project Status (Checkpoint)

This file is a short “where am I?” map for the repo.

## Where you are now
- Current phase in the roadmap: **PHASE 3** (Orders + Inventory).
- What’s solid already: distributed locks (Phase 1), scheduler worker + queue semantics (Phase 2), infra/metrics (Phase 0/2), and Orders checkout fundamentals (Phase 3).

## Roadmap (phases)
- Phase 0: Infra & foundation (Compose, schemas, CI).
- Phase 1: Cassandra LWT distributed lock client (fencing tokens, takeover, renew/release) + IT.
- Phase 2: Job scheduler queue + worker (polling, retries/backoff+jitter, DLQ, REST API, metrics).
- Phase 3: Orders + Inventory business flow (idempotent checkout, reserve inventory safely, enqueue payment job).
- Phase 4: Payment orchestration (idempotent payment, at-most-once execution, order → PAID, trigger fulfilment).
- Phase 5: Fulfilment + notification (shipping/notification workers + outbox).
- Phase 6: Observability + alerting + tracing + RCA.
- Phase 7: Hardening/security/scale/chaos/canary.

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
- Phase 3 (partial): Orders checkout with idempotency + optimistic locking + passing IT.

### Still missing (🟡 / next)
- Phase 3: either extract a separate InventoryService API, or keep it in-process but document the decision.
- Phase 3→4 bridge: formalize the **payment job payload contract** and make enqueue robust (retry/metrics).
- Phase 4: actual payment worker + payment idempotency + order status transitions.

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
  - `POST /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/run`
- Orders: `http://localhost:8081`
  - `POST /checkout`

## Recommended next step (pick one)
1) **Finish Phase 3 “as spec”**: implement a separate InventoryService and call it from Orders.
2) **Start Phase 4**: implement PaymentWorker using a `payment:{orderId}` lock + idempotency in Postgres; update order → PAID; enqueue fulfilment.
