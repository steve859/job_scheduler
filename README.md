# Job Scheduler (Cassandra LWT + Distributed Locks)

## Overview
This project provides a distributed job scheduler backed by Apache Cassandra using lightweight transactions (LWT) for safe, fencing-token based locking. A worker service exposes REST endpoints to create and run jobs and a background loop processes pending jobs while emitting Prometheus metrics.

## Components
- `client-lib/`: Java lock client (acquire / renew / release) using `locks`, `lock_seq`.
- `worker/`: Worker HTTP service + polling loop.
- `infra/`: Docker Compose stack (3-node Cassandra, Prometheus, Grafana, worker) and schema.
- `scripts/`: Helper scripts for starting stack, applying schema, smoke test.

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
- `GET /healthz` -> health check

## Locking Semantics
Fencing tokens are generated via CAS on `lock_seq`. Lock acquisition stores `fencing_token` and `expire_at`. Expired locks are taken over by incrementing `epoch` (CAS on `epoch`). Clients use their token to ensure ordered execution.

## Schema Application
`scripts/init_schema.sh` waits for Cassandra readiness and applies `infra/schema.cql` idempotently.

## Prometheus & Grafana
Prometheus scrapes:
- Worker metrics (`worker1:8080/metrics`)
- Cassandra exporters (placeholder ports)

Grafana container starts with persistent volume `grafana_data`.

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