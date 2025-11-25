# Repository Standards (Phase 0)

## Modules
- client-lib: distributed lock client
- worker: job scheduler worker + REST API
- orders / inventory / payment / fulfilment: domain services (placeholders Phase 0)
- infra: Docker Compose, schema
- docs: design & planning

## Logging
SLF4J; avoid expensive string formatting. Use key markers: `"resource={} token={}"`.

## Metrics Naming
`lock_acquire_total`, `lock_takeover_total`, `lwt_latency_seconds`, `job_duration_seconds`, `jobs_in_progress`, `jobs_processed_total{status}`.

## Java Version
All modules compile with Java 17 via parent `pom.xml`.

## Build & CI
GitHub Actions workflow `ci.yml` builds all modules and runs unit tests.

## Code Style
Governed by `.editorconfig`.

## Testing Strategy
- Unit: pure logic (lock token generation, job state machines)
- Integration: Cassandra via Testcontainers (Phase 1), Postgres/Redis (Phase 2+)
- Smoke: `scripts/smoke_test.sh` against compose

## Security & Secrets
No plaintext secrets committed; use env vars. Future phases add Vault/KMS.

## Future Expansion
Tracing (OpenTelemetry), structured logging (JSON), advanced dashboards.
