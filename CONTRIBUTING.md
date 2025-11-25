# Contributing

## Branching
Use feature branches: `feat/<short-name>`; keep PRs small and focused.

## Commit Messages
Follow Conventional Commits:
- feat: new feature
- fix: bug fix
- chore: tooling/infra
- docs: documentation only
- test: adding or refactoring tests

## Code Style
Java 17, spaces (4), LF endings, no trailing whitespace. See `.editorconfig`.

## Testing
Add unit tests for new logic. Integration tests (Cassandra/Postgres) should use Testcontainers.

## Logging
Use SLF4J with structured message segments: avoid concatenation inside hot paths.
Levels:
- INFO: lifecycle events
- WARN: transient recoverable issues
- ERROR: failures requiring attention

## Metrics
Prefer Prometheus naming: `<component>_<metric>_<unit>`.
Types:
- Counter: monotonic (`*_total`)
- Gauge: current value
- Histogram: latency/duration (`_seconds` suffix)

## Pull Requests
Include rationale, implementation summary, test evidence (output or screenshot). Reference related issues.

## Security
Do not commit secrets. Use environment variables or secret management (future Phase 6).
