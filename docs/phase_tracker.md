# Phase Tracker (0–7)

Cập nhật lần cuối: 2026-01-17

## Tổng quan nhanh
- Phase hiện tại: **Phase 5 — Observability & AIOps**
- Trạng thái: **đã có Alertmanager + alert rules + AIOps webhook (MVP)**; phần log stack/tracing/RCA nâng cao còn để mở rộng.

---

## Phase 0 — Problem Definition & System Design
- [x] Infra baseline (Docker Compose) có Cassandra cluster + services
- [x] Tài liệu kế hoạch/báo cáo (docs)
- [ ] (Tuỳ chọn) Diagram kiến trúc/CAP trong docs theo format nộp

Deliverables đã thấy:
- `docs/plan.md`, `docs/report.md`
- `infra/docker-compose.yaml`

---

## Phase 1 — Data Modeling & Cassandra Schema Design
- [x] Có `infra/schema.cql`
- [x] Có bảng `locks`, `jobs_sharded`, `jobs_pending_by_bucket`, `job_history`, `jobs_dlq`
- [x] (Fix) đã xoá định nghĩa trùng `lock_seq_shard` để tránh nhầm
- [x] (Mới) thêm bảng cron: `cron_jobs`, `cron_pending_by_bucket`

Deliverables đã thấy:
- `infra/schema.cql`

---

## Phase 2 — Distributed Locking Core Implementation (CP Layer)
- [x] `LockClient` có acquire/renew/release/takeover + fencing token
- [x] Có benchmark/smoke client

Deliverables đã thấy:
- `client-lib/src/main/java/com/example/lock/LockClient.java`
- `client-lib/src/main/java/com/example/lock/ClientBench.java`

---

## Phase 3 — Worker Engine & Job Execution (AP Layer)
- [x] Worker polling theo bucket
- [x] Job lifecycle cơ bản + retry/backoff + DLQ
- [x] Health/ready/metrics

Deliverables đã thấy:
- `worker/src/main/java/com/example/worker/WorkerMain.java`

---

## Phase 4 — Scheduler API & System Integration
- [x] Tách Scheduler service riêng
- [x] REST API submit + query status + run-now
- [x] **Delayed jobs**: `runAt` / `delaySeconds`
- [x] **Cron jobs**: `POST /cronjobs`, cron engine poll + LWT-claim + enqueue occurrences
- [x] Wire vào `docker-compose` + Prometheus scrape
- [x] Update smoke test đi qua Scheduler

Deliverables đã thấy:
- `scheduler/src/main/java/com/example/scheduler/SchedulerMain.java`
- `infra/scheduler/Dockerfile`
- `infra/docker-compose.yaml`
- `scripts/smoke_test.sh`

Còn thiếu để “đóng Phase 4” theo acceptance:
- [x] OpenAPI/Swagger (hoặc docs API chuẩn hoá)
- [x] End-to-end demo flow được mô tả rõ (Scheduler → Worker) + ví dụ cron/delayed
- [x] (Tuỳ chọn) Validation/sharding logic chặt hơn (vd: validate schedule/runAt/delay)

---

## Phase 5 — Observability & AIOps
- [x] Prometheus + Grafana dashboard starter
- [x] Alertmanager + alert rules (Prometheus rule_files)
- [ ] Log stack (Loki/ELK) (chưa thấy)
- [x] AIOps service (FastAPI) + webhook receiver (MVP)

Deliverables đã thấy:
- `infra/prometheus/prometheus.yml`
- `infra/prometheus/alerts.yml`
- `infra/alertmanager/alertmanager.yml`
- `infra/aiops/`
- `infra/grafana/dashboards/job_scheduler_overview.json`

---

## Phase 6 — Benchmarking, Chaos Testing & Optimization
- [ ] Benchmark report/charts (chưa thấy)
- [ ] Chaos testing scripts (kill worker/node, delay network) (chưa thấy)

---

## Phase 7 — Documentation & Final Delivery
- [ ] TDD/Slides/Demo video/live demo checklist (chưa đủ)

---

## Next actions (gợi ý ưu tiên)
1) Đóng Phase 4: thêm tài liệu API (OpenAPI) + mô tả E2E flow cron/delayed.
2) Sang Phase 5: thêm alert rules + log aggregation (tối thiểu Loki) và wiring vào compose.
3) Phase 6: viết chaos scripts (stop worker/cassandra node) + benchmark harness xuất CSV.
