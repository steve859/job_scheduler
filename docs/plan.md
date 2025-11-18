Distributed Job Scheduler using Cassandra Distributed Locking (Hard-Mode, 12 Weeks)
1. Overview

Dự án nhằm xây dựng một Distributed Job Scheduler sử dụng Cassandra như backend để cung cấp distributed locking, đảm bảo:

At-most-one execution: chỉ một worker được chạy một job tại một thời điểm.

Safety: sử dụng LWT (Lightweight Transactions) + fencing token để chống stale worker.

Liveness: worker chết → lock hết hạn qua TTL → worker khác tự lấy lock.

Observability: metrics, dashboard, benchmark.

Reliability: chaos testing để đánh giá tính chịu lỗi.

Thời gian thực hiện: 12 tuần, với deliverable rõ ràng và có thể reproducible bằng script.

2. Final Deliverables

GitHub Repository đầy đủ (client library, worker, demo, infra).

Docker Compose cho Cassandra 3-node.

Java Client Library (acquire, renew, release) + unit/integration tests.

Worker binary (JAR) + demo scheduler.

Benchmark suite (low/medium/high contention).

Chaos toolkit (node kill, network partition, latency injection).

Prometheus + Grafana dashboard (JSON import).

Báo cáo 25–35 trang + slide 15–20 trang + demo script.

3. Acceptance Criteria

Không bao giờ xuất hiện double-holder trong toàn bộ functional test.

TTL + Renew hoạt động đúng: worker chết → lock expire → worker khác acquire.

Có benchmark với p50, p95, p99 latency và throughput.

Chaos tests chạy thành công và có report.

Repo có README + script giúp reproduce toàn bộ hệ thống.

4. Architecture & Technologies

Cassandra 3.x/4.x (Docker)

Java + DataStax Java Driver

Prometheus, Grafana, OpenTelemetry (optional)

Python/Go cho benchmark tool

Docker Compose

tc/netem, docker kill, nodetool cho chaos tests

5. 12-Week Timeline
Week 1 — Research & Environment Setup

Nghiên cứu LWT/Paxos và fencing token.

Tạo Docker Compose cluster Cassandra 3-node.

Tạo cấu trúc repo và tài liệu hướng dẫn.

Deliverable: infra/ + README chạy Cassandra.

Week 2 — Schema Design & CQL Prototype

Thiết kế bảng: locks, lock_seq, jobs, job_history.

Prototype LWT với INSERT IF NOT EXISTS và UPDATE IF.

Deliverable: schema.cql + script demo.

Week 3 — Client Library (Acquire / Try / Renew / Release)

Implement API: acquire, tryAcquire, renew, release.

Backoff + jitter.

Unit tests.

Deliverable: client-lib v0.1.

Week 4 — Fencing Token (Monotonic Sequence)

CAS sequence generator bằng LWT.

Trả fencing token khi acquire thành công.

Tests đảm bảo monotonicity.

Deliverable: client-lib v0.2.

Week 5 — Heartbeater & Session Management

Heartbeat auto-renew (TTL/3).

Renew failure → abort job.

Graceful shutdown (release lock nếu có thể).

Deliverable: client-lib v0.3 + integration tests.

Week 6 — Worker Implementation & Demo Scheduler

Worker polling jobs hoặc thông qua REST queue.

Acquire lock → execute job → record job_history.

Dockerize worker.

Deliverable: chạy 2 worker đồng thời trong demo.

Week 7 — Observability & Dashboard

Expose Prometheus metrics: acquire latency, renew failures, contention.

Tạo Grafana dashboard JSON.

Deliverable: dashboard + hướng dẫn import.

Week 8 — Benchmark Harness

Python/Go multiprocess benchmark tool.

Chạy low/medium/high contention benchmark.

Lưu kết quả CSV/JSON.

Deliverable: benchmark suite + baseline numbers.

Week 9 — Chaos Suite

Script: kill-node.sh, partition.sh, kill-worker.sh.

Chạy chaos trong high contention.

Ghi log outcomes.

Deliverable: chaos test report.

Week 10 — Optimizations

Sharded fencing counters.

Reduce LWT frequency.

Cache read-path prototype.

Deliverable: optimization PR + so sánh hiệu năng.

Week 11 — Comparative Evaluation

Implement:

Redis Redlock

Zookeeper lock

Benchmark + chaos theo cùng kịch bản.

Deliverable: comparison table + phân tích.

Week 12 — Documentation & Presentation

Viết final report (25–35 trang).

Chuẩn bị slide (15–20 trang).

Cleanup repo và tạo reproducibility script.

Deliverable: report + slides + demo script.

6. Test Checklist
Functional Tests

Acquire success khi chỉ có một worker.

Concurrent acquire → chỉ 1 worker giữ lock.

Renew kéo dài TTL.

Release giải phóng resource.

Chaos Tests

Kill worker → TTL expire → worker khác acquire.

Kill Cassandra node → không xảy ra double-holder.

Network partition → không split-brain.

Performance Tests

Low contention: p50 < X ms (tự đặt mục tiêu).

High contention: đo throughput và p99 latency.