package com.example.worker;

import com.example.lock.LockClient;
import com.example.lock.LockMetrics;
import com.example.worker.metrics.PromLockMetrics;
import com.example.lock.LockResult;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

public class WorkerMain {
    private static final int DEFAULT_BUCKETS = 16;
    private static final AtomicLong jobsProcessedSuccess = new AtomicLong();
    private static final AtomicLong jobsProcessedFailed = new AtomicLong();
    private static final AtomicLong jobsInProgress = new AtomicLong();

    public static void main(String[] args) throws Exception {
        String contactPoint = env("CASSANDRA_CONTACT_POINT", "127.0.0.1");
        int port = Integer.parseInt(env("CASSANDRA_PORT", "9042"));
        String keyspace = env("CASSANDRA_KEYSPACE", "scheduler");
        String workerId = env("WORKER_ID", "worker-" + UUID.randomUUID());
        int httpPort = Integer.parseInt(env("WORKER_HTTP_PORT", "8080"));
        int pollIntervalMs = Integer.parseInt(env("WORKER_POLL_INTERVAL_MS", "2000"));
        int buckets = Integer.parseInt(env("WORKER_BUCKETS", String.valueOf(DEFAULT_BUCKETS)));
        String localDc = env("CASS_LOCAL_DC", "DC1");

        // Build session WITHOUT keyspace first to allow auto-create if missing
        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(localDc)
                .build();

        ensureKeyspace(session, keyspace);
        // Switch to keyspace explicitly
        session.execute("USE " + keyspace);

        // Prometheus metrics registry and LockMetrics implementation
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        LockMetrics metrics = new PromLockMetrics(registry);
        LockClient lockClient = new LockClient(contactPoint, port, keyspace, localDc, metrics); // now keyspace exists

        PreparedStatement insertJobStmt = session.prepare(
                "INSERT INTO jobs (job_id, schedule, payload, max_duration_seconds, created_at) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS");
        PreparedStatement selectJobStmt = session.prepare(
                "SELECT job_id, schedule, payload, max_duration_seconds, created_at FROM jobs WHERE job_id = ?");
        PreparedStatement enqueueJobStmt = session.prepare(
                "INSERT INTO jobs_sharded (bucket_id, scheduled_time, job_id, payload, status, attempts) VALUES (?, ?, ?, ?, 'pending', 0)");
        PreparedStatement enqueuePendingStmt = session.prepare(
                "INSERT INTO jobs_pending_by_bucket (bucket_id, scheduled_time, job_id, payload) VALUES (?, ?, ?, ?)");
        PreparedStatement updateJobStatusStmt = session.prepare(
                "UPDATE jobs_sharded SET status = ? WHERE bucket_id = ? AND scheduled_time = ? AND job_id = ?");
        PreparedStatement deletePendingStmt = session.prepare(
                "DELETE FROM jobs_pending_by_bucket WHERE bucket_id = ? AND scheduled_time = ? AND job_id = ?");
        PreparedStatement insertHistoryStmt = session.prepare(
                "INSERT INTO job_history (job_id, run_id, worker_id, fencing_token, start_at, end_at, status) VALUES (?, ?, ?, ?, ?, ?, ?) ");
        PreparedStatement updateHistoryEndStmt = session
                .prepare("UPDATE job_history SET end_at = ?, status = ? WHERE job_id = ? AND run_id = ?");
        PreparedStatement selectDuePendingStmt = session.prepare(
                "SELECT bucket_id, scheduled_time, job_id, payload FROM jobs_pending_by_bucket WHERE bucket_id = ? AND scheduled_time <= ? LIMIT 32");
        PreparedStatement selectHistoryByJobStmt = session.prepare(
                "SELECT run_id, start_at, end_at, status FROM job_history WHERE job_id = ?");

        ObjectMapper mapper = new ObjectMapper();
        // Enable JavaTime (Instant, etc.) serialization as ISO-8601 strings
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
        server.createContext("/healthz", exchange -> respond(exchange, 200, "OK"));
        server.createContext("/metrics", new MetricsHandlerProm(registry));
        server.createContext("/jobs", new JobsHandler(session, insertJobStmt, selectJobStmt, enqueueJobStmt,
                enqueuePendingStmt, selectHistoryByJobStmt, buckets, mapper));
        server.createContext("/jobs/run", exchange -> respond(exchange, 400, "Specify /jobs/{id}/run"));
        server.createContext("/jobs/",
                new JobActionHandler(session, selectJobStmt, enqueueJobStmt, enqueuePendingStmt, buckets, mapper));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("[worker] HTTP server started on port " + httpPort + " workerId=" + workerId);

        // Background poll loop
        Thread poller = new Thread(() -> {
            int bucketCursor = 0;
            while (true) {
                int bucketId = bucketCursor++ % buckets;
                try {
                    ResultSet rs = session.execute(selectDuePendingStmt.bind(bucketId, Instant.now()));
                    for (Row row : rs) {
                        UUID jobId = row.getUuid("job_id");
                        Instant scheduledTime = row.getInstant("scheduled_time");
                        processJob(session, lockClient, updateJobStatusStmt, deletePendingStmt, insertHistoryStmt,
                                updateHistoryEndStmt,
                                bucketId, scheduledTime, jobId, workerId);
                        break; // process one then move to next bucket
                    }
                    Thread.sleep(pollIntervalMs);
                } catch (Exception e) {
                    System.err.println("[worker] Poll error: " + e.getMessage());
                    try {
                        Thread.sleep(pollIntervalMs);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
        poller.setDaemon(true);
        poller.start();
    }

    private static void ensureKeyspace(CqlSession session, String keyspace) {
        try {
            ResultSet rs = session.execute(
                    "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='" + keyspace + "'");
            if (rs.one() == null) {
                System.out.println("[worker] Keyspace '" + keyspace + "' not found. Creating...");
                // SimpleStrategy for local dev; adjust for production
                session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                        + " WITH replication = {'class':'SimpleStrategy','replication_factor':3}");
                System.out.println("[worker] Keyspace created.");
            } else {
                System.out.println("[worker] Keyspace '" + keyspace + "' exists.");
            }
        } catch (Exception e) {
            System.err.println("[worker] Failed checking/creating keyspace: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processJob(CqlSession session,
            LockClient lockClient,
            PreparedStatement updateJobStatusStmt,
            PreparedStatement deletePendingStmt,
            PreparedStatement insertHistoryStmt,
            PreparedStatement updateHistoryEndStmt,
            int bucketId,
            Instant scheduledTime,
            UUID jobId,
            String workerId) {
        String resource = "job:" + jobId;
        Duration ttl = Duration.ofSeconds(30);
        LockResult lr = lockClient.acquire(resource, workerId, ttl, Duration.ofSeconds(5));
        if (!lr.isAcquired()) {
            return; // skip if cannot acquire
        }
        jobsInProgress.incrementAndGet();
        // Remove from pending queue to avoid duplicate pickup
        try {
            session.execute(deletePendingStmt.bind(bucketId, scheduledTime, jobId));
        } catch (Exception ignored) {
        }
        UUID runId = UUID.randomUUID();
        Instant start = Instant.now();
        // Heartbeat thread to renew lock until finished
        final boolean[] done = { false };
        Thread heartbeat = new Thread(() -> {
            while (!done[0]) {
                try {
                    Thread.sleep(ttl.toMillis() / 3);
                    boolean ok = lockClient.renew(resource, workerId, lr.getToken(), ttl);
                    if (!ok) {
                        System.err.println("[worker] Renew failed for job " + jobId + ", token=" + lr.getToken());
                        // If renew fails we allow job to proceed but warn; could abort in future.
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ex) {
                    System.err.println("[worker] Heartbeat exception: " + ex.getMessage());
                }
            }
        });
        heartbeat.setDaemon(true);
        try {
            // Initial history entry (end_at null)
            session.execute(
                    insertHistoryStmt.bind(jobId.toString(), runId, workerId, lr.getToken(), start, null, "running"));
            session.execute(updateJobStatusStmt.bind("running", bucketId, scheduledTime, jobId));
            heartbeat.start();
            // Simulated work (placeholder for actual payload handling)
            Thread.sleep(500);
            Instant end = Instant.now();
            // Update existing history row instead of second insert overwrite
            session.execute(updateHistoryEndStmt.bind(end, "completed", jobId.toString(), runId));
            session.execute(updateJobStatusStmt.bind("completed", bucketId, scheduledTime, jobId));
            jobsProcessedSuccess.incrementAndGet();
        } catch (Exception ex) {
            Instant end = Instant.now();
            session.execute(updateHistoryEndStmt.bind(end, "failed", jobId.toString(), runId));
            session.execute(updateJobStatusStmt.bind("failed", bucketId, scheduledTime, jobId));
            jobsProcessedFailed.incrementAndGet();
            System.err.println("[worker] Job " + jobId + " failed: " + ex.getMessage());
        } finally {
            done[0] = true;
            try {
                heartbeat.join(2000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            lockClient.release(resource, workerId, lr.getToken());
            jobsInProgress.decrementAndGet();
        }
    }

    private static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null ? d : v;
    }

    private static void respond(HttpExchange exchange, int code, String body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(code, body.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body.getBytes());
        }
    }

    private static void respondJson(HttpExchange exchange, int code, Object value, ObjectMapper mapper)
            throws IOException {
        byte[] data = mapper.writeValueAsBytes(value);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(code, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }

    static class MetricsHandlerProm implements HttpHandler {
        private final CollectorRegistry registry;

        MetricsHandlerProm(CollectorRegistry registry) {
            this.registry = registry;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", TextFormat.CONTENT_TYPE_004);
            // augment custom gauges/counters via a simple text block
            StringBuilder custom = new StringBuilder();
            custom.append("# HELP jobs_processed_total Total jobs processed\n");
            custom.append("# TYPE jobs_processed_total counter\n");
            custom.append("jobs_processed_total{status=\"success\"} ").append(jobsProcessedSuccess.get()).append('\n');
            custom.append("jobs_processed_total{status=\"failed\"} ").append(jobsProcessedFailed.get()).append('\n');
            custom.append("# HELP jobs_in_progress Current jobs in progress\n");
            custom.append("# TYPE jobs_in_progress gauge\n");
            custom.append("jobs_in_progress ").append(jobsInProgress.get()).append('\n');

            // write Prometheus registry metrics then our custom block
            StringBuilder body = new StringBuilder();
            java.io.StringWriter writer = new java.io.StringWriter();
            TextFormat.write004(writer, registry.metricFamilySamples());
            body.append(writer.toString());
            body.append(custom.toString());
            byte[] data = body.toString().getBytes();
            exchange.sendResponseHeaders(200, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        }
    }

    static class JobsHandler implements HttpHandler {
        private final CqlSession session;
        private final PreparedStatement insertJobStmt;
        private final PreparedStatement selectJobStmt;
        private final PreparedStatement enqueueJobStmt;
        private final PreparedStatement enqueuePendingStmt;
        private final PreparedStatement selectHistoryByJobStmt;
        private final int buckets;
        private final ObjectMapper mapper;

        JobsHandler(CqlSession session, PreparedStatement insertJobStmt, PreparedStatement selectJobStmt,
                PreparedStatement enqueueJobStmt, PreparedStatement enqueuePendingStmt,
                PreparedStatement selectHistoryByJobStmt, int buckets,
                ObjectMapper mapper) {
            this.session = session;
            this.insertJobStmt = insertJobStmt;
            this.selectJobStmt = selectJobStmt;
            this.enqueueJobStmt = enqueueJobStmt;
            this.enqueuePendingStmt = enqueuePendingStmt;
            this.selectHistoryByJobStmt = selectHistoryByJobStmt;
            this.buckets = buckets;
            this.mapper = mapper;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                try (InputStream is = exchange.getRequestBody()) {
                    com.example.worker.api.JobCreateRequest req = mapper.readValue(is,
                            com.example.worker.api.JobCreateRequest.class);
                    UUID jobId = UUID.randomUUID();
                    Instant now = Instant.now();
                    String schedule = Optional.ofNullable(req.getSchedule()).orElse("immediate");
                    int maxDur = Optional.ofNullable(req.getMaxDurationSeconds()).orElse(3600);
                    Object payloadObj = Optional.ofNullable(req.getPayload()).orElse(new java.util.HashMap<>());
                    String payloadJson = mapper.writeValueAsString(payloadObj);
                    BoundStatement bs = insertJobStmt.bind(jobId.toString(), schedule, payloadJson, maxDur, now);
                    session.execute(bs);
                    int bucket = Math.abs(jobId.hashCode()) % buckets;
                    Instant when = Instant.now();
                    session.execute(enqueueJobStmt.bind(bucket, when, jobId, payloadJson));
                    session.execute(enqueuePendingStmt.bind(bucket, when, jobId, payloadJson));
                    respondJson(exchange, 201, new com.example.worker.api.JobResponse(
                            jobId.toString(), schedule, payloadObj, maxDur, now, "pending", null, null, null), mapper);
                } catch (Exception ex) {
                    respondJson(exchange, 400, new com.example.worker.api.ErrorResponse("bad_request", ex.getMessage()),
                            mapper);
                }
            } else if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                URI uri = exchange.getRequestURI();
                String path = uri.getPath();
                if (path.startsWith("/jobs/")) {
                    String id = path.substring("/jobs/".length());
                    Row r = session.execute(selectJobStmt.bind(id)).one();
                    if (r == null) {
                        respondJson(exchange, 404,
                                new com.example.worker.api.ErrorResponse("not_found", "job " + id + " not found"),
                                mapper);
                        return;
                    }
                    String schedule = r.getString("schedule");
                    String payloadStr = r.getString("payload");
                    Object payloadObj = null;
                    try {
                        payloadObj = payloadStr == null ? null : mapper.readTree(payloadStr);
                    } catch (Exception ignored) {
                    }
                    Integer maxDur = r.isNull("max_duration_seconds") ? null : r.getInt("max_duration_seconds");
                    Instant createdAt = r.getInstant("created_at");
                    // Status aggregation deferred (needs join/calc). For now unknown.
                    // Aggregate latest run status
                    ResultSet hrs = session.execute(selectHistoryByJobStmt.bind(id));
                    Instant latestStart = null;
                    Instant latestEnd = null;
                    String latestRunStatus = null;
                    for (Row hr : hrs) {
                        Instant s = hr.getInstant("start_at");
                        if (latestStart == null || (s != null && s.isAfter(latestStart))) {
                            latestStart = s;
                            latestEnd = hr.getInstant("end_at");
                            latestRunStatus = hr.getString("status");
                        }
                    }
                    String status = latestRunStatus; // if null => no runs yet
                    respondJson(exchange, 200, new com.example.worker.api.JobResponse(id, schedule, payloadObj, maxDur,
                            createdAt, status, latestStart, latestEnd, latestRunStatus), mapper);
                } else {
                    respondJson(exchange, 400,
                            new com.example.worker.api.ErrorResponse("bad_request", "Specify /jobs/{id}"), mapper);
                }
            } else {
                respondJson(exchange, 405,
                        new com.example.worker.api.ErrorResponse("method_not_allowed", exchange.getRequestMethod()),
                        mapper);
            }
        }
    }

    static class JobActionHandler implements HttpHandler {
        private final CqlSession session;
        private final PreparedStatement selectJobStmt;
        private final PreparedStatement enqueueJobStmt;
        private final PreparedStatement enqueuePendingStmt;
        private final int buckets;
        private final ObjectMapper mapper;

        JobActionHandler(CqlSession session, PreparedStatement selectJobStmt, PreparedStatement enqueueJobStmt,
                PreparedStatement enqueuePendingStmt, int buckets, ObjectMapper mapper) {
            this.session = session;
            this.selectJobStmt = selectJobStmt;
            this.enqueueJobStmt = enqueueJobStmt;
            this.enqueuePendingStmt = enqueuePendingStmt;
            this.buckets = buckets;
            this.mapper = mapper;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            if (!path.startsWith("/jobs/")) {
                respondJson(exchange, 404, new com.example.worker.api.ErrorResponse("not_found", "invalid path"),
                        mapper);
                return;
            }
            String remainder = path.substring("/jobs/".length());
            if (remainder.endsWith("/run")) {
                String id = remainder.substring(0, remainder.length() - "/run".length());
                UUID jobId;
                try {
                    jobId = UUID.fromString(id);
                } catch (IllegalArgumentException ex) {
                    respondJson(exchange, 400, new com.example.worker.api.ErrorResponse("bad_request", "bad job id"),
                            mapper);
                    return;
                }
                Row r = session.execute(selectJobStmt.bind(id)).one();
                if (r == null) {
                    respondJson(exchange, 404,
                            new com.example.worker.api.ErrorResponse("not_found", "job " + id + " not found"), mapper);
                    return;
                }
                int bucket = Math.abs(jobId.hashCode()) % buckets;
                Instant when = Instant.now();
                session.execute(enqueueJobStmt.bind(bucket, when, jobId, "{}"));
                session.execute(enqueuePendingStmt.bind(bucket, when, jobId, "{}"));
                respondJson(exchange, 200, new com.example.worker.api.EnqueueResponse(id, "enqueued"), mapper);
            } else {
                respondJson(exchange, 400,
                        new com.example.worker.api.ErrorResponse("bad_request", "unsupported action"), mapper);
            }
        }
    }
}
