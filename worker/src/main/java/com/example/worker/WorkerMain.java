package com.example.worker;

import com.example.lock.LockClient;
import com.example.lock.LockResult;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

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

        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(localDc)
                .withKeyspace(keyspace)
                .build();

        LockClient lockClient = new LockClient(contactPoint, port, keyspace);

        PreparedStatement insertJobStmt = session.prepare("INSERT INTO jobs (job_id, schedule, payload, max_duration_seconds, created_at) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS");
        PreparedStatement selectJobStmt = session.prepare("SELECT job_id, schedule, payload, max_duration_seconds, created_at FROM jobs WHERE job_id = ?");
        PreparedStatement enqueueJobStmt = session.prepare("INSERT INTO jobs_sharded (bucket_id, scheduled_time, job_id, payload, status, attempts) VALUES (?, ?, ?, ?, 'pending', 0)");
        PreparedStatement updateJobStatusStmt = session.prepare("UPDATE jobs_sharded SET status = ? WHERE bucket_id = ? AND scheduled_time = ? AND job_id = ?");
        PreparedStatement insertHistoryStmt = session.prepare("INSERT INTO job_history (job_id, run_id, worker_id, fencing_token, start_at, end_at, status) VALUES (?, ?, ?, ?, ?, ?, ?) ");
        PreparedStatement updateHistoryEndStmt = session.prepare("UPDATE job_history SET end_at = ?, status = ? WHERE job_id = ? AND run_id = ?");
        PreparedStatement selectDueStmt = session.prepare("SELECT bucket_id, scheduled_time, job_id, payload, status, attempts FROM jobs_sharded WHERE bucket_id = ? AND status = 'pending' LIMIT 32");

        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
        server.createContext("/healthz", exchange -> respond(exchange, 200, "OK"));
        server.createContext("/metrics", new MetricsHandler());
        server.createContext("/jobs", new JobsHandler(session, insertJobStmt, selectJobStmt, enqueueJobStmt, buckets));
        server.createContext("/jobs/run", exchange -> respond(exchange, 400, "Specify /jobs/{id}/run"));
        server.createContext("/jobs/", new JobActionHandler(session, selectJobStmt, enqueueJobStmt, buckets));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("[worker] HTTP server started on port " + httpPort + " workerId=" + workerId);

        // Background poll loop
        Thread poller = new Thread(() -> {
            int bucketCursor = 0;
            while (true) {
                int bucketId = bucketCursor++ % buckets;
                try {
                    ResultSet rs = session.execute(selectDueStmt.bind(bucketId));
                    for (Row row : rs) {
                        UUID jobId = row.getUuid("job_id");
                        Instant scheduledTime = row.getInstant("scheduled_time");
                        processJob(session, lockClient, updateJobStatusStmt, insertHistoryStmt, updateHistoryEndStmt, bucketId, scheduledTime, jobId, workerId);
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

    private static void processJob(CqlSession session,
                                   LockClient lockClient,
                                   PreparedStatement updateJobStatusStmt,
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
        UUID runId = UUID.randomUUID();
        Instant start = Instant.now();
        // Heartbeat thread to renew lock until finished
        final boolean[] done = {false};
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
            session.execute(insertHistoryStmt.bind(jobId.toString(), runId, workerId, lr.getToken(), start, null, "running"));
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
            try { heartbeat.join(2000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            lockClient.release(resource, workerId, lr.getToken());
            jobsInProgress.decrementAndGet();
        }
    }

    private static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null ? d : v;
    }

    private static void respond(HttpExchange exchange, int code, String body) throws IOException {
        exchange.sendResponseHeaders(code, body.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body.getBytes());
        }
    }

    static class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            StringBuilder sb = new StringBuilder();
            sb.append("# HELP jobs_processed_total Total jobs processed\n");
            sb.append("# TYPE jobs_processed_total counter\n");
            sb.append("jobs_processed_total{status=\"success\"} ").append(jobsProcessedSuccess.get()).append('\n');
            sb.append("jobs_processed_total{status=\"failed\"} ").append(jobsProcessedFailed.get()).append('\n');
            sb.append("# HELP jobs_in_progress Current jobs in progress\n");
            sb.append("# TYPE jobs_in_progress gauge\n");
            sb.append("jobs_in_progress ").append(jobsInProgress.get()).append('\n');
            respond(exchange, 200, sb.toString());
        }
    }

    static class JobsHandler implements HttpHandler {
        private final CqlSession session;
        private final PreparedStatement insertJobStmt;
        private final PreparedStatement selectJobStmt;
        private final PreparedStatement enqueueJobStmt;
        private final int buckets;

        JobsHandler(CqlSession session, PreparedStatement insertJobStmt, PreparedStatement selectJobStmt, PreparedStatement enqueueJobStmt, int buckets) {
            this.session = session;
            this.insertJobStmt = insertJobStmt;
            this.selectJobStmt = selectJobStmt;
            this.enqueueJobStmt = enqueueJobStmt;
            this.buckets = buckets;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                UUID jobId = UUID.randomUUID();
                Instant now = Instant.now();
                // Minimal body handling (consume but ignore for now)
                exchange.getRequestBody().close();
                BoundStatement bs = insertJobStmt.bind(jobId.toString(), now.toString(), "{}", 3600, now);
                session.execute(bs);
                int bucket = Math.abs(jobId.hashCode()) % buckets;
                session.execute(enqueueJobStmt.bind(bucket, Instant.now(), jobId, "{}"));
                respond(exchange, 201, "{\"job_id\":\"" + jobId + "\"}\n");
            } else if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                URI uri = exchange.getRequestURI();
                String path = uri.getPath();
                if (path.startsWith("/jobs/")) {
                    String id = path.substring("/jobs/".length());
                    Row r = session.execute(selectJobStmt.bind(id)).one();
                    if (r == null) {
                        respond(exchange, 404, "not found");
                        return;
                    }
                    String body = "{" + "\"job_id\":\"" + id + "\"}";
                    respond(exchange, 200, body);
                } else {
                    respond(exchange, 400, "Specify /jobs/{id}");
                }
            } else {
                respond(exchange, 405, "Method Not Allowed");
            }
        }
    }

    static class JobActionHandler implements HttpHandler {
        private final CqlSession session;
        private final PreparedStatement selectJobStmt;
        private final PreparedStatement enqueueJobStmt;
        private final int buckets;

        JobActionHandler(CqlSession session, PreparedStatement selectJobStmt, PreparedStatement enqueueJobStmt, int buckets) {
            this.session = session;
            this.selectJobStmt = selectJobStmt;
            this.enqueueJobStmt = enqueueJobStmt;
            this.buckets = buckets;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            if (!path.startsWith("/jobs/")) {
                respond(exchange, 404, "not found");
                return;
            }
            String remainder = path.substring("/jobs/".length());
            if (remainder.endsWith("/run")) {
                String id = remainder.substring(0, remainder.length() - "/run".length());
                UUID jobId;
                try {
                    jobId = UUID.fromString(id);
                } catch (IllegalArgumentException ex) {
                    respond(exchange, 400, "bad job id");
                    return;
                }
                Row r = session.execute(selectJobStmt.bind(id)).one();
                if (r == null) {
                    respond(exchange, 404, "not found");
                    return;
                }
                int bucket = Math.abs(jobId.hashCode()) % buckets;
                session.execute(enqueueJobStmt.bind(bucket, Instant.now(), jobId, "{}"));
                respond(exchange, 200, "enqueued\n");
            } else {
                respond(exchange, 400, "unsupported action");
            }
        }
    }
}
