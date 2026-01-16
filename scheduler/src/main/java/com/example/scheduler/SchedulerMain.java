package com.example.scheduler;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.example.scheduler.api.EnqueueResponse;
import com.example.scheduler.api.ErrorResponse;
import com.example.scheduler.api.JobCreateRequest;
import com.example.scheduler.api.JobResponse;
import com.example.scheduler.api.cron.CronJobCreateRequest;
import com.example.scheduler.api.cron.CronJobResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.exporter.common.TextFormat;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

public class SchedulerMain {
    private static final int DEFAULT_BUCKETS = 16;
    private static volatile boolean ready = false;
    private static volatile boolean shuttingDown = false;

    private static Counter httpRequestsTotal;

    public static void main(String[] args) throws Exception {
        String contactPoint = env("CASSANDRA_CONTACT_POINT", "127.0.0.1");
        int port = Integer.parseInt(env("CASSANDRA_PORT", "9042"));
        String keyspace = env("CASSANDRA_KEYSPACE", "scheduler");
        String localDc = env("CASS_LOCAL_DC", "DC1");
        int httpPort = Integer.parseInt(env("SCHEDULER_HTTP_PORT", "8082"));
        int buckets = Integer.parseInt(env("SCHEDULER_BUCKETS", String.valueOf(DEFAULT_BUCKETS)));
        int cronPollIntervalMs = Integer.parseInt(env("SCHEDULER_CRON_POLL_INTERVAL_MS", "1000"));
        int cronBuckets = Integer.parseInt(env("SCHEDULER_CRON_BUCKETS", String.valueOf(DEFAULT_BUCKETS)));

        CqlSession bootstrap = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(localDc)
                .build();
        ensureKeyspace(bootstrap, keyspace);
        bootstrap.close();

        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(localDc)
                .withKeyspace(keyspace)
                .build();

        PreparedStatement insertJobStmt = session.prepare(
                "INSERT INTO jobs (job_id, schedule, payload, max_duration_seconds, created_at) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS");
        PreparedStatement selectJobStmt = session.prepare(
                "SELECT job_id, schedule, payload, max_duration_seconds, created_at FROM jobs WHERE job_id = ?");
        PreparedStatement enqueueJobStmt = session.prepare(
                "INSERT INTO jobs_sharded (bucket_id, scheduled_time, job_id, payload, status, attempts) VALUES (?, ?, ?, ?, 'pending', 0)");
        PreparedStatement enqueuePendingStmt = session.prepare(
                "INSERT INTO jobs_pending_by_bucket (bucket_id, scheduled_time, job_id, payload) VALUES (?, ?, ?, ?)");
        PreparedStatement selectHistoryByJobStmt = session.prepare(
                "SELECT run_id, start_at, end_at, status FROM job_history WHERE job_id = ?");

        // Cron tables
        PreparedStatement insertCronJobStmt = session.prepare(
            "INSERT INTO cron_jobs (cron_id, cron_expr, timezone, payload, max_duration_seconds, enabled, bucket_id, next_run_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement selectCronJobStmt = session.prepare(
            "SELECT cron_expr, timezone, payload, max_duration_seconds, enabled, bucket_id, next_run_at, created_at FROM cron_jobs WHERE cron_id = ?");
        PreparedStatement deleteCronJobStmt = session.prepare(
            "DELETE FROM cron_jobs WHERE cron_id = ?");
        PreparedStatement insertCronPendingStmt = session.prepare(
            "INSERT INTO cron_pending_by_bucket (bucket_id, next_run_at, cron_id) VALUES (?, ?, ?)");
        PreparedStatement selectDueCronPendingStmt = session.prepare(
            "SELECT bucket_id, next_run_at, cron_id FROM cron_pending_by_bucket WHERE bucket_id = ? AND next_run_at <= ? LIMIT 32");
        PreparedStatement claimCronPendingStmt = session.prepare(
            "DELETE FROM cron_pending_by_bucket WHERE bucket_id = ? AND next_run_at = ? AND cron_id = ? IF EXISTS");
        PreparedStatement updateCronNextRunStmt = session.prepare(
            "UPDATE cron_jobs SET next_run_at = ?, updated_at = ? WHERE cron_id = ?");

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        httpRequestsTotal = Counter.build()
                .name("scheduler_http_requests_total")
                .help("Scheduler HTTP requests")
                .labelNames("path", "method", "status")
                .register(registry);

        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
        server.createContext("/healthz", exchange -> {
            int code = 200;
            try {
                session.execute("SELECT now() FROM system.local");
            } catch (Exception e) {
                code = 503;
            }
            respond(exchange, code, code == 200 ? "OK" : "UNHEALTHY");
        });
        server.createContext("/readyz", exchange -> respond(exchange, ready ? 200 : 503, ready ? "READY" : "NOT_READY"));
        server.createContext("/metrics", new MetricsHandlerProm(registry));

        server.createContext("/jobs", new JobsHandler(session, insertJobStmt, selectJobStmt, enqueueJobStmt,
                enqueuePendingStmt, selectHistoryByJobStmt, buckets, mapper));
        server.createContext("/jobs/", new JobActionHandler(session, selectJobStmt, enqueueJobStmt, enqueuePendingStmt,
                selectHistoryByJobStmt, buckets, mapper));

        server.createContext("/cronjobs", new CronJobsHandler(session, insertCronJobStmt, selectCronJobStmt,
            insertCronPendingStmt, buckets, mapper));
        server.createContext("/cronjobs/", new CronJobActionHandler(session, selectCronJobStmt, deleteCronJobStmt,
            mapper));

        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();
        ready = true;
        System.out.println("[scheduler] HTTP server started on port " + httpPort);

        // Cron engine background loop
        Thread cronEngine = new Thread(() -> runCronEngine(session, insertJobStmt, enqueueJobStmt, enqueuePendingStmt,
            selectCronJobStmt, insertCronPendingStmt, selectDueCronPendingStmt, claimCronPendingStmt,
            updateCronNextRunStmt, cronBuckets, buckets, cronPollIntervalMs));
        cronEngine.setDaemon(true);
        cronEngine.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown = true;
            try {
                server.stop(1);
            } catch (Exception ignored) {
            }
            try {
                session.close();
            } catch (Exception ignored) {
            }
        }));
    }

    private static void ensureKeyspace(CqlSession session, String keyspace) {
        try {
            ResultSet rs = session.execute(
                    "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='" + keyspace + "'");
            if (rs.one() == null) {
                System.out.println("[scheduler] Keyspace '" + keyspace + "' not found. Creating...");
                session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                        + " WITH replication = {'class':'SimpleStrategy','replication_factor':3}");
                System.out.println("[scheduler] Keyspace created.");
            }
        } catch (Exception e) {
            System.err.println("[scheduler] Failed checking/creating keyspace: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null ? d : v;
    }

    private static void respond(HttpExchange exchange, int code, String body) throws IOException {
        httpRequestsTotal.labels(normalizePath(exchange.getRequestURI().getPath()), exchange.getRequestMethod(), String.valueOf(code)).inc();
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(code, body.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body.getBytes());
        }
    }

    private static void respondJson(HttpExchange exchange, int code, Object value, ObjectMapper mapper) throws IOException {
        byte[] data = mapper.writeValueAsBytes(value);
        httpRequestsTotal.labels(normalizePath(exchange.getRequestURI().getPath()), exchange.getRequestMethod(), String.valueOf(code)).inc();
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(code, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }

    private static String normalizePath(String rawPath) {
        if (rawPath == null) {
            return "";
        }
        if (rawPath.startsWith("/jobs/") && rawPath.endsWith("/run")) {
            return "/jobs/:id/run";
        }
        if (rawPath.startsWith("/jobs/")) {
            return "/jobs/:id";
        }
        return rawPath;
    }

    private static Instant resolveScheduledTime(JobCreateRequest req) {
        if (req == null) {
            return Instant.now();
        }
        if (req.getRunAt() != null && !req.getRunAt().trim().isEmpty()) {
            try {
                return Instant.parse(req.getRunAt().trim());
            } catch (Exception e) {
                throw new IllegalArgumentException("runAt must be ISO-8601 Instant, got: " + req.getRunAt());
            }
        }
        if (req.getDelaySeconds() != null) {
            if (req.getDelaySeconds() < 0) {
                throw new IllegalArgumentException("delaySeconds must be >= 0");
            }
            return Instant.now().plusSeconds(req.getDelaySeconds());
        }
        // backward-compatible
        return parseScheduleToInstant(req.getSchedule());
    }

    private static Instant parseScheduleToInstant(String schedule) {
        String s = schedule == null ? "" : schedule.trim();
        if (s.isEmpty() || "immediate".equalsIgnoreCase(s) || "now".equalsIgnoreCase(s)) {
            return Instant.now();
        }
        try {
            return Instant.parse(s);
        } catch (Exception e) {
            throw new IllegalArgumentException("schedule must be 'now'/'immediate' or ISO-8601 Instant, got: " + schedule);
        }
    }

    private static CronParser cronParser() {
        CronDefinition def = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);
        return new CronParser(def);
    }

    private static Instant nextCronRun(String cronExpr, String timezone, Instant baseInstant) {
        String expr = (cronExpr == null ? "" : cronExpr.trim());
        if (expr.isEmpty()) {
            throw new IllegalArgumentException("cron expression is required");
        }
        ZoneId zone = ZoneId.of((timezone == null || timezone.isBlank()) ? "UTC" : timezone.trim());
        Cron cron = cronParser().parse(expr);
        cron.validate();
        ExecutionTime et = ExecutionTime.forCron(cron);
        ZonedDateTime base = ZonedDateTime.ofInstant(baseInstant, zone);
        return et.nextExecution(base)
                .map(ZonedDateTime::toInstant)
                .orElseThrow(() -> new IllegalArgumentException("cron has no next execution time"));
    }

    private static void runCronEngine(
            CqlSession session,
            PreparedStatement insertJobStmt,
            PreparedStatement enqueueJobStmt,
            PreparedStatement enqueuePendingStmt,
            PreparedStatement selectCronJobStmt,
            PreparedStatement insertCronPendingStmt,
            PreparedStatement selectDueCronPendingStmt,
            PreparedStatement claimCronPendingStmt,
            PreparedStatement updateCronNextRunStmt,
            int cronBuckets,
            int jobBuckets,
            int pollIntervalMs) {
        int cursor = 0;
        while (!shuttingDown) {
            int bucketId = cursor++ % Math.max(1, cronBuckets);
            try {
                ResultSet rs = session.execute(selectDueCronPendingStmt.bind(bucketId, Instant.now()));
                for (Row row : rs) {
                    Instant dueAt = row.getInstant("next_run_at");
                    UUID cronId = row.getUuid("cron_id");
                    if (cronId == null || dueAt == null) {
                        continue;
                    }

                    // LWT-claim the pending row so multiple schedulers won't double-enqueue
                    Row appliedRow = session.execute(claimCronPendingStmt.bind(bucketId, dueAt, cronId)).one();
                    boolean applied = appliedRow != null && appliedRow.getBoolean("[applied]");
                    if (!applied) {
                        continue;
                    }

                    // Load cron definition
                    Row cronRow = session.execute(selectCronJobStmt.bind(cronId)).one();
                    if (cronRow == null) {
                        continue;
                    }
                    boolean enabled = !cronRow.isNull("enabled") && cronRow.getBoolean("enabled");
                    if (!enabled) {
                        continue;
                    }
                    String cronExpr = cronRow.getString("cron_expr");
                    String timezone = cronRow.getString("timezone");
                    String payload = cronRow.getString("payload");
                    int maxDur = cronRow.isNull("max_duration_seconds") ? 3600 : cronRow.getInt("max_duration_seconds");

                    // Enqueue a job occurrence scheduled at dueAt
                    UUID jobId = UUID.randomUUID();
                    Instant createdAt = Instant.now();
                    String schedule = "cron:" + cronId;
                    String payloadJson = (payload == null ? "{}" : payload);

                    session.execute(insertJobStmt.bind(jobId.toString(), schedule, payloadJson, maxDur, createdAt));
                    int jobBucket = Math.abs(jobId.hashCode()) % Math.max(1, jobBuckets);
                    session.execute(enqueueJobStmt.bind(jobBucket, dueAt, jobId, payloadJson));
                    session.execute(enqueuePendingStmt.bind(jobBucket, dueAt, jobId, payloadJson));

                    // Compute and schedule next run (misfires are skipped; schedule next after NOW)
                    Instant next = nextCronRun(cronExpr, timezone, Instant.now());
                    session.execute(updateCronNextRunStmt.bind(next, Instant.now(), cronId));
                    session.execute(insertCronPendingStmt.bind(bucketId, next, cronId));
                }
            } catch (Exception e) {
                System.err.println("[scheduler] cron engine error: " + e.getMessage());
            }

            try {
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
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
            java.io.StringWriter writer = new java.io.StringWriter();
            TextFormat.write004(writer, registry.metricFamilySamples());
            byte[] data = writer.toString().getBytes();
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

        JobsHandler(CqlSession session,
                    PreparedStatement insertJobStmt,
                    PreparedStatement selectJobStmt,
                    PreparedStatement enqueueJobStmt,
                    PreparedStatement enqueuePendingStmt,
                    PreparedStatement selectHistoryByJobStmt,
                    int buckets,
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
                    JobCreateRequest req = mapper.readValue(is, JobCreateRequest.class);
                    UUID jobId = UUID.randomUUID();
                    Instant createdAt = Instant.now();

                    String schedule = Optional.ofNullable(req.getSchedule()).orElse("immediate");
                    int maxDur = Optional.ofNullable(req.getMaxDurationSeconds()).orElse(3600);
                    Object payloadObj = Optional.ofNullable(req.getPayload()).orElse(new java.util.HashMap<>());
                    String payloadJson = mapper.writeValueAsString(payloadObj);

                    Instant scheduledTime = resolveScheduledTime(req);

                    BoundStatement bs = insertJobStmt.bind(jobId.toString(), schedule, payloadJson, maxDur, createdAt);
                    session.execute(bs);

                    int bucket = Math.abs(jobId.hashCode()) % buckets;
                    session.execute(enqueueJobStmt.bind(bucket, scheduledTime, jobId, payloadJson));
                    session.execute(enqueuePendingStmt.bind(bucket, scheduledTime, jobId, payloadJson));

                    respondJson(exchange, 201, new JobResponse(
                            jobId.toString(), schedule, payloadObj, maxDur, createdAt,
                            "pending", null, null, null), mapper);
                } catch (IllegalArgumentException ex) {
                    respondJson(exchange, 400, new ErrorResponse("bad_request", ex.getMessage()), mapper);
                } catch (Exception ex) {
                    respondJson(exchange, 400, new ErrorResponse("bad_request", ex.getMessage()), mapper);
                }
                return;
            }

            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                URI uri = exchange.getRequestURI();
                String path = uri.getPath();
                if (!path.startsWith("/jobs/")) {
                    respondJson(exchange, 400, new ErrorResponse("bad_request", "Specify /jobs/{id}"), mapper);
                    return;
                }
                String id = path.substring("/jobs/".length());
                respondWithJob(exchange, id);
                return;
            }

            respondJson(exchange, 405, new ErrorResponse("method_not_allowed", exchange.getRequestMethod()), mapper);
        }

        private void respondWithJob(HttpExchange exchange, String jobId) throws IOException {
            Row r = session.execute(selectJobStmt.bind(jobId)).one();
            if (r == null) {
                respondJson(exchange, 404, new ErrorResponse("not_found", "job " + jobId + " not found"), mapper);
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

            Instant latestStart = null;
            Instant latestEnd = null;
            String latestRunStatus = null;
            ResultSet hrs = session.execute(selectHistoryByJobStmt.bind(jobId));
            for (Row hr : hrs) {
                Instant s = hr.getInstant("start_at");
                if (latestStart == null || (s != null && s.isAfter(latestStart))) {
                    latestStart = s;
                    latestEnd = hr.getInstant("end_at");
                    latestRunStatus = hr.getString("status");
                }
            }
            respondJson(exchange, 200, new JobResponse(jobId, schedule, payloadObj, maxDur, createdAt,
                    latestRunStatus, latestStart, latestEnd, latestRunStatus), mapper);
        }

        // schedule parsing moved to top-level helpers
    }

    static class CronJobsHandler implements HttpHandler {
        private final CqlSession session;
        private final PreparedStatement insertCronJobStmt;
        private final PreparedStatement selectCronJobStmt;
        private final PreparedStatement insertCronPendingStmt;
        private final int buckets;
        private final ObjectMapper mapper;

        CronJobsHandler(CqlSession session,
                       PreparedStatement insertCronJobStmt,
                       PreparedStatement selectCronJobStmt,
                       PreparedStatement insertCronPendingStmt,
                       int buckets,
                       ObjectMapper mapper) {
            this.session = session;
            this.insertCronJobStmt = insertCronJobStmt;
            this.selectCronJobStmt = selectCronJobStmt;
            this.insertCronPendingStmt = insertCronPendingStmt;
            this.buckets = buckets;
            this.mapper = mapper;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                respondJson(exchange, 405, new ErrorResponse("method_not_allowed", exchange.getRequestMethod()), mapper);
                return;
            }
            try (InputStream is = exchange.getRequestBody()) {
                CronJobCreateRequest req = mapper.readValue(is, CronJobCreateRequest.class);
                String cronExpr = req.getCron();
                String timezone = (req.getTimezone() == null || req.getTimezone().isBlank()) ? "UTC" : req.getTimezone().trim();
                boolean enabled = req.getEnabled() == null ? true : req.getEnabled();
                int maxDur = req.getMaxDurationSeconds() == null ? 3600 : req.getMaxDurationSeconds();
                Object payloadObj = Optional.ofNullable(req.getPayload()).orElse(new java.util.HashMap<>());
                String payloadJson = mapper.writeValueAsString(payloadObj);

                Instant now = Instant.now();
                Instant nextRunAt = nextCronRun(cronExpr, timezone, now);

                UUID cronId = UUID.randomUUID();
                int bucket = Math.abs(cronId.hashCode()) % Math.max(1, buckets);

                session.execute(insertCronJobStmt.bind(
                        cronId,
                        cronExpr,
                        timezone,
                        payloadJson,
                        maxDur,
                        enabled,
                        bucket,
                        nextRunAt,
                        now,
                        now));

                if (enabled) {
                    session.execute(insertCronPendingStmt.bind(bucket, nextRunAt, cronId));
                }

                respondJson(exchange, 201, new CronJobResponse(
                        cronId.toString(), cronExpr, timezone, payloadObj, maxDur, enabled, nextRunAt, now), mapper);
            } catch (IllegalArgumentException ex) {
                respondJson(exchange, 400, new ErrorResponse("bad_request", ex.getMessage()), mapper);
            } catch (Exception ex) {
                respondJson(exchange, 400, new ErrorResponse("bad_request", ex.getMessage()), mapper);
            }
        }
    }

    static class CronJobActionHandler implements HttpHandler {
        private final CqlSession session;
        private final PreparedStatement selectCronJobStmt;
        private final PreparedStatement deleteCronJobStmt;
        private final ObjectMapper mapper;

        CronJobActionHandler(CqlSession session,
                             PreparedStatement selectCronJobStmt,
                             PreparedStatement deleteCronJobStmt,
                             ObjectMapper mapper) {
            this.session = session;
            this.selectCronJobStmt = selectCronJobStmt;
            this.deleteCronJobStmt = deleteCronJobStmt;
            this.mapper = mapper;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            if (!path.startsWith("/cronjobs/")) {
                respondJson(exchange, 404, new ErrorResponse("not_found", "invalid path"), mapper);
                return;
            }
            String id = path.substring("/cronjobs/".length());
            UUID cronId;
            try {
                cronId = UUID.fromString(id);
            } catch (IllegalArgumentException e) {
                respondJson(exchange, 400, new ErrorResponse("bad_request", "bad cron id"), mapper);
                return;
            }

            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                Row r = session.execute(selectCronJobStmt.bind(cronId)).one();
                if (r == null) {
                    respondJson(exchange, 404, new ErrorResponse("not_found", "cronjob " + id + " not found"), mapper);
                    return;
                }
                String cronExpr = r.getString("cron_expr");
                String timezone = r.getString("timezone");
                String payloadStr = r.getString("payload");
                Object payloadObj = null;
                try {
                    payloadObj = payloadStr == null ? null : mapper.readTree(payloadStr);
                } catch (Exception ignored) {
                }
                Integer maxDur = r.isNull("max_duration_seconds") ? null : r.getInt("max_duration_seconds");
                Boolean enabled = r.isNull("enabled") ? null : r.getBoolean("enabled");
                Instant nextRunAt = r.getInstant("next_run_at");
                Instant createdAt = r.getInstant("created_at");

                respondJson(exchange, 200, new CronJobResponse(id, cronExpr, timezone, payloadObj, maxDur, enabled, nextRunAt, createdAt), mapper);
                return;
            }

            if ("DELETE".equalsIgnoreCase(exchange.getRequestMethod())) {
                session.execute(deleteCronJobStmt.bind(cronId));
                respondJson(exchange, 200, new EnqueueResponse(id, "deleted"), mapper);
                return;
            }

            respondJson(exchange, 405, new ErrorResponse("method_not_allowed", exchange.getRequestMethod()), mapper);
        }
    }

    static class JobActionHandler implements HttpHandler {
        private final CqlSession session;
        private final PreparedStatement selectJobStmt;
        private final PreparedStatement enqueueJobStmt;
        private final PreparedStatement enqueuePendingStmt;
        private final PreparedStatement selectHistoryByJobStmt;
        private final int buckets;
        private final ObjectMapper mapper;

        JobActionHandler(CqlSession session,
                         PreparedStatement selectJobStmt,
                         PreparedStatement enqueueJobStmt,
                         PreparedStatement enqueuePendingStmt,
                         PreparedStatement selectHistoryByJobStmt,
                         int buckets,
                         ObjectMapper mapper) {
            this.session = session;
            this.selectJobStmt = selectJobStmt;
            this.enqueueJobStmt = enqueueJobStmt;
            this.enqueuePendingStmt = enqueuePendingStmt;
            this.selectHistoryByJobStmt = selectHistoryByJobStmt;
            this.buckets = buckets;
            this.mapper = mapper;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            if (!path.startsWith("/jobs/")) {
                respondJson(exchange, 404, new ErrorResponse("not_found", "invalid path"), mapper);
                return;
            }
            String remainder = path.substring("/jobs/".length());

            if (remainder.endsWith("/run")) {
                String id = remainder.substring(0, remainder.length() - "/run".length());
                UUID jobUuid;
                try {
                    jobUuid = UUID.fromString(id);
                } catch (IllegalArgumentException ex) {
                    respondJson(exchange, 400, new ErrorResponse("bad_request", "bad job id"), mapper);
                    return;
                }

                Row r = session.execute(selectJobStmt.bind(id)).one();
                if (r == null) {
                    respondJson(exchange, 404, new ErrorResponse("not_found", "job " + id + " not found"), mapper);
                    return;
                }
                String payload = r.getString("payload");
                if (payload == null) payload = "{}";

                int bucket = Math.abs(jobUuid.hashCode()) % buckets;
                Instant when = Instant.now();
                session.execute(enqueueJobStmt.bind(bucket, when, jobUuid, payload));
                session.execute(enqueuePendingStmt.bind(bucket, when, jobUuid, payload));
                respondJson(exchange, 200, new EnqueueResponse(id, "enqueued"), mapper);
                return;
            }

            // fall back: treat as GET /jobs/{id}
            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                String id = remainder;
                // delegate by reusing the same logic as JobsHandler would
                // minimal inline implementation:
                Row r = session.execute(selectJobStmt.bind(id)).one();
                if (r == null) {
                    respondJson(exchange, 404, new ErrorResponse("not_found", "job " + id + " not found"), mapper);
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

                Instant latestStart = null;
                Instant latestEnd = null;
                String latestRunStatus = null;
                ResultSet hrs = session.execute(selectHistoryByJobStmt.bind(id));
                for (Row hr : hrs) {
                    Instant s = hr.getInstant("start_at");
                    if (latestStart == null || (s != null && s.isAfter(latestStart))) {
                        latestStart = s;
                        latestEnd = hr.getInstant("end_at");
                        latestRunStatus = hr.getString("status");
                    }
                }
                respondJson(exchange, 200, new JobResponse(id, schedule, payloadObj, maxDur, createdAt,
                        latestRunStatus, latestStart, latestEnd, latestRunStatus), mapper);
                return;
            }

            respondJson(exchange, 400, new ErrorResponse("bad_request", "unsupported action"), mapper);
        }
    }
}
