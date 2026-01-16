package com.example.orders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.sql.*;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class OrdersMain {
    public static void main(String[] args) throws Exception {
        String host = env("PG_HOST", "localhost");
        int port = Integer.parseInt(env("PG_PORT", "5432"));
        String db = env("PG_DB", "orders");
        String user = env("PG_USER", "app");
        String pass = env("PG_PASSWORD", "app");
        int httpPort = Integer.parseInt(env("ORDERS_HTTP_PORT", "8081"));

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl("jdbc:postgresql://" + host + ":" + port + "/" + db);
        hc.setUsername(user);
        hc.setPassword(pass);
        hc.setMaximumPoolSize(10);
        HikariDataSource ds = new HikariDataSource(hc);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        HttpServer server = startServer(ds, mapper, httpPort);
        System.out.println("[orders] HTTP server on " + server.getAddress().getPort());
    }

    // Exposed for tests
    public static HttpServer startServer(HikariDataSource ds, ObjectMapper mapper, int httpPort) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
        server.createContext("/healthz", ex -> respond(ex, 200, "OK"));
        server.createContext("/checkout", ex -> handleCheckout(ex, ds, mapper));
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();
        return server;
    }

    static void handleCheckout(HttpExchange ex, HikariDataSource ds, ObjectMapper mapper) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            respond(ex, 405, "Method Not Allowed");
            return;
        }
        CheckoutRequest req;
        try (InputStream is = ex.getRequestBody()) {
            req = mapper.readValue(is, CheckoutRequest.class);
        } catch (Exception e) {
            respond(ex, 400, "bad_request");
            return;
        }
        if (req == null || req.idempotencyKey == null || req.userId == null || req.items == null
                || req.items.isEmpty()) {
            respond(ex, 400, "missing fields");
            return;
        }
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                // Idempotency check
                UUID existing = findExistingOrderId(c, req.idempotencyKey);
                if (existing != null) {
                    c.commit();
                    respondJson(ex, 200, new CheckoutResponse(existing.toString(), "idempotent"), mapper);
                    return;
                }
                insertIdempotencyKey(c, req.idempotencyKey);

                UUID orderId = UUID.randomUUID();
                int total = req.items.stream().mapToInt(it -> it.priceCents * it.qty).sum();
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO orders(id, user_id, status, total_cents, created_at) VALUES (?,?,?,?,?)")) {
                    ps.setObject(1, orderId);
                    ps.setString(2, req.userId);
                    ps.setString(3, "PENDING");
                    ps.setInt(4, total);
                    ps.setTimestamp(5, Timestamp.from(Instant.now()));
                    ps.executeUpdate();
                }
                int line = 0;
                for (CheckoutItem it : req.items) {
                    // optimistic locking on inventory
                    InventoryRow inv = loadInventory(c, it.sku);
                    if (inv == null || inv.available < it.qty) {
                        c.rollback();
                        respond(ex, 409, "insufficient_inventory");
                        return;
                    }
                    try (PreparedStatement up = c.prepareStatement(
                            "UPDATE inventory SET available = available - ?, reserved = reserved + ?, version = version + 1, updated_at = now() WHERE sku = ? AND version = ?")) {
                        up.setInt(1, it.qty);
                        up.setInt(2, it.qty);
                        up.setString(3, it.sku);
                        up.setInt(4, inv.version);
                        int updated = up.executeUpdate();
                        if (updated == 0) {
                            c.rollback();
                            respond(ex, 409, "inventory_conflict");
                            return;
                        }
                    }
                    try (PreparedStatement ins = c.prepareStatement(
                            "INSERT INTO order_items(order_id, line_no, sku, qty, price_cents) VALUES (?,?,?,?,?)")) {
                        ins.setObject(1, orderId);
                        ins.setInt(2, ++line);
                        ins.setString(3, it.sku);
                        ins.setInt(4, it.qty);
                        ins.setInt(5, it.priceCents);
                        ins.executeUpdate();
                    }
                }
                // set order_id for idempotency key
                try (PreparedStatement ps = c
                        .prepareStatement("UPDATE idempotency_keys SET order_id=? WHERE idempotency_key=?")) {
                    ps.setObject(1, orderId);
                    ps.setString(2, req.idempotencyKey);
                    ps.executeUpdate();
                }
                c.commit();
                // Best-effort enqueue payment job to worker if configured
                try {
                    enqueuePayment(orderId);
                } catch (Exception ignore) {
                }
                respondJson(ex, 201, new CheckoutResponse(orderId.toString(), "created"), mapper);
            } catch (SQLException sqle) {
                c.rollback();
                if (isUniqueViolation(sqle)) {
                    // Idempotency key inserted concurrently: fetch and return
                    UUID existing = findExistingOrderId(c, req.idempotencyKey);
                    if (existing != null) {
                        respondJson(ex, 200, new CheckoutResponse(existing.toString(), "idempotent"), mapper);
                    } else {
                        respond(ex, 409, "idempotency_conflict");
                    }
                } else {
                    respond(ex, 500, "db_error");
                }
            } finally {
                c.setAutoCommit(true);
            }
        } catch (SQLException e) {
            respond(ex, 500, "db_unavailable");
        }
    }

    static void enqueuePayment(UUID orderId) {
        String workerUrl = env("WORKER_URL", "");
        if (workerUrl == null || workerUrl.isEmpty())
            return;
        try {
            String payload = "{\"orderId\":\"" + orderId + "\"}";
            String body = "{\"schedule\":\"now\",\"payload\":" + payload + ",\"max_duration_seconds\":600}";
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(workerUrl + "/jobs"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            System.err.println("[orders] enqueue payment failed: " + e.getMessage());
        }
    }

    static boolean isUniqueViolation(SQLException e) {
        return "23505".equals(e.getSQLState());
    }

    static InventoryRow loadInventory(Connection c, String sku) throws SQLException {
        try (PreparedStatement ps = c
                .prepareStatement("SELECT sku, available, reserved, version FROM inventory WHERE sku = ? FOR UPDATE")) {
            ps.setString(1, sku);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next())
                    return null;
                InventoryRow r = new InventoryRow();
                r.sku = rs.getString(1);
                r.available = rs.getInt(2);
                r.reserved = rs.getInt(3);
                r.version = rs.getInt(4);
                return r;
            }
        }
    }

    static void insertIdempotencyKey(Connection c, String key) throws SQLException {
        try (PreparedStatement ps = c
                .prepareStatement("INSERT INTO idempotency_keys(idempotency_key, created_at) VALUES (?, now())")) {
            ps.setString(1, key);
            ps.executeUpdate();
        }
    }

    static UUID findExistingOrderId(Connection c, String key) throws SQLException {
        try (PreparedStatement ps = c
                .prepareStatement("SELECT order_id FROM idempotency_keys WHERE idempotency_key = ?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Object obj = rs.getObject(1);
                    if (obj instanceof UUID)
                        return (UUID) obj;
                }
            }
        }
        return null;
    }

    static void respond(HttpExchange ex, int code, String body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(code, body.getBytes().length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body.getBytes());
        }
    }

    static void respondJson(HttpExchange ex, int code, Object value, ObjectMapper mapper) throws IOException {
        byte[] data = mapper.writeValueAsBytes(value);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, data.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(data);
        }
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null ? d : v;
    }

    // DTOs
    public static class CheckoutRequest {
        public String idempotencyKey;
        public String userId;
        public List<CheckoutItem> items;
    }

    public static class CheckoutItem {
        public String sku;
        public int qty;
        public int priceCents;
    }

    public static class CheckoutResponse {
        public String orderId;
        public String status;

        public CheckoutResponse() {
        }

        public CheckoutResponse(String orderId, String status) {
            this.orderId = orderId;
            this.status = status;
        }
    }

    static class InventoryRow {
        String sku;
        int available;
        int reserved;
        int version;
    }
}
