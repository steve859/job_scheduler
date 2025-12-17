package com.example.orders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.sun.net.httpserver.HttpServer;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class OrdersIT {
    private static PostgreSQLContainer<?> pg;
    private static HikariDataSource ds;
    private static HttpServer server;
    private static int port;

    @BeforeClass
    public static void setup() throws Exception {
        pg = new PostgreSQLContainer<>("postgres:15").withInitScript("init.sql");
        pg.start();

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(pg.getJdbcUrl());
        hc.setUsername(pg.getUsername());
        hc.setPassword(pg.getPassword());
        ds = new HikariDataSource(hc);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        // bind to ephemeral port 0
        server = OrdersMain.startServer(ds, mapper, 0);
        port = server.getAddress().getPort();
    }

    @AfterClass
    public static void teardown() {
        if (server != null)
            server.stop(0);
        if (ds != null)
            ds.close();
        if (pg != null)
            pg.stop();
    }

    @Test
    public void checkout_is_idempotent() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String body = "{\"idempotencyKey\":\"KEY-IT-1\",\"userId\":\"u1\",\"items\":[{\"sku\":\"SKU-RED-TSHIRT\",\"qty\":1,\"priceCents\":1999}]}";
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/checkout"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> r1 = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(r1.statusCode(), is(201));
        String orderId1 = r1.body().replaceAll(".*\"orderId\":\"([^\"]+)\".*", "$1");
        assertThat(orderId1.length() > 0, is(true));

        HttpResponse<String> r2 = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(r2.statusCode(), is(200));
        String orderId2 = r2.body().replaceAll(".*\"orderId\":\"([^\"]+)\".*", "$1");
        assertThat(orderId2, is(orderId1));
    }

    @Test
    public void concurrent_checkout_prevents_oversell() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        // Prior test consumed 1 unit; request 99 to consume remaining stock.
        String body1 = "{\"idempotencyKey\":\"KEY-IT-2A\",\"userId\":\"u1\",\"items\":[{\"sku\":\"SKU-RED-TSHIRT\",\"qty\":99,\"priceCents\":1999}]}";
        String body2 = "{\"idempotencyKey\":\"KEY-IT-2B\",\"userId\":\"u2\",\"items\":[{\"sku\":\"SKU-RED-TSHIRT\",\"qty\":99,\"priceCents\":1999}]}";
        HttpRequest req1 = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/checkout"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body1))
                .build();
        HttpRequest req2 = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/checkout"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body2))
                .build();

        CompletableFuture<HttpResponse<String>> f1 = client.sendAsync(req1, HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> f2 = client.sendAsync(req2, HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> r1 = f1.get();
        HttpResponse<String> r2 = f2.get();

        // One should succeed (201), the other should conflict (409)
        boolean ok201 = r1.statusCode() == 201 || r2.statusCode() == 201;
        boolean conflict409 = r1.statusCode() == 409 || r2.statusCode() == 409;
        assertTrue("expect one 201 and one 409, got: " + r1.statusCode() + ", " + r2.statusCode(),
                ok201 && conflict409);
    }
}
