package com.example.lock;

import java.time.Duration;

public class ClientSmoke {
    public static void main(String[] args) throws Exception {
        String contactPoint = System.getenv().getOrDefault("CASS_CONTACT", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("CASS_PORT", "9042"));
        String keyspace = System.getenv().getOrDefault("CASS_KEYSPACE", "scheduler");

        try (LockClient client = new LockClient(contactPoint, port, keyspace)) {
            System.out.println("Connected to Cassandra at " + contactPoint + ":" + port);
            LockResult r = client.acquire("job-smoke-1", "worker-smoke", Duration.ofSeconds(30),
                    Duration.ofSeconds(10));
            System.out.println("Acquire result: " + r);
            if (r.isAcquired()) {
                boolean renewed = client.renew("job-smoke-1", "worker-smoke", r.getToken(), Duration.ofSeconds(30));
                System.out.println("Renewed: " + renewed);
                boolean released = client.release("job-smoke-1", "worker-smoke", r.getToken());
                System.out.println("Released: " + released);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
