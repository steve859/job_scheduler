package com.example.lock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Minimal concurrent benchmark for LockClient acquire/renew/release.
 * Env vars:
 * - CASS_CONTACT (default 127.0.0.1)
 * - CASS_PORT (default 9042)
 * - CASS_KEYSPACE (default scheduler)
 * - THREADS (default 8)
 * - DURATION_SECONDS (default 60)
 * - TTL_SECONDS (default 5)
 * - RESOURCE_PREFIX (default bench-)
 */
public class ClientBench {
    public static void main(String[] args) throws Exception {
        String contactPoint = System.getenv().getOrDefault("CASS_CONTACT", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("CASS_PORT", "9042"));
        String keyspace = System.getenv().getOrDefault("CASS_KEYSPACE", "scheduler");
        int threads = Integer.parseInt(System.getenv().getOrDefault("THREADS", "8"));
        int durationSec = Integer.parseInt(System.getenv().getOrDefault("DURATION_SECONDS", "60"));
        int ttlSec = Integer.parseInt(System.getenv().getOrDefault("TTL_SECONDS", "5"));
        String prefix = System.getenv().getOrDefault("RESOURCE_PREFIX", "bench-");

        System.out.printf("Starting bench: %d threads, %ds, ttl=%ds%n", threads, durationSec, ttlSec);

        try (LockClient client = new LockClient(contactPoint, port, keyspace)) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            AtomicBoolean stop = new AtomicBoolean(false);
            LongAdder successes = new LongAdder();
            LongAdder failures = new LongAdder();
            List<Long> latSamples = Collections.synchronizedList(new ArrayList<>(50000));

            Runnable task = () -> {
                String holder = "bench-" + UUID.randomUUID();
                ThreadLocal<java.util.Random> rnd = ThreadLocal.withInitial(java.util.Random::new);
                while (!stop.get()) {
                    String resource = prefix + rnd.get().nextInt(1000);
                    long t0 = System.nanoTime();
                    LockResult r = client.acquire(resource, holder, Duration.ofSeconds(ttlSec), Duration.ofSeconds(2));
                    long durUs = (System.nanoTime() - t0) / 1000;
                    if (r.isAcquired()) {
                        successes.increment();
                        if (latSamples.size() < 50000)
                            latSamples.add(durUs);
                        // brief work then release
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        client.release(resource, holder, r.getToken());
                    } else {
                        failures.increment();
                    }
                }
            };

            for (int i = 0; i < threads; i++)
                pool.submit(task);
            long start = System.nanoTime();
            TimeUnit.SECONDS.sleep(durationSec);
            stop.set(true);
            pool.shutdown();
            pool.awaitTermination(30, TimeUnit.SECONDS);
            long elapsedSec = Math.max(1, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start));

            long ok = successes.sum();
            long ko = failures.sum();
            double qps = ok * 1.0 / elapsedSec;
            double p95 = percentile(latSamples, 95);
            double p99 = percentile(latSamples, 99);

            System.out.println("==== Bench Results ====");
            System.out.printf("throughput_ok_qps=%.2f, ok=%d, fail=%d, p95_acquire_us=%.0f, p99_acquire_us=%.0f%n",
                    qps, ok, ko, p95, p99);
        }
    }

    static double percentile(List<Long> samples, int pct) {
        if (samples.isEmpty())
            return Double.NaN;
        List<Long> copy;
        synchronized (samples) {
            copy = new ArrayList<>(samples);
        }
        Collections.sort(copy);
        int idx = Math.min(copy.size() - 1, (int) Math.ceil((pct / 100.0) * copy.size()) - 1);
        return copy.get(idx);
    }
}
