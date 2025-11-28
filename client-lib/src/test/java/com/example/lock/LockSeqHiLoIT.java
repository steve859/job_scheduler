package com.example.lock;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.CassandraContainer;

import com.datastax.oss.driver.api.core.CqlSession;

public class LockSeqHiLoIT {
    private static CassandraContainer<?> cass;

    @BeforeClass
    public static void startCassandra() {
        cass = new CassandraContainer<>("cassandra:3.11");
        cass.start();
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cass.getHost(), cass.getFirstMappedPort()))
                .withLocalDatacenter("datacenter1")
                .build()) {
            session.execute(
                    "CREATE KEYSPACE IF NOT EXISTS scheduler WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("USE scheduler");
            session.execute("CREATE TABLE IF NOT EXISTS lock_seq (resource text PRIMARY KEY, last_seq bigint)");
        }
    }

    @AfterClass
    public static void stopCassandra() {
        if (cass != null)
            cass.stop();
    }

    private static LockClient newClient() {
        return new LockClient(cass.getHost(), cass.getFirstMappedPort(), "scheduler");
    }

    @Test
    public void nextSeq_isContinuous_andUnique_underConcurrency() throws Exception {
        String resource = "seq-" + UUID.randomUUID();
        int threads = 8;
        int perThread = 50;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<Long> all = Collections.synchronizedList(new ArrayList<>());

        try (LockClient client = newClient()) {
            for (int i = 0; i < threads; i++) {
                new Thread(() -> {
                    try {
                        start.await();
                        for (int j = 0; j < perThread; j++) {
                            long v = client.nextSeq(resource);
                            all.add(v);
                        }
                    } catch (Exception ignored) {
                    } finally {
                        done.countDown();
                    }
                }).start();
            }
            start.countDown();
            done.await();
        }

        // Validate uniqueness and continuity (no gaps)
        assertThat(all.size(), is(threads * perThread));
        List<Long> sorted = new ArrayList<>(all);
        Collections.sort(sorted);
        long min = sorted.get(0);
        long max = sorted.get(sorted.size() - 1);
        assertThat(max - min + 1, is((long) sorted.size()));
        // also ensure all values >=1
        assertThat(min >= 1L, is(true));
    }
}
