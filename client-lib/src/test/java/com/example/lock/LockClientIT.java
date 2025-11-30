package com.example.lock;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.CassandraContainer;

import com.datastax.oss.driver.api.core.CqlSession;

public class LockClientIT {
    private static final String KEYSPACE = "scheduler";
    private static CassandraContainer<?> cass;

    @BeforeClass
    public static void startCassandra() {
        cass = new CassandraContainer<>("cassandra:3.11");
        cass.start();
        try (CqlSession s = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(cass.getHost(), cass.getFirstMappedPort()))
                .withLocalDatacenter("datacenter1")
                .build()) {
            s.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
                    + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            s.execute("USE " + KEYSPACE);
            s.execute("CREATE TABLE IF NOT EXISTS lock_seq (resource text PRIMARY KEY, last_seq bigint)");
            s.execute(
                    "CREATE TABLE IF NOT EXISTS locks (resource_id text PRIMARY KEY, holder text, fencing_token bigint, expire_at timestamp, epoch bigint, metadata map<text,text>)");
        }
    }

    @AfterClass
    public static void stopCassandra() {
        if (cass != null)
            cass.stop();
    }

    private static LockClient newClient(LockMetrics metrics) {
        return new LockClient(cass.getHost(), cass.getFirstMappedPort(), KEYSPACE, "datacenter1", metrics);
    }

    @Test
    public void raceOnlyOneAcquires() throws Exception {
        AtomicInteger acquired = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(2);
        Runnable r = () -> {
            try (LockClient c = newClient(LockMetrics.noop())) {
                LockResult res = c.acquire("res-race-1", "w" + Thread.currentThread().getId(), Duration.ofSeconds(5),
                        Duration.ofSeconds(2));
                if (res.isAcquired())
                    acquired.incrementAndGet();
            } finally {
                latch.countDown();
            }
        };
        new Thread(r).start();
        new Thread(r).start();
        latch.await();
        assertThat("exactly one should acquire", acquired.get() == 1, is(true));
    }

    @Test
    public void takeoverAfterExpire() throws Exception {
        try (LockClient c1 = newClient(LockMetrics.noop()); LockClient c2 = newClient(LockMetrics.noop())) {
            LockResult r1 = c1.acquire("res-expire-1", "w1", Duration.ofMillis(800), Duration.ofSeconds(2));
            assertThat(r1.isAcquired(), is(true));
            Thread.sleep(1200); // let it expire
            LockResult r2 = c2.acquire("res-expire-1", "w2", Duration.ofSeconds(2), Duration.ofSeconds(2));
            assertThat(r2.isAcquired(), is(true));
            boolean renewOld = c1.renew("res-expire-1", "w1", r1.getToken(), Duration.ofSeconds(1));
            assertThat("renew after expiry should fail", renewOld, is(false));
        }
    }

    @Test
    public void renewSuccessThenFailAfterExpiry() throws Exception {
        class CountingMetrics implements LockMetrics {
            int renewOk = 0, renewFail = 0;

            public void observeLwtLatencySeconds(String op, double seconds) {
            }

            public void incAcquireAttempt() {
            }

            public void incAcquireSuccess() {
            }

            public void incTakeoverSuccess() {
            }

            public void incRenewSuccess() {
                renewOk++;
            }

            public void incRenewFailure() {
                renewFail++;
            }

            public void incReleaseSuccess() {
            }

            public void incReleaseFailure() {
            }
        }
        CountingMetrics m = new CountingMetrics();
        try (LockClient c = newClient(m)) {
            LockResult r = c.acquire("res-renew-1", "w1", Duration.ofMillis(600), Duration.ofSeconds(2));
            assertThat(r.isAcquired(), is(true));
            boolean ok1 = c.renew("res-renew-1", "w1", r.getToken(), Duration.ofMillis(600));
            assertThat(ok1, is(true));
            Thread.sleep(900);
            boolean ok2 = c.renew("res-renew-1", "w1", r.getToken(), Duration.ofMillis(600));
            assertThat(ok2, is(false));
            assertThat(m.renewOk >= 1, is(true));
            assertThat(m.renewFail >= 1, is(true));
        }
    }
}
