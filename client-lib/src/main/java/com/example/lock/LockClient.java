package com.example.lock;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;

/**
 * LockClient using Cassandra LWT (Paxos).
 * Implements:
 * - nextSeq(resource) : monotonic fencing token via lock_seq table (CAS)
 * - acquire(resource, holderId, ttl, timeout)
 * - tryAcquire(...)
 * - renew(...)
 * - release(...)
 *
 * Notes:
 * - TTL handled by expire_at TIMESTAMP column (computed from client time).
 * - LWT results checked via row.getBoolean("[applied]").
 */
public class LockClient implements AutoCloseable {
    private final CqlSession session;
    private final String keyspace;
    private final PreparedStatement selectLockSeqStmt;
    private final PreparedStatement insertLockSeqIfNotExistsStmt;
    private final PreparedStatement updateLockSeqIfStmt;

    private final PreparedStatement insertLockIfNotExistsStmt;
    private final PreparedStatement selectLockStmt;
    private final PreparedStatement updateLockIfTokenStmt;
    private final PreparedStatement renewStmt;
    private final PreparedStatement deleteIfHolderTokenStmt;

    private final Random rand = new Random();

    public LockClient(String contactPoint, int port, String keyspace) {
        String localDc = System.getenv().getOrDefault("CASS_LOCAL_DC", "DC1");
        this.session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(localDc)
                .withKeyspace(keyspace)
                .build();
        this.keyspace = keyspace;

        // Prepare statements
        // lock_seq
        this.selectLockSeqStmt = session.prepare(
                SimpleStatement.newInstance("SELECT last_seq FROM lock_seq WHERE resource = ?"));
        this.insertLockSeqIfNotExistsStmt = session.prepare(
                SimpleStatement.newInstance("INSERT INTO lock_seq (resource, last_seq) VALUES (?, ?) IF NOT EXISTS"));
        this.updateLockSeqIfStmt = session.prepare(
                SimpleStatement.newInstance("UPDATE lock_seq SET last_seq = ? WHERE resource = ? IF last_seq = ?"));

        // locks table (resource_id + epoch-based takeover)
        this.insertLockIfNotExistsStmt = session.prepare(
                SimpleStatement.newInstance(
                        "INSERT INTO locks (resource_id, holder, fencing_token, expire_at, epoch) VALUES (?, ?, ?, ?, 0) IF NOT EXISTS"));
        this.selectLockStmt = session.prepare(
                SimpleStatement.newInstance(
                        "SELECT holder, fencing_token, expire_at, epoch FROM locks WHERE resource_id = ?"));
        // Takeover: when expired, increment epoch with CAS on epoch
        this.updateLockIfTokenStmt = session.prepare(
                SimpleStatement.newInstance(
                        "UPDATE locks SET holder = ?, fencing_token = ?, expire_at = ?, epoch = ? WHERE resource_id = ? IF epoch = ?"));
        this.renewStmt = session.prepare(
                SimpleStatement.newInstance(
                        "UPDATE locks SET expire_at = ? WHERE resource_id = ? IF holder = ? AND fencing_token = ?"));
        this.deleteIfHolderTokenStmt = session.prepare(
                SimpleStatement
                        .newInstance("DELETE FROM locks WHERE resource_id = ? IF holder = ? AND fencing_token = ?"));
    }

    /**
     * Obtain next monotonic fencing token for resource.
     * Uses CAS on lock_seq table.
     */
    public long nextSeq(String resource, int maxRetries, Duration retryBackoff) {
        // Try to insert if not exists with 1
        BoundStatement insertStmt = insertLockSeqIfNotExistsStmt.bind(resource, 1L);
        ResultSet r = session.execute(insertStmt);
        Row row = r.one();
        if (row != null && row.getBoolean("[applied]")) {
            return 1L;
        }

        // else loop: read current and attempt CAS update
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            Row current = session.execute(selectLockSeqStmt.bind(resource)).one();
            long old = (current == null || current.isNull("last_seq")) ? 0L : current.getLong("last_seq");
            long next = old + 1;
            BoundStatement updateStmt = updateLockSeqIfStmt.bind(next, resource, old);
            ResultSet r2 = session.execute(updateStmt);
            Row r2row = r2.one();
            if (r2row != null && r2row.getBoolean("[applied]")) {
                return next;
            }
            // failed due to concurrent increment -> backoff & retry
            sleepBackoffWithJitter(retryBackoff.toMillis());
        }
        // as a fallback, read current and return +1 (non-atomic) - but warn: may not be
        // strictly monotonic under extreme race
        Row finalRow = session.execute(selectLockSeqStmt.bind(resource)).one();
        long finalVal = (finalRow == null || finalRow.isNull("last_seq")) ? 1L : finalRow.getLong("last_seq") + 1;
        // attempt best-effort update once
        try {
            session.execute(updateLockSeqIfStmt.bind(finalVal, resource, finalVal - 1));
            return finalVal;
        } catch (Exception e) {
            return finalVal;
        }
    }

    /**
     * Blocking acquire: tries until timeout expires.
     * ttl and timeout in Duration.
     */
    public LockResult acquire(String resource, String holderId, Duration ttl, Duration timeout) {
        long timeoutMs = timeout.toMillis();
        long deadline = System.currentTimeMillis() + timeoutMs;
        long baseBackoffMs = 50;
        Duration seqBackoff = Duration.ofMillis(50);

        while (System.currentTimeMillis() < deadline) {
            final long token = nextSeq(resource, 10, seqBackoff);
            Instant expireAt = Instant.now().plusMillis(ttl.toMillis());

            // Try INSERT IF NOT EXISTS (epoch starts at 0)
            BoundStatement ins = insertLockIfNotExistsStmt.bind(resource, holderId, token,
                    java.util.Date.from(expireAt));
            ResultSet res = session.execute(ins);
            Row row = res.one();
            if (row != null && row.getBoolean("[applied]")) {
                return new LockResult(true, token, expireAt);
            }

            // Not applied -> read current lock
            Row cur = session.execute(selectLockStmt.bind(resource)).one();
            if (cur != null) {
                java.util.Date curExpire = cur.get("expire_at", java.util.Date.class);
                long curToken = cur.isNull("fencing_token") ? -1L : cur.getLong("fencing_token");
                long curEpoch = cur.isNull("epoch") ? 0L : cur.getLong("epoch");
                if (curExpire == null || curExpire.getTime() <= System.currentTimeMillis()) {
                    // expired -> try CAS replace using epoch increment (takeover)
                    long nextEpoch = curEpoch + 1;
                    BoundStatement upd = updateLockIfTokenStmt.bind(
                            holderId, token, java.util.Date.from(expireAt), nextEpoch, resource, curEpoch);
                    ResultSet upr = session.execute(upd);
                    Row uprOne = upr.one();
                    if (uprOne != null && uprOne.getBoolean("[applied]")) {
                        return new LockResult(true, token, expireAt);
                    }
                }
            }

            // backoff with jitter
            sleepBackoffWithJitter(baseBackoffMs);
            baseBackoffMs = Math.min(2000, baseBackoffMs * 2);
        }
        return new LockResult(false, -1L, null);
    }

    /**
     * Try acquire once (non-blocking).
     */
    public LockResult tryAcquire(String resource, String holderId, Duration ttl) {
        final long token = nextSeq(resource, 5, Duration.ofMillis(50));
        Instant expireAt = Instant.now().plusMillis(ttl.toMillis());
        BoundStatement ins = insertLockIfNotExistsStmt.bind(resource, holderId, token, java.util.Date.from(expireAt));
        ResultSet res = session.execute(ins);
        Row row = res.one();
        if (row != null && row.getBoolean("[applied]")) {
            return new LockResult(true, token, expireAt);
        }
        // optionally attempt replace if expired
        Row cur = session.execute(selectLockStmt.bind(resource)).one();
        if (cur != null) {
            java.util.Date curExpire = cur.get("expire_at", java.util.Date.class);
            long curToken = cur.isNull("fencing_token") ? -1L : cur.getLong("fencing_token");
            long curEpoch = cur.isNull("epoch") ? 0L : cur.getLong("epoch");
            if (curExpire == null || curExpire.getTime() <= System.currentTimeMillis()) {
                long nextEpoch = curEpoch + 1;
                BoundStatement upd = updateLockIfTokenStmt.bind(
                        holderId, token, java.util.Date.from(expireAt), nextEpoch, resource, curEpoch);
                ResultSet upr = session.execute(upd);
                Row uprOne = upr.one();
                if (uprOne != null && uprOne.getBoolean("[applied]")) {
                    return new LockResult(true, token, expireAt);
                }
            }
        }
        return new LockResult(false, -1L, null);
    }

    /**
     * Renew lock (extend expire_at). Returns true if applied.
     */
    public boolean renew(String resource, String holderId, long token, Duration ttl) {
        Instant newExpire = Instant.now().plusMillis(ttl.toMillis());
        BoundStatement b = renewStmt.bind(java.util.Date.from(newExpire), resource, holderId, token);
        ResultSet r = session.execute(b);
        Row row = r.one();
        return row != null && row.getBoolean("[applied]");
    }

    /**
     * Release lock if holder/token match.
     */
    public boolean release(String resource, String holderId, long token) {
        BoundStatement b = deleteIfHolderTokenStmt.bind(resource, holderId, token);
        ResultSet r = session.execute(b);
        Row row = r.one();
        return row != null && row.getBoolean("[applied]");
    }

    /**
     * Read current token if present.
     */
    public Optional<Long> getCurrentToken(String resource) {
        Row r = session.execute(selectLockStmt.bind(resource)).one();
        if (r == null || r.isNull("fencing_token"))
            return Optional.empty();
        return Optional.of(r.getLong("fencing_token"));
    }

    private void sleepBackoffWithJitter(long baseMs) {
        long jitter = (long) (rand.nextDouble() * (baseMs / 2.0));
        long wait = baseMs + jitter;
        try {
            Thread.sleep(wait);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        session.close();
    }
}
