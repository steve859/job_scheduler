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
    private final LockMetrics metrics;
    private final int seqBlockSize;
    private final java.util.concurrent.ConcurrentHashMap<String, Block> seqBlocks = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<String, Object> seqLocks = new java.util.concurrent.ConcurrentHashMap<>();
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
        this(contactPoint, port, keyspace,
                System.getenv().getOrDefault("CASS_LOCAL_DC", "DC1"),
                LockMetrics.noop());
    }

    public LockClient(String contactPoint, int port, String keyspace, String localDc) {
        this(contactPoint, port, keyspace, localDc, LockMetrics.noop());
    }

    public LockClient(String contactPoint, int port, String keyspace, String localDc, LockMetrics metrics) {
        this.session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(localDc)
                .withKeyspace(keyspace)
                .build();
        this.keyspace = keyspace;
        this.metrics = (metrics == null ? LockMetrics.noop() : metrics);
        this.seqBlockSize = Integer.parseInt(System.getenv().getOrDefault("LOCK_SEQ_BLOCK_SIZE", "64"));

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
                        "UPDATE locks SET expire_at = ? WHERE resource_id = ? IF holder = ? AND fencing_token = ? AND expire_at = ?"));
        this.deleteIfHolderTokenStmt = session.prepare(
                SimpleStatement
                        .newInstance("DELETE FROM locks WHERE resource_id = ? IF holder = ? AND fencing_token = ?"));
    }

    /**
     * Obtain next monotonic fencing token for resource.
     * Uses CAS on lock_seq table.
     */
    public long nextSeq(String resource, int maxRetries, Duration retryBackoff) {
        // Hi/Lo block allocation to reduce Paxos contention.
        Object lock = seqLocks.computeIfAbsent(resource, r -> new Object());
        synchronized (lock) {
            Block b = seqBlocks.get(resource);
            if (b != null && b.next <= b.end) {
                return b.next++;
            }
            // Need to allocate a new block from Cassandra using LWT
            // First attempt to insert starting block if row not exists: last_seq =
            // seqBlockSize
            long t0 = System.nanoTime();
            BoundStatement ins = insertLockSeqIfNotExistsStmt.bind(resource, (long) seqBlockSize);
            ResultSet ir = session.execute(ins);
            metrics.observeLwtLatencySeconds("next_seq.block.insert", (System.nanoTime() - t0) / 1_000_000_000.0);
            Row irow = ir.one();
            if (irow != null && irow.getBoolean("[applied]")) {
                long start = 1L;
                long end = seqBlockSize;
                seqBlocks.put(resource, new Block(start, end));
                return start++;
            }

            for (int attempt = 0; attempt < maxRetries; attempt++) {
                long t1 = System.nanoTime();
                Row current = session.execute(selectLockSeqStmt.bind(resource)).one();
                long old = (current == null || current.isNull("last_seq")) ? 0L : current.getLong("last_seq");
                long nextLast = old + seqBlockSize;
                BoundStatement upd = updateLockSeqIfStmt.bind(nextLast, resource, old);
                ResultSet ur = session.execute(upd);
                metrics.observeLwtLatencySeconds("next_seq.block.update", (System.nanoTime() - t1) / 1_000_000_000.0);
                Row urow = ur.one();
                if (urow != null && urow.getBoolean("[applied]")) {
                    long start = old + 1;
                    long end = nextLast;
                    seqBlocks.put(resource, new Block(start, end));
                    return start++;
                }
                // contention -> backoff and retry
                sleepBackoffWithJitter(retryBackoff.toMillis());
            }
            // As a last resort, fall back to 1-by-1 increment (degraded mode)
            for (int attempt = 0; attempt < maxRetries; attempt++) {
                long t2 = System.nanoTime();
                Row current = session.execute(selectLockSeqStmt.bind(resource)).one();
                long old = (current == null || current.isNull("last_seq")) ? 0L : current.getLong("last_seq");
                long next = old + 1;
                BoundStatement upd1 = updateLockSeqIfStmt.bind(next, resource, old);
                ResultSet r2 = session.execute(upd1);
                metrics.observeLwtLatencySeconds("next_seq.fallback.update",
                        (System.nanoTime() - t2) / 1_000_000_000.0);
                Row r2row = r2.one();
                if (r2row != null && r2row.getBoolean("[applied]")) {
                    return next;
                }
                sleepBackoffWithJitter(retryBackoff.toMillis());
            }
            // final read-only fallback (not strictly monotonic under extreme race)
            Row finalRow = session.execute(selectLockSeqStmt.bind(resource)).one();
            long finalVal = (finalRow == null || finalRow.isNull("last_seq")) ? 1L : finalRow.getLong("last_seq") + 1;
            return finalVal;
        }
    }

    /** Convenience wrapper with defaults. */
    public long nextSeq(String resource) {
        return nextSeq(resource, 10, Duration.ofMillis(50));
    }

    private static class Block {
        long next;
        long end;

        Block(long start, long end) {
            this.next = start;
            this.end = end;
        }
    }

    /**
     * Blocking acquire: tries until timeout expires.
     * ttl and timeout in Duration.
     */
    public LockResult acquire(String resource, String holderId, Duration ttl, Duration timeout) {
        metrics.incAcquireAttempt();
        long timeoutMs = timeout.toMillis();
        long deadline = System.currentTimeMillis() + timeoutMs;
        long baseBackoffMs = 50;
        Duration seqBackoff = Duration.ofMillis(50);

        while (System.currentTimeMillis() < deadline) {
            final long token = nextSeq(resource, 10, seqBackoff);
            Instant expireAt = Instant.now().plusMillis(ttl.toMillis());

            // Try INSERT IF NOT EXISTS (epoch starts at 0)
            long tIns = System.nanoTime();
            BoundStatement ins = insertLockIfNotExistsStmt.bind(resource, holderId, token, expireAt);
            ResultSet res = session.execute(ins);
            metrics.observeLwtLatencySeconds("acquire.insert", (System.nanoTime() - tIns) / 1_000_000_000.0);
            Row row = res.one();
            if (row != null && row.getBoolean("[applied]")) {
                metrics.incAcquireSuccess();
                return new LockResult(true, token, expireAt);
            }

            // Not applied -> read current lock
            Row cur = session.execute(selectLockStmt.bind(resource)).one();
            if (cur != null) {
                Instant curExpire = cur.get("expire_at", Instant.class);
                long curEpoch = cur.isNull("epoch") ? 0L : cur.getLong("epoch");
                if (curExpire == null || curExpire.toEpochMilli() <= System.currentTimeMillis()) {
                    // expired -> try CAS replace using epoch increment (takeover)
                    long nextEpoch = curEpoch + 1;
                    long tUpd = System.nanoTime();
                    BoundStatement upd = updateLockIfTokenStmt.bind(
                            holderId, token, expireAt, nextEpoch, resource, curEpoch);
                    ResultSet upr = session.execute(upd);
                    metrics.observeLwtLatencySeconds("acquire.takeover", (System.nanoTime() - tUpd) / 1_000_000_000.0);
                    Row uprOne = upr.one();
                    if (uprOne != null && uprOne.getBoolean("[applied]")) {
                        metrics.incAcquireSuccess();
                        metrics.incTakeoverSuccess();
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
        metrics.incAcquireAttempt();
        final long token = nextSeq(resource, 5, Duration.ofMillis(50));
        Instant expireAt = Instant.now().plusMillis(ttl.toMillis());
        long tIns = System.nanoTime();
        BoundStatement ins = insertLockIfNotExistsStmt.bind(resource, holderId, token, expireAt);
        ResultSet res = session.execute(ins);
        metrics.observeLwtLatencySeconds("try_acquire.insert", (System.nanoTime() - tIns) / 1_000_000_000.0);
        Row row = res.one();
        if (row != null && row.getBoolean("[applied]")) {
            metrics.incAcquireSuccess();
            return new LockResult(true, token, expireAt);
        }
        // optionally attempt replace if expired
        Row cur = session.execute(selectLockStmt.bind(resource)).one();
        if (cur != null) {
            Instant curExpire = cur.get("expire_at", Instant.class);
            long curEpoch = cur.isNull("epoch") ? 0L : cur.getLong("epoch");
            if (curExpire == null || curExpire.toEpochMilli() <= System.currentTimeMillis()) {
                long nextEpoch = curEpoch + 1;
                long tUpd = System.nanoTime();
                BoundStatement upd = updateLockIfTokenStmt.bind(
                        holderId, token, expireAt, nextEpoch, resource, curEpoch);
                ResultSet upr = session.execute(upd);
                metrics.observeLwtLatencySeconds("try_acquire.takeover", (System.nanoTime() - tUpd) / 1_000_000_000.0);
                Row uprOne = upr.one();
                if (uprOne != null && uprOne.getBoolean("[applied]")) {
                    metrics.incAcquireSuccess();
                    metrics.incTakeoverSuccess();
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
        // Read current lock to ensure it's still valid and owned by the caller
        Row cur = session.execute(selectLockStmt.bind(resource)).one();
        if (cur == null) {
            metrics.incRenewFailure();
            return false;
        }
        String curHolder = cur.getString("holder");
        long curToken = cur.isNull("fencing_token") ? -1L : cur.getLong("fencing_token");
        Instant curExpire = cur.get("expire_at", Instant.class);
        long nowMs = System.currentTimeMillis();
        if (!holderId.equals(curHolder) || curToken != token || curExpire == null
                || curExpire.toEpochMilli() <= nowMs) {
            metrics.incRenewFailure();
            return false;
        }

        Instant newExpire = Instant.now().plusMillis(ttl.toMillis());
        long t = System.nanoTime();
        BoundStatement b = renewStmt.bind(newExpire, resource, holderId, token, curExpire);
        ResultSet r = session.execute(b);
        metrics.observeLwtLatencySeconds("renew.update", (System.nanoTime() - t) / 1_000_000_000.0);
        Row row = r.one();
        boolean ok = row != null && row.getBoolean("[applied]");
        if (ok)
            metrics.incRenewSuccess();
        else
            metrics.incRenewFailure();
        return ok;
    }

    /**
     * Release lock if holder/token match.
     */
    public boolean release(String resource, String holderId, long token) {
        long t = System.nanoTime();
        BoundStatement b = deleteIfHolderTokenStmt.bind(resource, holderId, token);
        ResultSet r = session.execute(b);
        metrics.observeLwtLatencySeconds("release.delete", (System.nanoTime() - t) / 1_000_000_000.0);
        Row row = r.one();
        boolean ok = row != null && row.getBoolean("[applied]");
        if (ok)
            metrics.incReleaseSuccess();
        else
            metrics.incReleaseFailure();
        return ok;
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
