package com.example.worker;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ResultSet;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Optional;

/**
 * Minimal poller for jobs_sharded.
 * NOTE: status filtering is done client-side for now to avoid ALLOW FILTERING.
 */
public class JobPoller implements AutoCloseable {
    private final CqlSession session;
    private final PreparedStatement selectDueStmt;

    public JobPoller(String contactPoint, int port, String keyspace) {
        String localDc = System.getenv().getOrDefault("CASS_LOCAL_DC", "DC1");
        this.session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(localDc)
                .withKeyspace(keyspace)
                .build();
        this.selectDueStmt = session.prepare(
                "SELECT scheduled_time, job_id, payload, status, attempts FROM jobs_sharded WHERE bucket_id = ? AND scheduled_time <= ? LIMIT 32");
    }

    public Optional<Row> pollNext(int bucketId) {
        BoundStatement bs = selectDueStmt.bind(bucketId, Instant.now());
        ResultSet rs = session.execute(bs);
        for (Row row : rs) {
            String status = row.getString("status");
            if (status == null || "pending".equalsIgnoreCase(status)) {
                return Optional.of(row);
            }
        }
        return Optional.empty();
    }

    @Override
    public void close() {
        if (session != null)
            session.close();
    }
}
