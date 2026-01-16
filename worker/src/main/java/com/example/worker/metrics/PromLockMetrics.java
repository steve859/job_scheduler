package com.example.worker.metrics;

import com.example.lock.LockMetrics;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * Prometheus-backed implementation of LockMetrics.
 */
public class PromLockMetrics implements LockMetrics {
    private final CollectorRegistry registry;
    private final Counter acquireAttempts;
    private final Counter acquireSuccess;
    private final Counter takeoverSuccess;
    private final Counter renewSuccess;
    private final Counter renewFailure;
    private final Counter releaseSuccess;
    private final Counter releaseFailure;
    private final Histogram lwtLatencySeconds;

    public PromLockMetrics(CollectorRegistry registry) {
        this.registry = registry;
        // register default JVM metrics once
        DefaultExports.initialize();

        this.acquireAttempts = Counter.build()
                .name("lock_acquire_attempt_total")
                .help("Total lock acquire attempts")
                .register(registry);
        this.acquireSuccess = Counter.build()
                .name("lock_acquire_success_total")
                .help("Total successful lock acquires")
                .register(registry);
        this.takeoverSuccess = Counter.build()
                .name("lock_takeover_success_total")
                .help("Total successful lock takeovers after expiry")
                .register(registry);
        this.renewSuccess = Counter.build()
                .name("lock_renew_success_total")
                .help("Total successful lock renewals")
                .register(registry);
        this.renewFailure = Counter.build()
                .name("lock_renew_failure_total")
                .help("Total failed lock renewals")
                .register(registry);
        this.releaseSuccess = Counter.build()
                .name("lock_release_success_total")
                .help("Total successful lock releases")
                .register(registry);
        this.releaseFailure = Counter.build()
                .name("lock_release_failure_total")
                .help("Total failed lock releases")
                .register(registry);
        this.lwtLatencySeconds = Histogram.build()
                .name("lwt_latency_seconds")
                .help("Latency of LWT operations in seconds")
                .buckets(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0)
                .labelNames("op")
                .register(registry);
    }

    @Override
    public void observeLwtLatencySeconds(String op, double seconds) {
        lwtLatencySeconds.labels(op).observe(seconds);
    }

    @Override
    public void incAcquireAttempt() {
        acquireAttempts.inc();
    }

    @Override
    public void incAcquireSuccess() {
        acquireSuccess.inc();
    }

    @Override
    public void incTakeoverSuccess() {
        takeoverSuccess.inc();
    }

    @Override
    public void incRenewSuccess() {
        renewSuccess.inc();
    }

    @Override
    public void incRenewFailure() {
        renewFailure.inc();
    }

    @Override
    public void incReleaseSuccess() {
        releaseSuccess.inc();
    }

    @Override
    public void incReleaseFailure() {
        releaseFailure.inc();
    }
}
