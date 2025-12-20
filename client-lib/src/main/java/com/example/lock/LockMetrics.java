package com.example.lock;

/**
 * Minimal metrics hook for LockClient. Default is no-op.
 */
public interface LockMetrics {
    void observeLwtLatencySeconds(String op, double seconds);

    void incAcquireAttempt();

    void incAcquireSuccess();

    void incTakeoverSuccess();

    void incRenewSuccess();

    void incRenewFailure();

    void incReleaseSuccess();

    void incReleaseFailure();

    static LockMetrics noop() {
        return new LockMetrics() {
            public void observeLwtLatencySeconds(String op, double seconds) {
            }

            public void incAcquireAttempt() {
            }

            public void incAcquireSuccess() {
            }

            public void incTakeoverSuccess() {
            }

            public void incRenewSuccess() {
            }

            public void incRenewFailure() {
            }

            public void incReleaseSuccess() {
            }

            public void incReleaseFailure() {
            }
        };
    }
}
