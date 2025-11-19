package com.example.lock;

import java.time.Instant;

public class LockResult {
    private final boolean acquired;
    private final long token;
    private final Instant expireAt;

    public LockResult(boolean acquired, long token, Instant expireAt) {
        this.acquired = acquired;
        this.token = token;
        this.expireAt = expireAt;
    }

    public boolean isAcquired() {
        return acquired;
    }

    public long getToken() {
        return token;
    }

    public Instant getExpireAt() {
        return expireAt;
    }

    @Override
    public String toString() {
        return "LockResult{acquired=" + acquired + ", token=" + token + ", expireAt=" + expireAt + "}";
    }
}
