package com.example.orders.exception;

public class IdempotencyKeyConflictException extends RuntimeException {
    public IdempotencyKeyConflictException(String key) {
        super("Operation already processed for idempotency key " + key);
    }
}
