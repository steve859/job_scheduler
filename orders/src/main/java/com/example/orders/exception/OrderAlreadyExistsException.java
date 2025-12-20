package com.example.orders.exception;

public class OrderAlreadyExistsException extends RuntimeException {
    public OrderAlreadyExistsException(Integer orderId) {
        super("Order with id " + orderId + " already exists!");
    }
}
