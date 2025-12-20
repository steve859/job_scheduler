package com.example.orders.exception;

public class OrderNotFoundException extends RuntimeException {
    public OrderNotFoundException(Integer orderId) {
        super("Order with id " + orderId + " not found");
    }
}
