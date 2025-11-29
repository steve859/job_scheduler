package com.example.orders.dto.response;

import com.example.orders.domain.OrderItem;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;

public class CheckoutResponse {
    private Integer orderId;
    private String status;
    private BigDecimal totalAmount;
    private String userId;
    private Instant createdAt;
    private Instant updatedAt;
    private Set<OrderItem> items;
}
