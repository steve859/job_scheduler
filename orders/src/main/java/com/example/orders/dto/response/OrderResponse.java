package com.example.orders.dto.response;

import com.example.orders.domain.OrderItem;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderResponse {
    private Integer orderId;
    private String status;
    private BigDecimal totalAmount;
    private UUID userId;
    private Instant createdAt;
    private Instant updatedAt;
    private Set<OrderItem> items;
}
