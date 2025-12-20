package com.example.orders.dto.request;

import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.List;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderCreateRequest {
    @NotNull
    private UUID userId;
    @NotNull
    private String idempotencyKey;
    @NotNull
    private List<OrderItemRequest> items;

}
