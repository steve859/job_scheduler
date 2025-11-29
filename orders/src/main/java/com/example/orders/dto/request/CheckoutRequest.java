package com.example.orders.dto.request;

import java.math.BigDecimal;
import java.util.List;

public class CheckoutRequest {
    private String userId;
    private String idempotencyKey;
    private List<Item> items;

    public static class Item {
        private String sku;
        private int quantity;
        private BigDecimal price;
    }
}
