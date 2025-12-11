package com.example.orders.service;

import com.example.orders.dto.request.OrderCreateRequest;
import com.example.orders.dto.request.OrderUpdateRequest;
import com.example.orders.dto.response.OrderResponse;

import java.util.List;

public interface OrderService {
    OrderResponse createOrder(OrderCreateRequest request);
    OrderResponse getOrder(Integer orderId);
    List<OrderResponse> getOrders();
    OrderResponse updateOrder(Integer orderId, OrderUpdateRequest request);
    void deleteOrder(Integer orderId);
}
