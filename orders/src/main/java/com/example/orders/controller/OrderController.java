package com.example.orders.controller;

import com.example.orders.domain.Order;
import com.example.orders.dto.request.OrderCreateRequest;
import com.example.orders.dto.request.OrderUpdateRequest;
import com.example.orders.dto.response.ApiResponse;
import com.example.orders.dto.response.OrderResponse;
import com.example.orders.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    ApiResponse<OrderResponse> createOrder(@Valid @RequestBody OrderCreateRequest request) {
        return ApiResponse.success("Order has been created",orderService.createOrder(request));
    }

    @GetMapping
    ApiResponse<List<OrderResponse>> getOrders() {
        return ApiResponse.success(orderService.getOrders());
    }

    @GetMapping("/{orderId}")
    ApiResponse<OrderResponse> getOrder(@PathVariable Integer orderId) {
        return ApiResponse.success(orderService.getOrder(orderId));
    }

    @PutMapping("/{orderId}")
    ApiResponse<OrderResponse> updateOrder(@PathVariable Integer orderId, @Valid @RequestBody OrderUpdateRequest request) {
        return ApiResponse.success("Order has been updated", orderService.updateOrder(orderId,request));
    }

    @DeleteMapping("/{orderId}")
    ApiResponse<String> deleteOrder(@PathVariable Integer orderId) {
        orderService.deleteOrder(orderId);
        return ApiResponse.success("Order has been deleted");
    }



}
