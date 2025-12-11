package com.example.orders.service;

import com.example.orders.domain.Order;
import com.example.orders.dto.request.OrderCreateRequest;
import com.example.orders.dto.request.OrderUpdateRequest;
import com.example.orders.dto.response.OrderResponse;
import com.example.orders.exception.IdempotencyKeyConflictException;
import com.example.orders.exception.OrderNotFoundException;
import com.example.orders.mapper.OrderMapper;
import com.example.orders.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;

    @Override
    @Transactional
    public OrderResponse createOrder(OrderCreateRequest request) {
        if(orderRepository.findByUserIdAndIdempotencyKey(request.getUserId(), request.getIdempotencyKey()) != null){
            throw new IdempotencyKeyConflictException(request.getIdempotencyKey());
        }
        Order newOrder = orderMapper.toOrder(request);
        return orderMapper.toOrderResponse(orderRepository.save(newOrder));
    }

    @Override
    public OrderResponse getOrder(Integer orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));

        return orderMapper.toOrderResponse(order);
    }

    @Override
    public List<OrderResponse> getOrders() {
        return orderRepository.findAll().stream().map(orderMapper::toOrderResponse).toList();
    }

    @Override
    @Transactional
    public OrderResponse updateOrder(Integer orderId, OrderUpdateRequest request) {

        Order order = orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));

        // Update allowed fields only
//        order.setStatus(request.getStatus());
//        order.setUpdatedAt(java.time.Instant.now());

        // Nếu request có items thì phải update riêng,
        // KHÔNG được overwrite toàn bộ list

        Order updatedOrder = orderRepository.save(order);
        return orderMapper.toOrderResponse(updatedOrder);
    }

    @Override
    @Transactional
    public void deleteOrder(Integer orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
        orderRepository.delete(order);
    }
}
