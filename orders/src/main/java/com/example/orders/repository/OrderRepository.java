package com.example.orders.repository;

import com.example.orders.domain.Order;
import com.example.orders.dto.request.OrderUpdateRequest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface OrderRepository extends JpaRepository <Order, Integer>{
    Order findByUserIdAndIdempotencyKey(UUID userId, String idempotencyKey);
}
