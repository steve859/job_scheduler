package com.example.orders.mapper;

import com.example.orders.domain.Order;
import com.example.orders.dto.request.OrderCreateRequest;
import com.example.orders.dto.request.OrderUpdateRequest;
import com.example.orders.dto.response.OrderResponse;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring", uses = OrderMapper.class)
public interface OrderMapper {
    Order toOrder(OrderCreateRequest request);
    OrderResponse toOrderResponse(Order order);
}
