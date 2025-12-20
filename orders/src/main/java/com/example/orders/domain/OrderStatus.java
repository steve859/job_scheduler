package com.example.orders.domain;

public enum OrderStatus {

    PENDING,        // Đơn mới tạo, chưa thanh toán
    PAID,           // Đã thanh toán thành công
    PREPARING,      // Đang chuẩn bị hàng
    SHIPPING,       // Đang vận chuyển
    COMPLETED,      // Giao thành công
    CANCELLED;      // Đơn bị hủy

    public boolean canTransitionTo(OrderStatus next) {
        return switch (this) {
            case PENDING -> next == PAID || next == CANCELLED;
            case PAID -> next == PREPARING || next == CANCELLED;
            case PREPARING -> next == SHIPPING || next == CANCELLED;
            case SHIPPING -> next == COMPLETED || next == CANCELLED;
            case COMPLETED, CANCELLED -> false;
        };
    }
}
