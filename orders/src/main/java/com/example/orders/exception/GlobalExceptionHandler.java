package com.example.orders.exception;

import com.example.orders.dto.response.ApiResponse;
import com.example.orders.dto.response.OrderResponse;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler extends RuntimeException {
    @ExceptionHandler(OrderNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ApiResponse<?> handleOrderNotFoundException(OrderNotFoundException ex) {
        return ApiResponse.error(ex.getMessage());
    }

    @ExceptionHandler(IdempotencyKeyConflictException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ApiResponse<?> handleIdempotencyKeyConflictException(IdempotencyKeyConflictException ex) {
        return ApiResponse.error(ex.getMessage());
    }

    @ExceptionHandler(OrderAlreadyExistsException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ApiResponse<?> handleOrderAlreadyExistsException(OrderAlreadyExistsException ex) {
        return ApiResponse.error(ex.getMessage());
    }
}
