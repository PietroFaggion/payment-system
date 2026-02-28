package com.pietrofaggion.paymentsystem.exception;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.OffsetDateTime;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(InsufficientFundsException.class)
    public ResponseEntity<ApiError> handleInsufficientFundsException(InsufficientFundsException ex) {
        return build(HttpStatus.UNPROCESSABLE_ENTITY, "Insufficient funds");
    }

    @ExceptionHandler(AccountNotFoundException.class)
    public ResponseEntity<ApiError> handleAccountNotFoundException(AccountNotFoundException ex) {
        return build(HttpStatus.NOT_FOUND, "Account not found");
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleMethodArgumentNotValidException(MethodArgumentNotValidException ex) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(FieldError::getDefaultMessage)
                .orElse("Validation failed");
        return build(HttpStatus.BAD_REQUEST, message);
    }

    @ExceptionHandler({DataIntegrityViolationException.class, org.hibernate.exception.ConstraintViolationException.class})
    public ResponseEntity<ApiError> handleDataIntegrityViolation(Exception ex) {
        StringBuilder builder = new StringBuilder();
        Throwable current = ex;
        while (current != null) {
            if (current.getMessage() != null) {
                builder.append(current.getMessage().toLowerCase()).append(" ");
            }
            current = current.getCause();
        }
        String message = builder.toString();
        if (message.contains("idempotency_key")) {
            return build(HttpStatus.CONFLICT, "Duplicated idempotency key");
        }
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "Data integrity violation");
    }

    private ResponseEntity<ApiError> build(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(new ApiError(message, OffsetDateTime.now()));
    }
}
