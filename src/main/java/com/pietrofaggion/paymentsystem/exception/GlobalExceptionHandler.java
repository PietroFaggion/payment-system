package com.pietrofaggion.paymentsystem.exception;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
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

    @ExceptionHandler(CurrencyMismatchException.class)
    public ResponseEntity<ApiError> handleCurrencyMismatch(CurrencyMismatchException ex) {
        return build(HttpStatus.UNPROCESSABLE_ENTITY, "Currency mismatch: request currency does not match sender account currency");
    }

    @ExceptionHandler(AccountNotFoundException.class)
    public ResponseEntity<ApiError> handleAccountNotFoundException(AccountNotFoundException ex) {
        return build(HttpStatus.NOT_FOUND, "Account not found");
    }

    @ExceptionHandler(TransactionNotFoundException.class)
    public ResponseEntity<ApiError> handleTransactionNotFoundException(TransactionNotFoundException ex) {
        return build(HttpStatus.NOT_FOUND, "Transaction not found");
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleMethodArgumentNotValidException(MethodArgumentNotValidException ex) {
        // Field errors take priority; fall back to class-level constraint errors (e.g. self-transfer).
        String message = ex.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(FieldError::getDefaultMessage)
                .or(() -> ex.getBindingResult().getGlobalErrors().stream()
                        .findFirst()
                        .map(ObjectError::getDefaultMessage))
                .orElse("Validation failed");
        return build(HttpStatus.BAD_REQUEST, message);
    }

    @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
    public ResponseEntity<ApiError> handleOptimisticLock(ObjectOptimisticLockingFailureException ex) {
        return build(HttpStatus.CONFLICT, "Concurrent modification detected, please retry");
    }

    @ExceptionHandler({DataIntegrityViolationException.class, org.hibernate.exception.ConstraintViolationException.class})
    public ResponseEntity<ApiError> handleDataIntegrityViolation(Exception ex) {
        // Walk the cause chain looking for a ConstraintViolationException so we can
        // use getConstraintName() instead of fragile message-string matching.
        Throwable cause = ex;
        while (cause != null) {
            if (cause instanceof org.hibernate.exception.ConstraintViolationException cve) {
                if ("transactions_idempotency_key_uk".equals(cve.getConstraintName())) {
                    return build(HttpStatus.CONFLICT, "Duplicated idempotency key");
                }
                return build(HttpStatus.INTERNAL_SERVER_ERROR, "Data integrity violation");
            }
            cause = cause.getCause();
        }
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "Data integrity violation");
    }

    private ResponseEntity<ApiError> build(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(new ApiError(message, OffsetDateTime.now()));
    }
}
