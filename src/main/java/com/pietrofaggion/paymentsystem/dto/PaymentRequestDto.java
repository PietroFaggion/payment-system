package com.pietrofaggion.paymentsystem.dto;

import jakarta.validation.constraints.*;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class PaymentRequestDto {

    @NotNull(message = "senderAccountId is required")
    private Long senderAccountId;

    @NotNull(message = "receiverAccountId is required")
    private Long receiverAccountId;

    @NotNull(message = "amount is required")
    @DecimalMin(value = "0.0001", message = "amount must be greater than 0")
    @Digits(integer = 15, fraction = 4, message = "amount must have at most 15 integer digits and 4 decimal places")
    private BigDecimal amount;

    @NotBlank(message = "currency is required")
    @Pattern(regexp = "^[A-Z]{3}$", message = "currency must be a 3-letter ISO 4217 code")
    private String currency;

    @NotBlank(message = "idempotencyKey is required")
    @Size(max = 255, message = "idempotencyKey must not exceed 255 characters")
    private String idempotencyKey;

    @AssertFalse(message = "Sender and receiver accounts must be different")
    public boolean isSelfTransfer() {
        return senderAccountId != null && senderAccountId.equals(receiverAccountId);
    }
}
