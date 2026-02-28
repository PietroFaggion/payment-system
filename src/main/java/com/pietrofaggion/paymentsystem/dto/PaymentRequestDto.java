package com.pietrofaggion.paymentsystem.dto;

import jakarta.validation.constraints.*;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class PaymentRequestDto {

    @NotNull
    private Long senderAccountId;

    @NotNull
    private Long receiverAccountId;

    @NotNull
    @DecimalMin(value = "0.0001")
    @Digits(integer = 15, fraction = 4)
    private BigDecimal amount;

    @NotBlank
    @Pattern(regexp = "^[A-Z]{3}$")
    private String currency;

    @NotBlank
    private String idempotencyKey;
}
