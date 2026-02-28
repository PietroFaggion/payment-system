package com.pietrofaggion.paymentsystem.dto;

import com.pietrofaggion.paymentsystem.entity.TransactionStatus;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class PaymentResponseDto {

    private Long transactionId;
    private Long senderAccountId;
    private Long receiverAccountId;
    private BigDecimal amount;
    private String currency;
    private String idempotencyKey;
    private TransactionStatus status;
}
