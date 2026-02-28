package com.pietrofaggion.paymentsystem.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pietrofaggion.paymentsystem.entity.Transaction;
import com.pietrofaggion.paymentsystem.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class NotificationServiceImpl implements NotificationService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.payment-notifications:payment-notifications}")
    private String paymentNotificationsTopic;

    @Override
    public void send(Transaction transaction) {
        OffsetDateTime timestamp = transaction.getUpdatedAt() != null ? transaction.getUpdatedAt() : OffsetDateTime.now();

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("transactionId", String.valueOf(transaction.getId()));
        payload.put("senderAccountId", String.valueOf(transaction.getSenderAccount().getId()));
        payload.put("amount", transaction.getAmount().toPlainString());
        payload.put("currency", transaction.getCurrency());
        payload.put("status", transaction.getStatus().name());
        payload.put("timestamp", timestamp.toString());

        String message;
        try {
            message = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize payment notification", e);
        }

        kafkaTemplate.send(
                paymentNotificationsTopic,
                String.valueOf(transaction.getSenderAccount().getId()),
                message
        );
    }
}
