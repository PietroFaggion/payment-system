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
import java.util.concurrent.ExecutionException;

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

        // Block until the broker acknowledges the send so that any failure is thrown
        // synchronously. This lets the outbox scheduler catch the exception, increment
        // the retry count, and reschedule the row rather than silently dropping it.
        try {
            kafkaTemplate.send(
                    paymentNotificationsTopic,
                    String.valueOf(transaction.getSenderAccount().getId()),
                    message
            ).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka send interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Kafka send failed: " + e.getCause().getMessage(), e);
        }
    }
}
