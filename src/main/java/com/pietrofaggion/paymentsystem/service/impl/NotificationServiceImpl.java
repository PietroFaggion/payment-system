package com.pietrofaggion.paymentsystem.service.impl;

import com.pietrofaggion.paymentsystem.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
@RequiredArgsConstructor
public class NotificationServiceImpl implements NotificationService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topics.payment-notifications:payment-notifications}")
    private String paymentNotificationsTopic;

    @Override
    public void send(String payload, String kafkaKey) {
        // Block until the broker acknowledges the send so that any failure is thrown
        // synchronously and the OutboxRowProcessor can update the retry count.
        try {
            kafkaTemplate.send(paymentNotificationsTopic, kafkaKey, payload).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka send interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Kafka send failed: " + e.getCause().getMessage(), e);
        }
    }
}
