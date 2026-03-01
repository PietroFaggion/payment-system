package com.pietrofaggion.paymentsystem.service.impl;

import com.pietrofaggion.paymentsystem.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

/**
 * Publishes payment notification messages to Kafka synchronously.
 * <p>
 * The send blocks until the broker acknowledges the produce request so that any broker
 * failure is propagated as an exception immediately, allowing
 * {@link com.pietrofaggion.paymentsystem.scheduler.OutboxRowProcessor} to increment the
 * retry count and schedule a later delivery attempt.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationServiceImpl implements NotificationService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topics.payment-notifications:payment-notifications}")
    private String paymentNotificationsTopic;

    /**
     * Publishes {@code payload} to the payment-notifications Kafka topic keyed by {@code kafkaKey}.
     * <p>
     * The call blocks until the broker acknowledges the produce request ({@code acks=all}) so
     * that failures surface synchronously and the outbox processor can react accordingly.
     *
     * @param payload   JSON-encoded notification message to publish
     * @param kafkaKey  Kafka partition key (sender account ID as a string)
     * @throws IllegalStateException if the broker rejects the message or the calling thread is interrupted
     */
    @Override
    public void send(String payload, String kafkaKey) {
        log.debug("Sending Kafka message - topic={} key={}", paymentNotificationsTopic, kafkaKey);
        try {
            kafkaTemplate.send(paymentNotificationsTopic, kafkaKey, payload).get();
            log.debug("Kafka message acknowledged - topic={} key={}", paymentNotificationsTopic, kafkaKey);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka send interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Kafka send failed: " + e.getCause().getMessage(), e);
        }
    }
}
