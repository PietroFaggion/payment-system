package com.pietrofaggion.paymentsystem.service.impl;

import com.pietrofaggion.paymentsystem.service.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationServiceImplTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private NotificationService notificationService;

    @BeforeEach
    void setUp() {
        notificationService = new NotificationServiceImpl(kafkaTemplate);
        ReflectionTestUtils.setField(notificationService, "paymentNotificationsTopic", "payment-notifications");
    }

    @Test
    void sendShouldPublishPayloadWithCorrectTopicAndKey() {
        String payload = "{\"transactionId\":\"77\",\"senderAccountId\":\"10\"}";
        String key = "10";
        when(kafkaTemplate.send(eq("payment-notifications"), eq(key), eq(payload)))
                .thenReturn(CompletableFuture.completedFuture(null));

        notificationService.send(payload, key);

        verify(kafkaTemplate).send("payment-notifications", key, payload);
    }

    @Test
    void sendShouldPropagateExceptionWhenKafkaFails() {
        String payload = "{\"transactionId\":\"77\"}";
        String key = "10";
        CompletableFuture<Object> failedFuture = CompletableFuture.failedFuture(
                new RuntimeException("broker unavailable"));
        when(kafkaTemplate.send(eq("payment-notifications"), eq(key), eq(payload)))
                .thenReturn((CompletableFuture) failedFuture);

        assertThatThrownBy(() -> notificationService.send(payload, key))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Kafka send failed");
    }
}
