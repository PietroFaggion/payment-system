package com.pietrofaggion.paymentsystem.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pietrofaggion.paymentsystem.entity.Account;
import com.pietrofaggion.paymentsystem.entity.Transaction;
import com.pietrofaggion.paymentsystem.entity.TransactionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationServiceImplTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private NotificationServiceImpl notificationService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        notificationService = new NotificationServiceImpl(kafkaTemplate, objectMapper);
        ReflectionTestUtils.setField(notificationService, "paymentNotificationsTopic", "payment-notifications");
    }

    @Test
    void sendShouldPublishJsonNotificationWithCorrectTopicKeyAndPayload() throws Exception {
        Transaction transaction = buildTransaction();
        when(kafkaTemplate.send(eq("payment-notifications"), eq("10"), any(String.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        notificationService.send(transaction);

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("payment-notifications"), eq("10"), payloadCaptor.capture());

        JsonNode message = objectMapper.readTree(payloadCaptor.getValue());
        assertThat(message.get("transactionId").asText()).isEqualTo("77");
        assertThat(message.get("senderAccountId").asText()).isEqualTo("10");
        assertThat(message.get("amount").asText()).isEqualTo("42.5000");
        assertThat(message.get("currency").asText()).isEqualTo("EUR");
        assertThat(message.get("status").asText()).isEqualTo("COMPLETED");
        assertThat(message.get("timestamp").asText()).isEqualTo("2026-02-23T12:00Z");
    }

    private Transaction buildTransaction() {
        Account sender = new Account();
        sender.setId(10L);

        Account receiver = new Account();
        receiver.setId(20L);

        Transaction transaction = new Transaction();
        transaction.setId(77L);
        transaction.setSenderAccount(sender);
        transaction.setReceiverAccount(receiver);
        transaction.setAmount(new BigDecimal("42.5000"));
        transaction.setCurrency("EUR");
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setUpdatedAt(OffsetDateTime.parse("2026-02-23T12:00:00Z"));
        return transaction;
    }
}
