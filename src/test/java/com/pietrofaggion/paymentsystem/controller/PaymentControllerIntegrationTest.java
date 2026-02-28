package com.pietrofaggion.paymentsystem.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pietrofaggion.paymentsystem.dto.PaymentRequestDto;
import com.pietrofaggion.paymentsystem.entity.*;
import com.pietrofaggion.paymentsystem.repository.AccountRepository;
import com.pietrofaggion.paymentsystem.repository.NotificationOutboxRepository;
import com.pietrofaggion.paymentsystem.repository.TransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:postgresql://localhost:5433/payments_db",
        "spring.datasource.username=payments_user",
        "spring.datasource.password=payments_pass",
        "spring.kafka.bootstrap-servers=localhost:9092"
})
class PaymentControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private NotificationOutboxRepository notificationOutboxRepository;

    @BeforeEach
    void setUp() {
        notificationOutboxRepository.deleteAll();
        transactionRepository.deleteAll();
        accountRepository.deleteAll();
    }

    @Test
    void createPaymentShouldPersistTransactionAndOutbox() throws Exception {
        Account sender = accountRepository.save(buildAccount("Sender", "100.0000"));
        Account receiver = accountRepository.save(buildAccount("Receiver", "10.0000"));

        PaymentRequestDto request = new PaymentRequestDto();
        request.setSenderAccountId(sender.getId());
        request.setReceiverAccountId(receiver.getId());
        request.setAmount(new BigDecimal("25.0000"));
        request.setCurrency("EUR");
        request.setIdempotencyKey("idem-int-1");

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.idempotencyKey").value("idem-int-1"))
                .andExpect(jsonPath("$.status").value(TransactionStatus.COMPLETED.name()))
                .andExpect(jsonPath("$.senderAccountId").value(sender.getId()))
                .andExpect(jsonPath("$.receiverAccountId").value(receiver.getId()));

        Optional<Transaction> transactionOpt = transactionRepository.findByIdempotencyKey("idem-int-1");
        assertThat(transactionOpt).isPresent();
        assertThat(transactionOpt.get().getStatus()).isEqualTo(TransactionStatus.COMPLETED);

        assertThat(notificationOutboxRepository.findByStatus(OutboxStatus.PENDING)).hasSize(1);
        NotificationOutbox outbox = notificationOutboxRepository.findByStatus(OutboxStatus.PENDING).get(0);
        assertThat(outbox.getPayload()).contains("\"idempotencyKey\":\"idem-int-1\"");
    }

    private Account buildAccount(String ownerName, String balance) {
        Account account = new Account();
        account.setOwnerName(ownerName);
        account.setBalance(new BigDecimal(balance));
        account.setCurrency("EUR");
        account.setCreatedAt(OffsetDateTime.now());
        return account;
    }
}
