package com.pietrofaggion.paymentsystem.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pietrofaggion.paymentsystem.dto.PaymentRequestDto;
import com.pietrofaggion.paymentsystem.entity.Account;
import com.pietrofaggion.paymentsystem.entity.NotificationOutbox;
import com.pietrofaggion.paymentsystem.entity.OutboxStatus;
import com.pietrofaggion.paymentsystem.repository.AccountRepository;
import com.pietrofaggion.paymentsystem.repository.NotificationOutboxRepository;
import com.pietrofaggion.paymentsystem.repository.TransactionRepository;
import com.pietrofaggion.paymentsystem.service.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 1,
        topics = {"payment-notifications"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:0", "port=0"}
)
@DirtiesContext
class OutboxRetryIntegrationTest {

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

    @Autowired
    private OutboxScheduler outboxScheduler;

    // Replace the real NotificationService so Kafka send behaviour is fully controlled.
    @MockitoBean
    private NotificationService notificationService;

    @BeforeEach
    void setUp() {
        notificationOutboxRepository.deleteAll();
        transactionRepository.deleteAll();
        accountRepository.deleteAll();
    }

    // ------------------------------------------------------------------
    // 1) Kafka fails on first attempt, succeeds on second — the outbox
    //    retries and eventually marks the row SENT.
    // ------------------------------------------------------------------
    @Test
    void outboxShouldRetryOnKafkaFailureAndMarkSentOnSubsequentSuccess() throws Exception {
        Account sender = accountRepository.save(buildAccount("Sender", "200.0000", "EUR"));
        Account receiver = accountRepository.save(buildAccount("Receiver", "50.0000", "EUR"));

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                buildRequest(sender.getId(), receiver.getId(), "50.0000", "EUR", "idem-retry-1"))))
                .andExpect(status().isCreated());

        // First scheduler run: Kafka throws → row reset to PENDING with retryCount=1
        doThrow(new RuntimeException("Kafka broker unavailable"))
                .doNothing()
                .when(notificationService).send(any(), any());

        outboxScheduler.processOutbox();

        NotificationOutbox outbox = notificationOutboxRepository.findAll().get(0);
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.getRetryCount()).isEqualTo(1);
        assertThat(outbox.getSentAt()).isNull();

        // Second scheduler run: Kafka succeeds (second invocation of mock) → row marked SENT
        outboxScheduler.processOutbox();

        outbox = notificationOutboxRepository.findAll().get(0);
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(outbox.getRetryCount()).isEqualTo(1);
        assertThat(outbox.getSentAt()).isNotNull();
    }

    // ------------------------------------------------------------------
    // 2) Kafka always fails — after 3 attempts the row is marked FAILED.
    // ------------------------------------------------------------------
    @Test
    void outboxShouldMarkAsFailedAfterThreeConsecutiveKafkaFailures() throws Exception {
        Account sender = accountRepository.save(buildAccount("Sender", "200.0000", "EUR"));
        Account receiver = accountRepository.save(buildAccount("Receiver", "50.0000", "EUR"));

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                buildRequest(sender.getId(), receiver.getId(), "50.0000", "EUR", "idem-fail-1"))))
                .andExpect(status().isCreated());

        doThrow(new RuntimeException("Kafka broker unavailable"))
                .when(notificationService).send(any(), any());

        // Attempt 1: retryCount=1, back to PENDING
        outboxScheduler.processOutbox();
        NotificationOutbox outbox = notificationOutboxRepository.findAll().get(0);
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.getRetryCount()).isEqualTo(1);

        // Attempt 2: retryCount=2, still PENDING
        outboxScheduler.processOutbox();
        outbox = notificationOutboxRepository.findAll().get(0);
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.getRetryCount()).isEqualTo(2);

        // Attempt 3: retryCount=3, FAILED — no more retries
        outboxScheduler.processOutbox();
        outbox = notificationOutboxRepository.findAll().get(0);
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(outbox.getRetryCount()).isEqualTo(3);
        assertThat(outbox.getSentAt()).isNull();

        // A fourth run must not touch the FAILED row (scheduler only claims PENDING)
        outboxScheduler.processOutbox();
        outbox = notificationOutboxRepository.findAll().get(0);
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(outbox.getRetryCount()).isEqualTo(3);
    }

    private PaymentRequestDto buildRequest(Long senderId, Long receiverId,
                                           String amount, String currency, String idempotencyKey) {
        PaymentRequestDto request = new PaymentRequestDto();
        request.setSenderAccountId(senderId);
        request.setReceiverAccountId(receiverId);
        request.setAmount(new BigDecimal(amount));
        request.setCurrency(currency);
        request.setIdempotencyKey(idempotencyKey);
        return request;
    }

    private Account buildAccount(String ownerName, String balance, String currency) {
        Account account = new Account();
        account.setOwnerName(ownerName);
        account.setBalance(new BigDecimal(balance));
        account.setCurrency(currency);
        account.setCreatedAt(OffsetDateTime.now());
        return account;
    }
}
