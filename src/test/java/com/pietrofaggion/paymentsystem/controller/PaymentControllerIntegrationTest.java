package com.pietrofaggion.paymentsystem.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pietrofaggion.paymentsystem.dto.PaymentRequestDto;
import com.pietrofaggion.paymentsystem.entity.*;
import com.pietrofaggion.paymentsystem.repository.AccountRepository;
import com.pietrofaggion.paymentsystem.repository.NotificationOutboxRepository;
import com.pietrofaggion.paymentsystem.repository.TransactionRepository;
import com.pietrofaggion.paymentsystem.scheduler.OutboxScheduler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 1,
        topics = {"payment-notifications"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:0", "port=0"}
)
@DirtiesContext
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

    @Autowired
    private OutboxScheduler outboxScheduler;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @BeforeEach
    void setUp() {
        notificationOutboxRepository.deleteAll();
        transactionRepository.deleteAll();
        accountRepository.deleteAll();
    }

    @Test
    void createPaymentShouldPersistTransactionAndOutbox() throws Exception {
        Account sender = accountRepository.save(buildAccount("Sender", "100.0000", "EUR"));
        Account receiver = accountRepository.save(buildAccount("Receiver", "10.0000", "EUR"));

        PaymentRequestDto request = buildRequest(sender.getId(), receiver.getId(), "25.0000", "EUR", "idem-int-1");

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
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

    @Test
    void getPaymentShouldReturnTransactionWhenFound() throws Exception {
        Account sender = accountRepository.save(buildAccount("Sender", "100.0000", "EUR"));
        Account receiver = accountRepository.save(buildAccount("Receiver", "10.0000", "EUR"));

        PaymentRequestDto request = buildRequest(sender.getId(), receiver.getId(), "10.0000", "EUR", "idem-get-1");

        String responseBody = mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        Long transactionId = objectMapper.readTree(responseBody).get("transactionId").asLong();

        mockMvc.perform(get("/payments/" + transactionId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transactionId").value(transactionId))
                .andExpect(jsonPath("$.status").value(TransactionStatus.COMPLETED.name()));
    }

    @Test
    void getPaymentShouldReturn404WhenNotFound() throws Exception {
        mockMvc.perform(get("/payments/99999"))
                .andExpect(status().isNotFound());
    }

    @Test
    void createPaymentShouldReturn422WhenCurrencyMismatch() throws Exception {
        Account sender = accountRepository.save(buildAccount("Sender", "100.0000", "USD"));
        Account receiver = accountRepository.save(buildAccount("Receiver", "10.0000", "EUR"));

        PaymentRequestDto request = buildRequest(sender.getId(), receiver.getId(), "10.0000", "EUR", "idem-currency-1");

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.message").value("Currency mismatch: request currency does not match sender account currency"));
    }

    @Test
    void createPaymentShouldReturn400WhenSelfTransfer() throws Exception {
        Account account = accountRepository.save(buildAccount("Owner", "100.0000", "EUR"));

        PaymentRequestDto request = buildRequest(account.getId(), account.getId(), "10.0000", "EUR", "idem-self-1");

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Sender and receiver accounts must be different"));
    }

    // ------------------------------------------------------------------
    // 1) Kafka message verification: payment → outbox → scheduler → Kafka topic
    // ------------------------------------------------------------------
    @Test
    void outboxSchedulerShouldPublishKafkaMessageAfterPayment() throws Exception {
        Account sender = accountRepository.save(buildAccount("Sender", "500.0000", "EUR"));
        Account receiver = accountRepository.save(buildAccount("Receiver", "50.0000", "EUR"));

        PaymentRequestDto request = buildRequest(sender.getId(), receiver.getId(), "100.0000", "EUR", "idem-kafka-1");

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("COMPLETED"));

        // Outbox row should be PENDING before the scheduler runs
        List<NotificationOutbox> pendingBefore = notificationOutboxRepository.findByStatus(OutboxStatus.PENDING);
        assertThat(pendingBefore).hasSize(1);

        // Create a Kafka consumer subscribed to the payment-notifications topic
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group-kafka-1", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
            consumer.subscribe(Collections.singletonList("payment-notifications"));

            // Trigger the outbox scheduler to process pending rows and send to Kafka
            outboxScheduler.processOutbox();

            // Outbox row should now be SENT
            List<NotificationOutbox> sent = notificationOutboxRepository.findByStatus(OutboxStatus.SENT);
            assertThat(sent).hasSize(1);
            assertThat(notificationOutboxRepository.findByStatus(OutboxStatus.PENDING)).isEmpty();

            // Verify the Kafka message content
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
            assertThat(records.count()).isGreaterThanOrEqualTo(1);

            String value = records.iterator().next().value();
            JsonNode message = objectMapper.readTree(value);
            assertThat(message.get("senderAccountId").asText()).isEqualTo(String.valueOf(sender.getId()));
            assertThat(message.get("amount").asText()).isEqualTo("100.0000");
            assertThat(message.get("currency").asText()).isEqualTo("EUR");
            assertThat(message.get("status").asText()).isEqualTo("COMPLETED");
        }
    }

    // ------------------------------------------------------------------
    // 2) DB locks: two opposite-direction payments complete without deadlock
    // ------------------------------------------------------------------
    @Test
    void concurrentOppositeDirectionPaymentsShouldNotDeadlock() throws Exception {
        Account alice = accountRepository.save(buildAccount("Alice", "1000.0000", "EUR"));
        Account bob = accountRepository.save(buildAccount("Bob", "1000.0000", "EUR"));

        PaymentRequestDto aliceToBob = buildRequest(alice.getId(), bob.getId(), "100.0000", "EUR", "idem-lock-a2b");
        PaymentRequestDto bobToAlice = buildRequest(bob.getId(), alice.getId(), "50.0000", "EUR", "idem-lock-b2a");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);

        Callable<MvcResult> task1 = () -> {
            barrier.await(5, TimeUnit.SECONDS);
            return mockMvc.perform(post("/payments")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(aliceToBob)))
                    .andReturn();
        };

        Callable<MvcResult> task2 = () -> {
            barrier.await(5, TimeUnit.SECONDS);
            return mockMvc.perform(post("/payments")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(bobToAlice)))
                    .andReturn();
        };

        Future<MvcResult> future1 = executor.submit(task1);
        Future<MvcResult> future2 = executor.submit(task2);

        MvcResult result1 = future1.get(10, TimeUnit.SECONDS);
        MvcResult result2 = future2.get(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Both should succeed (201) or one may get a conflict (409) due to optimistic locking — not deadlock
        int status1 = result1.getResponse().getStatus();
        int status2 = result2.getResponse().getStatus();
        assertThat(status1).isIn(201, 409);
        assertThat(status2).isIn(201, 409);
        // At least one must succeed
        assertThat(status1 == 201 || status2 == 201).isTrue();

        // Verify balance consistency: total money is conserved (2000 EUR total)
        Account refreshedAlice = accountRepository.findById(alice.getId()).orElseThrow();
        Account refreshedBob = accountRepository.findById(bob.getId()).orElseThrow();
        BigDecimal totalAfter = refreshedAlice.getBalance().add(refreshedBob.getBalance());

        if (status1 == 201 && status2 == 201) {
            // Both succeeded: Alice sent 100, received 50 → net -50; Bob sent 50, received 100 → net +50
            assertThat(totalAfter).isEqualByComparingTo("2000.0000");
            assertThat(refreshedAlice.getBalance()).isEqualByComparingTo("950.0000");
            assertThat(refreshedBob.getBalance()).isEqualByComparingTo("1050.0000");
        } else {
            // Only one succeeded: total stays the same
            assertThat(totalAfter).isEqualByComparingTo("2000.0000");
        }
    }

    // ------------------------------------------------------------------
    // 3) Idempotency: same key returns the existing transaction, not an error
    //    and a race condition on the same key gets a 409 CONFLICT
    // ------------------------------------------------------------------
    @Test
    void duplicateIdempotencyKeyShouldReturnExistingTransaction() throws Exception {
        Account sender = accountRepository.save(buildAccount("Sender", "200.0000", "EUR"));
        Account receiver = accountRepository.save(buildAccount("Receiver", "50.0000", "EUR"));

        PaymentRequestDto request = buildRequest(sender.getId(), receiver.getId(), "30.0000", "EUR", "idem-dup-1");

        // First call — should create the transaction
        String firstResponse = mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andReturn().getResponse().getContentAsString();

        Long firstTxId = objectMapper.readTree(firstResponse).get("transactionId").asLong();

        // Second call with the same idempotency key — should return the same transaction, NOT create a new one
        String secondResponse = mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        Long secondTxId = objectMapper.readTree(secondResponse).get("transactionId").asLong();

        assertThat(secondTxId).isEqualTo(firstTxId);

        // Balance should only reflect ONE deduction (30 EUR), not two
        Account refreshedSender = accountRepository.findById(sender.getId()).orElseThrow();
        assertThat(refreshedSender.getBalance()).isEqualByComparingTo("170.0000");

        // Only one transaction and one outbox entry should exist
        assertThat(transactionRepository.findAll()).hasSize(1);
        assertThat(notificationOutboxRepository.findAll()).hasSize(1);
    }

    @Test
    void concurrentRequestsWithSameIdempotencyKeyShouldNotDuplicateTransaction() throws Exception {
        Account sender = accountRepository.save(buildAccount("Sender", "500.0000", "EUR"));
        Account receiver = accountRepository.save(buildAccount("Receiver", "100.0000", "EUR"));

        PaymentRequestDto request = buildRequest(sender.getId(), receiver.getId(), "50.0000", "EUR", "idem-race-1");

        int threadCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);

        List<Future<MvcResult>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                return mockMvc.perform(post("/payments")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                        .andReturn();
            }));
        }

        List<Integer> statusCodes = new ArrayList<>();
        Set<Long> transactionIds = new HashSet<>();
        for (Future<MvcResult> future : futures) {
            MvcResult result = future.get(10, TimeUnit.SECONDS);
            int httpStatus = result.getResponse().getStatus();
            statusCodes.add(httpStatus);
            if (httpStatus == 201) {
                Long txId = objectMapper.readTree(result.getResponse().getContentAsString())
                        .get("transactionId").asLong();
                transactionIds.add(txId);
            }
        }
        executor.shutdown();

        // All successful responses must reference the same transaction ID
        assertThat(transactionIds).hasSize(1);

        // At least one must be 201; others can be 201 (idempotent return) or 409 (constraint race)
        assertThat(statusCodes).contains(201);
        statusCodes.forEach(code -> assertThat(code).isIn(201, 409));

        // Only one transaction should exist in the DB
        assertThat(transactionRepository.findAll()).hasSize(1);

        // Balance should reflect exactly one deduction
        Account refreshedSender = accountRepository.findById(sender.getId()).orElseThrow();
        assertThat(refreshedSender.getBalance()).isEqualByComparingTo("450.0000");
    }

    // ------------------------------------------------------------------
    // 4) Concurrency stress: many payments at once, verify balance coherence
    // ------------------------------------------------------------------
    @Test
    void massiveConcurrentPaymentsShouldMaintainBalanceCoherence() throws Exception {
        // Setup: one sender with a large balance, many receivers
        BigDecimal paymentAmount = new BigDecimal("10.0000");
        int numberOfPayments = 20;
        BigDecimal senderInitialBalance = paymentAmount.multiply(BigDecimal.valueOf(numberOfPayments));

        Account sender = accountRepository.save(buildAccount("Sender", senderInitialBalance.toPlainString(), "EUR"));

        List<Account> receivers = new ArrayList<>();
        for (int i = 0; i < numberOfPayments; i++) {
            receivers.add(accountRepository.save(buildAccount("Receiver-" + i, "0.0000", "EUR")));
        }

        BigDecimal totalMoneyBefore = senderInitialBalance; // receivers all start at 0

        ExecutorService executor = Executors.newFixedThreadPool(10);
        CyclicBarrier barrier = new CyclicBarrier(numberOfPayments);

        List<Future<MvcResult>> futures = new ArrayList<>();
        for (int i = 0; i < numberOfPayments; i++) {
            final int index = i;
            futures.add(executor.submit(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                PaymentRequestDto req = buildRequest(
                        sender.getId(),
                        receivers.get(index).getId(),
                        paymentAmount.toPlainString(),
                        "EUR",
                        "idem-stress-" + index
                );
                return mockMvc.perform(post("/payments")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(req)))
                        .andReturn();
            }));
        }

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger conflictCount = new AtomicInteger(0);
        AtomicInteger insufficientFundsCount = new AtomicInteger(0);

        for (Future<MvcResult> future : futures) {
            MvcResult result = future.get(30, TimeUnit.SECONDS);
            int httpStatus = result.getResponse().getStatus();
            if (httpStatus == 201) {
                successCount.incrementAndGet();
            } else if (httpStatus == 409) {
                conflictCount.incrementAndGet();
            } else if (httpStatus == 422) {
                insufficientFundsCount.incrementAndGet();
            }
        }
        executor.shutdown();

        // Verify all payments were processed (success, conflict to retry, or insufficient funds)
        assertThat(successCount.get() + conflictCount.get() + insufficientFundsCount.get())
                .isEqualTo(numberOfPayments);

        // Reload sender balance
        Account refreshedSender = accountRepository.findById(sender.getId()).orElseThrow();

        // Count successful transactions
        long completedTransactions = transactionRepository.findAll().stream()
                .filter(t -> t.getStatus() == TransactionStatus.COMPLETED)
                .count();

        assertThat(completedTransactions).isEqualTo(successCount.get());

        // Sender balance should equal initial - (successCount * paymentAmount)
        BigDecimal expectedSenderBalance = senderInitialBalance
                .subtract(paymentAmount.multiply(BigDecimal.valueOf(successCount.get())));
        assertThat(refreshedSender.getBalance()).isEqualByComparingTo(expectedSenderBalance);

        // Sender should never go negative
        assertThat(refreshedSender.getBalance()).isGreaterThanOrEqualTo(BigDecimal.ZERO);

        // Total money in the system must be conserved
        BigDecimal totalMoneyAfter = refreshedSender.getBalance();
        for (Account receiver : receivers) {
            Account refreshedReceiver = accountRepository.findById(receiver.getId()).orElseThrow();
            totalMoneyAfter = totalMoneyAfter.add(refreshedReceiver.getBalance());
        }
        assertThat(totalMoneyAfter).isEqualByComparingTo(totalMoneyBefore);

        // Each receiver got either the payment amount or nothing
        for (Account receiver : receivers) {
            Account refreshed = accountRepository.findById(receiver.getId()).orElseThrow();
            assertThat(refreshed.getBalance()).isIn(BigDecimal.ZERO, paymentAmount);
        }
    }

    @Test
    void concurrentPaymentsSameAccountPairShouldMaintainConsistency() throws Exception {
        // Both Alice and Bob send payments to each other concurrently
        Account alice = accountRepository.save(buildAccount("Alice", "500.0000", "EUR"));
        Account bob = accountRepository.save(buildAccount("Bob", "500.0000", "EUR"));

        int paymentsPerDirection = 5;
        BigDecimal amount = new BigDecimal("20.0000");
        int totalPayments = paymentsPerDirection * 2;

        ExecutorService executor = Executors.newFixedThreadPool(totalPayments);
        CyclicBarrier barrier = new CyclicBarrier(totalPayments);

        List<Future<MvcResult>> futures = new ArrayList<>();

        // Alice → Bob payments
        for (int i = 0; i < paymentsPerDirection; i++) {
            final int index = i;
            futures.add(executor.submit(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                PaymentRequestDto req = buildRequest(alice.getId(), bob.getId(),
                        amount.toPlainString(), "EUR", "idem-a2b-" + index);
                return mockMvc.perform(post("/payments")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(req)))
                        .andReturn();
            }));
        }
        // Bob → Alice payments
        for (int i = 0; i < paymentsPerDirection; i++) {
            final int index = i;
            futures.add(executor.submit(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                PaymentRequestDto req = buildRequest(bob.getId(), alice.getId(),
                        amount.toPlainString(), "EUR", "idem-b2a-" + index);
                return mockMvc.perform(post("/payments")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(req)))
                        .andReturn();
            }));
        }

        int successCount = 0;
        for (Future<MvcResult> future : futures) {
            MvcResult result = future.get(30, TimeUnit.SECONDS);
            int httpStatus = result.getResponse().getStatus();
            assertThat(httpStatus).isIn(201, 409, 422);
            if (httpStatus == 201) {
                successCount++;
            }
        }
        executor.shutdown();

        // Money conservation: total must still be 1000
        Account refreshedAlice = accountRepository.findById(alice.getId()).orElseThrow();
        Account refreshedBob = accountRepository.findById(bob.getId()).orElseThrow();
        BigDecimal totalAfter = refreshedAlice.getBalance().add(refreshedBob.getBalance());
        assertThat(totalAfter).isEqualByComparingTo("1000.0000");

        // Neither account should go negative
        assertThat(refreshedAlice.getBalance()).isGreaterThanOrEqualTo(BigDecimal.ZERO);
        assertThat(refreshedBob.getBalance()).isGreaterThanOrEqualTo(BigDecimal.ZERO);

        // Number of completed transactions matches success count
        long completedTx = transactionRepository.findAll().stream()
                .filter(t -> t.getStatus() == TransactionStatus.COMPLETED)
                .count();
        assertThat(completedTx).isEqualTo(successCount);
    }

    private PaymentRequestDto buildRequest(Long senderId, Long receiverId, String amount, String currency, String idempotencyKey) {
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
