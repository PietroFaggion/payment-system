package com.pietrofaggion.paymentsystem.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pietrofaggion.paymentsystem.dto.PaymentRequestDto;
import com.pietrofaggion.paymentsystem.dto.PaymentResponseDto;
import com.pietrofaggion.paymentsystem.entity.*;
import com.pietrofaggion.paymentsystem.exception.AccountNotFoundException;
import com.pietrofaggion.paymentsystem.exception.CurrencyMismatchException;
import com.pietrofaggion.paymentsystem.exception.IdempotencyConflictException;
import com.pietrofaggion.paymentsystem.exception.InsufficientFundsException;
import com.pietrofaggion.paymentsystem.exception.TransactionNotFoundException;
import com.pietrofaggion.paymentsystem.repository.AccountRepository;
import com.pietrofaggion.paymentsystem.repository.NotificationOutboxRepository;
import com.pietrofaggion.paymentsystem.repository.TransactionRepository;
import com.pietrofaggion.paymentsystem.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentServiceImpl implements PaymentService {

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final NotificationOutboxRepository notificationOutboxRepository;
    private final ObjectMapper objectMapper;

    /**
     * Executes a money transfer within a single database transaction.
     * <p>
     * Steps (all atomic - any failure rolls back everything):
     * <ol>
     *   <li><b>Idempotency check</b> - if the key already exists and all parameters match,
     *       the stored result is returned immediately without re-executing the transfer.
     *       If the key exists but with different parameters, {@link IdempotencyConflictException}
     *       is thrown to signal a fraudulent reuse attempt.</li>
     *   <li><b>Pessimistic lock</b> - both accounts are locked with {@code SELECT FOR UPDATE}
     *       in ascending ID order to prevent the classic A→B / B→A deadlock pattern.</li>
     *   <li><b>Validation</b> - currency match and sufficient balance are verified
     *       after locking to avoid race conditions with concurrent transfers.</li>
     *   <li><b>Balance update</b> - sender is debited and receiver is credited.</li>
     *   <li><b>Transaction record</b> - a {@link Transaction} row is persisted with
     *       status {@code COMPLETED}.</li>
     *   <li><b>Outbox row</b> - a {@link NotificationOutbox} row is written in the same
     *       transaction so the Kafka notification is delivered reliably by the background
     *       scheduler even if the broker is temporarily unavailable.</li>
     * </ol>
     *
     * @param request validated payment request
     * @return response DTO with the transaction ID and current status
     * @throws IdempotencyConflictException if the key is reused with different parameters
     * @throws AccountNotFoundException     if either account does not exist
     * @throws CurrencyMismatchException    if the request currency differs from the sender's account currency
     * @throws InsufficientFundsException   if the sender balance is insufficient
     */
    @Override
    @Transactional
    public PaymentResponseDto createPayment(PaymentRequestDto request) {
        Transaction existing = transactionRepository.findByIdempotencyKey(request.getIdempotencyKey())
                .orElse(null);

        if (existing != null) {
            if (!existing.getSenderAccount().getId().equals(request.getSenderAccountId())
                    || !existing.getReceiverAccount().getId().equals(request.getReceiverAccountId())
                    || existing.getAmount().compareTo(request.getAmount()) != 0
                    || !existing.getCurrency().equals(request.getCurrency())) {
                log.warn("Idempotency conflict - key={} reused with different parameters", request.getIdempotencyKey());
                throw new IdempotencyConflictException();
            }
            log.info("Idempotency hit - key={} returning existing transactionId={}", request.getIdempotencyKey(), existing.getId());
            return toResponse(existing);
        }

        Long senderId = request.getSenderAccountId();
        Long receiverId = request.getReceiverAccountId();

        // Acquire locks in ascending ID order to prevent deadlocks when two concurrent
        // payments involve the same account pair in opposite directions (A→B and B→A).
        Long lowId = Math.min(senderId, receiverId);
        Long highId = Math.max(senderId, receiverId);

        Account first = accountRepository.findByIdForUpdate(lowId)
                .orElseThrow(AccountNotFoundException::new);
        Account second = accountRepository.findByIdForUpdate(highId)
                .orElseThrow(AccountNotFoundException::new);

        Account sender = first.getId().equals(senderId) ? first : second;
        Account receiver = first.getId().equals(receiverId) ? first : second;

        if (!sender.getCurrency().equals(request.getCurrency())) {
            throw new CurrencyMismatchException();
        }

        if (sender.getBalance().compareTo(request.getAmount()) < 0) {
            throw new InsufficientFundsException();
        }

        sender.setBalance(sender.getBalance().subtract(request.getAmount()));
        receiver.setBalance(receiver.getBalance().add(request.getAmount()));
        accountRepository.save(sender);
        accountRepository.save(receiver);

        OffsetDateTime now = OffsetDateTime.now();

        Transaction transaction = new Transaction();
        transaction.setSenderAccount(sender);
        transaction.setReceiverAccount(receiver);
        transaction.setAmount(request.getAmount());
        transaction.setCurrency(request.getCurrency());
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setIdempotencyKey(request.getIdempotencyKey());
        transaction.setCreatedAt(now);
        transaction.setUpdatedAt(now);
        Transaction savedTransaction = transactionRepository.save(transaction);

        NotificationOutbox outbox = new NotificationOutbox();
        outbox.setTransaction(savedTransaction);
        outbox.setStatus(OutboxStatus.PENDING);
        outbox.setPayload(buildOutboxPayload(savedTransaction));
        outbox.setCreatedAt(now);
        outbox.setRetryCount(0);
        notificationOutboxRepository.save(outbox);

        log.info("Payment created - transactionId={} sender={} receiver={} amount={} currency={}",
                savedTransaction.getId(), senderId, receiverId, request.getAmount(), request.getCurrency());
        return toResponse(savedTransaction);
    }

    /**
     * Retrieves a transaction by its primary key in a read-only transaction.
     *
     * @param transactionId the transaction's primary key
     * @return the corresponding response DTO
     * @throws TransactionNotFoundException if no transaction exists with the given ID
     */
    @Override
    @Transactional(readOnly = true)
    public PaymentResponseDto getPaymentById(Long transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(TransactionNotFoundException::new);
        return toResponse(transaction);
    }

    /**
     * Serialises the relevant transaction fields to a JSON string that is stored in the outbox
     * row and later published verbatim as the Kafka message value.
     *
     * @param transaction the persisted transaction to serialise
     * @return JSON-encoded notification payload
     * @throws IllegalStateException if Jackson serialisation fails (should never occur)
     */
    private String buildOutboxPayload(Transaction transaction) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("transactionId", transaction.getId());
        payload.put("senderAccountId", transaction.getSenderAccount().getId());
        payload.put("receiverAccountId", transaction.getReceiverAccount().getId());
        payload.put("amount", transaction.getAmount());
        payload.put("currency", transaction.getCurrency());
        payload.put("status", transaction.getStatus().name());
        payload.put("idempotencyKey", transaction.getIdempotencyKey());
        payload.put("createdAt", transaction.getCreatedAt().toString());

        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize notification payload", e);
        }
    }

    /**
     * Maps a {@link Transaction} entity to the API response DTO.
     *
     * @param transaction the source entity
     * @return the corresponding {@link PaymentResponseDto}
     */
    private PaymentResponseDto toResponse(Transaction transaction) {
        PaymentResponseDto response = new PaymentResponseDto();
        response.setTransactionId(transaction.getId());
        response.setSenderAccountId(transaction.getSenderAccount().getId());
        response.setReceiverAccountId(transaction.getReceiverAccount().getId());
        response.setAmount(transaction.getAmount());
        response.setCurrency(transaction.getCurrency());
        response.setIdempotencyKey(transaction.getIdempotencyKey());
        response.setStatus(transaction.getStatus());
        return response;
    }
}
