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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class PaymentServiceImpl implements PaymentService {

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final NotificationOutboxRepository notificationOutboxRepository;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional
    public PaymentResponseDto createPayment(PaymentRequestDto request) {
        Transaction existing = transactionRepository.findByIdempotencyKey(request.getIdempotencyKey())
                .orElse(null);

        // the idea is to return an exception if a fraudulent attempt is made to reuse an idempotency key with different payment parameters,
        //  but allow repeated requests with exactly the same parameters to succeed without creating duplicate transactions
        if (existing != null) {
            if (!existing.getSenderAccount().getId().equals(request.getSenderAccountId())
                    || !existing.getReceiverAccount().getId().equals(request.getReceiverAccountId())
                    || existing.getAmount().compareTo(request.getAmount()) != 0
                    || !existing.getCurrency().equals(request.getCurrency())) {
                throw new IdempotencyConflictException();
            }
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

        return toResponse(savedTransaction);
    }

    @Override
    @Transactional(readOnly = true)
    public PaymentResponseDto getPaymentById(Long transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(TransactionNotFoundException::new);
        return toResponse(transaction);
    }

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
