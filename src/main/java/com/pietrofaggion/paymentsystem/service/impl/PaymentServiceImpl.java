package com.pietrofaggion.paymentsystem.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pietrofaggion.paymentsystem.dto.PaymentRequestDto;
import com.pietrofaggion.paymentsystem.dto.PaymentResponseDto;
import com.pietrofaggion.paymentsystem.entity.*;
import com.pietrofaggion.paymentsystem.exception.AccountNotFoundException;
import com.pietrofaggion.paymentsystem.exception.InsufficientFundsException;
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
        if (existing != null) {
            return toResponse(existing);
        }

        Account sender = accountRepository.findByIdForUpdate(request.getSenderAccountId())
                .orElseThrow(() -> new AccountNotFoundException());
        Account receiver = accountRepository.findById(request.getReceiverAccountId())
                .orElseThrow(() -> new AccountNotFoundException());

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
