package com.pietrofaggion.paymentsystem.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pietrofaggion.paymentsystem.dto.PaymentRequestDto;
import com.pietrofaggion.paymentsystem.dto.PaymentResponseDto;
import com.pietrofaggion.paymentsystem.entity.Account;
import com.pietrofaggion.paymentsystem.entity.Transaction;
import com.pietrofaggion.paymentsystem.entity.TransactionStatus;
import com.pietrofaggion.paymentsystem.exception.InsufficientFundsException;
import com.pietrofaggion.paymentsystem.repository.AccountRepository;
import com.pietrofaggion.paymentsystem.repository.NotificationOutboxRepository;
import com.pietrofaggion.paymentsystem.repository.TransactionRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentServiceImplTest {

    @Mock
    private AccountRepository accountRepository;

    @Mock
    private TransactionRepository transactionRepository;

    @Mock
    private NotificationOutboxRepository notificationOutboxRepository;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private PaymentServiceImpl paymentService;

    @Test
    void createPaymentShouldThrowWhenInsufficientFunds() {
        PaymentRequestDto request = buildRequest();
        Account sender = buildAccount(1L, "10.0000");
        Account receiver = buildAccount(2L, "30.0000");

        when(transactionRepository.findByIdempotencyKey(eq(request.getIdempotencyKey()))).thenReturn(Optional.empty());
        when(accountRepository.findByIdForUpdate(eq(1L))).thenReturn(Optional.of(sender));
        when(accountRepository.findById(eq(2L))).thenReturn(Optional.of(receiver));

        assertThatThrownBy(() -> paymentService.createPayment(request))
                .isInstanceOf(InsufficientFundsException.class);

        verify(transactionRepository, never()).save(any(Transaction.class));
        verifyNoInteractions(notificationOutboxRepository);
    }

    @Test
    void createPaymentShouldReturnExistingTransactionWhenIdempotencyKeyAlreadyExists() {
        PaymentRequestDto request = buildRequest();
        Account sender = buildAccount(1L, "100.0000");
        Account receiver = buildAccount(2L, "30.0000");

        Transaction existing = new Transaction();
        existing.setId(99L);
        existing.setSenderAccount(sender);
        existing.setReceiverAccount(receiver);
        existing.setAmount(request.getAmount());
        existing.setCurrency(request.getCurrency());
        existing.setIdempotencyKey(request.getIdempotencyKey());
        existing.setStatus(TransactionStatus.COMPLETED);

        when(transactionRepository.findByIdempotencyKey(eq(request.getIdempotencyKey())))
                .thenReturn(Optional.of(existing));

        PaymentResponseDto response = paymentService.createPayment(request);

        assertThat(response.getTransactionId()).isEqualTo(99L);
        assertThat(response.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
        verifyNoInteractions(accountRepository);
        verify(transactionRepository, never()).save(any(Transaction.class));
        verifyNoInteractions(notificationOutboxRepository);
    }

    private PaymentRequestDto buildRequest() {
        PaymentRequestDto request = new PaymentRequestDto();
        request.setSenderAccountId(1L);
        request.setReceiverAccountId(2L);
        request.setAmount(new BigDecimal("20.0000"));
        request.setCurrency("EUR");
        request.setIdempotencyKey("idem-unit-1");
        return request;
    }

    private Account buildAccount(Long id, String balance) {
        Account account = new Account();
        account.setId(id);
        account.setBalance(new BigDecimal(balance));
        account.setCurrency("EUR");
        return account;
    }
}
