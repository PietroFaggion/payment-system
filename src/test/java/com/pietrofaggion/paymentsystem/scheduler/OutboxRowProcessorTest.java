package com.pietrofaggion.paymentsystem.scheduler;

import com.pietrofaggion.paymentsystem.entity.Account;
import com.pietrofaggion.paymentsystem.entity.NotificationOutbox;
import com.pietrofaggion.paymentsystem.entity.OutboxStatus;
import com.pietrofaggion.paymentsystem.entity.Transaction;
import com.pietrofaggion.paymentsystem.repository.NotificationOutboxRepository;
import com.pietrofaggion.paymentsystem.service.NotificationService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxRowProcessorTest {

    @Mock
    private NotificationOutboxRepository notificationOutboxRepository;

    @Mock
    private NotificationService notificationService;

    @InjectMocks
    private OutboxRowProcessor outboxRowProcessor;

    @Test
    void processShouldMarkAsSentWhenNotificationSucceeds() {
        NotificationOutbox row = buildProcessingRow(0);
        when(notificationOutboxRepository.findById(1L)).thenReturn(Optional.of(row));

        outboxRowProcessor.process(1L);

        assertThat(row.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(row.getSentAt()).isNotNull();
        assertThat(row.getRetryCount()).isEqualTo(0);
        verify(notificationService).send(eq(row.getPayload()), any(String.class));
        verify(notificationOutboxRepository).save(row);
    }

    @Test
    void processShouldIncreaseRetryAndKeepPendingBeforeMaxRetries() {
        NotificationOutbox row = buildProcessingRow(1);
        when(notificationOutboxRepository.findById(1L)).thenReturn(Optional.of(row));
        doThrow(new RuntimeException("send failed"))
                .when(notificationService).send(any(), any());

        outboxRowProcessor.process(1L);

        assertThat(row.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(row.getRetryCount()).isEqualTo(2);
        assertThat(row.getSentAt()).isNull();
        verify(notificationOutboxRepository).save(row);
    }

    @Test
    void processShouldMarkAsFailedAtThirdRetry() {
        NotificationOutbox row = buildProcessingRow(2);
        when(notificationOutboxRepository.findById(1L)).thenReturn(Optional.of(row));
        doThrow(new RuntimeException("send failed"))
                .when(notificationService).send(any(), any());

        outboxRowProcessor.process(1L);

        assertThat(row.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(row.getRetryCount()).isEqualTo(3);
        assertThat(row.getSentAt()).isNull();
        verify(notificationOutboxRepository, times(1)).save(row);
    }

    @Test
    void processShouldSkipRowIfNotFoundOrNotProcessing() {
        when(notificationOutboxRepository.findById(99L)).thenReturn(Optional.empty());

        outboxRowProcessor.process(99L);

        verifyNoInteractions(notificationService);
        verify(notificationOutboxRepository, never()).save(any());
    }

    private NotificationOutbox buildProcessingRow(int retryCount) {
        Account sender = new Account();
        sender.setId(10L);

        Transaction transaction = new Transaction();
        transaction.setSenderAccount(sender);

        NotificationOutbox outbox = new NotificationOutbox();
        outbox.setId(1L);
        outbox.setStatus(OutboxStatus.PROCESSING);
        outbox.setRetryCount(retryCount);
        outbox.setPayload("{\"transactionId\":\"1\",\"senderAccountId\":\"10\"}");
        outbox.setTransaction(transaction);
        return outbox;
    }
}
