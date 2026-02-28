package com.pietrofaggion.paymentsystem.scheduler;

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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxSchedulerTest {

    @Mock
    private NotificationOutboxRepository notificationOutboxRepository;

    @Mock
    private NotificationService notificationService;

    @InjectMocks
    private OutboxScheduler outboxScheduler;

    @Test
    void processOutboxShouldMarkAsSentWhenNotificationSucceeds() {
        NotificationOutbox row = buildPendingRow(0);
        when(notificationOutboxRepository.findByStatus(OutboxStatus.PENDING)).thenReturn(List.of(row));

        outboxScheduler.processOutbox();

        assertThat(row.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(row.getSentAt()).isNotNull();
        assertThat(row.getRetryCount()).isEqualTo(0);
        verify(notificationService).send(row.getTransaction());
        verify(notificationOutboxRepository).save(row);
    }

    @Test
    void processOutboxShouldIncreaseRetryAndKeepPendingBeforeMaxRetries() {
        NotificationOutbox row = buildPendingRow(1);
        when(notificationOutboxRepository.findByStatus(OutboxStatus.PENDING)).thenReturn(List.of(row));
        doThrow(new RuntimeException("send failed")).when(notificationService).send(row.getTransaction());

        outboxScheduler.processOutbox();

        assertThat(row.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(row.getRetryCount()).isEqualTo(2);
        assertThat(row.getSentAt()).isNull();
        verify(notificationOutboxRepository).save(row);
    }

    @Test
    void processOutboxShouldMarkAsFailedAtThirdRetry() {
        NotificationOutbox row = buildPendingRow(2);
        when(notificationOutboxRepository.findByStatus(OutboxStatus.PENDING)).thenReturn(List.of(row));
        doThrow(new RuntimeException("send failed")).when(notificationService).send(row.getTransaction());

        outboxScheduler.processOutbox();

        assertThat(row.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(row.getRetryCount()).isEqualTo(3);
        assertThat(row.getSentAt()).isNull();
        verify(notificationOutboxRepository, times(1)).save(row);
    }

    private NotificationOutbox buildPendingRow(int retryCount) {
        NotificationOutbox outbox = new NotificationOutbox();
        outbox.setId(1L);
        outbox.setStatus(OutboxStatus.PENDING);
        outbox.setRetryCount(retryCount);
        outbox.setTransaction(new Transaction());
        return outbox;
    }
}
