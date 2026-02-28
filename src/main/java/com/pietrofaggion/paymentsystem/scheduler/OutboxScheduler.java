package com.pietrofaggion.paymentsystem.scheduler;

import com.pietrofaggion.paymentsystem.entity.NotificationOutbox;
import com.pietrofaggion.paymentsystem.entity.OutboxStatus;
import com.pietrofaggion.paymentsystem.repository.NotificationOutboxRepository;
import com.pietrofaggion.paymentsystem.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
public class OutboxScheduler {

    private static final int BATCH_SIZE = 50;

    private final NotificationOutboxRepository notificationOutboxRepository;
    private final NotificationService notificationService;

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void processOutbox() {
        // SKIP LOCKED ensures that rows already claimed by another app instance
        // are not fetched, preventing duplicate Kafka notifications.
        List<NotificationOutbox> pendingRows = notificationOutboxRepository
                .findPendingSkipLocked(OutboxStatus.PENDING, PageRequest.of(0, BATCH_SIZE));

        for (NotificationOutbox row : pendingRows) {
            try {
                notificationService.send(row.getTransaction());
                row.setStatus(OutboxStatus.SENT);
                row.setSentAt(OffsetDateTime.now());
            } catch (Exception ex) {
                int retries = row.getRetryCount() == null ? 0 : row.getRetryCount();
                retries++;
                row.setRetryCount(retries);
                if (retries >= 3) {
                    row.setStatus(OutboxStatus.FAILED);
                } else {
                    row.setStatus(OutboxStatus.PENDING);
                }
            }
            notificationOutboxRepository.save(row);
        }
    }
}
