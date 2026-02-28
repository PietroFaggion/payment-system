package com.pietrofaggion.paymentsystem.scheduler;

import com.pietrofaggion.paymentsystem.entity.NotificationOutbox;
import com.pietrofaggion.paymentsystem.entity.OutboxStatus;
import com.pietrofaggion.paymentsystem.repository.NotificationOutboxRepository;
import com.pietrofaggion.paymentsystem.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

/**
 * Processes a single outbox row in its own independent transaction
 * (REQUIRES_NEW). Each row commits or rolls back independently, so a Kafka
 * failure on one row does not affect sibling rows in the same scheduler run.
 */
@Component
@RequiredArgsConstructor
public class OutboxRowProcessor {

    private final NotificationOutboxRepository notificationOutboxRepository;
    private final NotificationService notificationService;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void process(Long rowId) {
        NotificationOutbox row = notificationOutboxRepository.findById(rowId).orElse(null);
        if (row == null || row.getStatus() != OutboxStatus.PROCESSING) {
            return; // Already handled (e.g. by another instance after a crash-recovery)
        }

        String kafkaKey = String.valueOf(row.getTransaction().getSenderAccount().getId());
        try {
            notificationService.send(row.getPayload(), kafkaKey);
            row.setStatus(OutboxStatus.SENT);
            row.setSentAt(OffsetDateTime.now());
        } catch (Exception ex) {
            int retries = row.getRetryCount() == null ? 0 : row.getRetryCount();
            retries++;
            row.setRetryCount(retries);
            row.setStatus(retries >= 3 ? OutboxStatus.FAILED : OutboxStatus.PENDING);
        }
        notificationOutboxRepository.save(row);
    }
}
