package com.pietrofaggion.paymentsystem.scheduler;

import com.pietrofaggion.paymentsystem.entity.NotificationOutbox;
import com.pietrofaggion.paymentsystem.entity.OutboxStatus;
import com.pietrofaggion.paymentsystem.repository.NotificationOutboxRepository;
import com.pietrofaggion.paymentsystem.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

/**
 * Processes a single outbox row in its own independent transaction
 * (REQUIRES_NEW). Each row commits or rolls back independently, so a Kafka
 * failure on one row does not affect sibling rows in the same scheduler run.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRowProcessor {

    private final NotificationOutboxRepository notificationOutboxRepository;
    private final NotificationService notificationService;

    /**
     * Sends the Kafka notification for a single outbox row and updates its final status,
     * all within an independent {@code REQUIRES_NEW} transaction so that a failure on this
     * row does not affect sibling rows processed in the same scheduler batch.
     * <p>
     * On <b>success</b> the row is marked {@code SENT} and {@code sentAt} is timestamped.<br>
     * On <b>failure</b> {@code retryCount} is incremented; once it reaches 3 the row is
     * permanently marked {@code FAILED} and will no longer be picked up by the scheduler.
     * <p>
     * If the row is not found or is no longer in {@code PROCESSING} state (e.g. claimed and
     * resolved by another instance after a crash-recovery), the method returns immediately
     * without taking any action.
     *
     * @param rowId primary key of the {@link NotificationOutbox} row to process
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void process(Long rowId) {
        NotificationOutbox row = notificationOutboxRepository.findById(rowId).orElse(null);
        if (row == null || row.getStatus() != OutboxStatus.PROCESSING) {
            log.debug("Outbox row id={} skipped - not found or no longer PROCESSING", rowId);
            return;
        }

        log.debug("Processing outbox row id={} transactionId={}", rowId, row.getTransaction().getId());
        String kafkaKey = String.valueOf(row.getTransaction().getSenderAccount().getId());
        try {
            notificationService.send(row.getPayload(), kafkaKey);
            row.setStatus(OutboxStatus.SENT);
            row.setSentAt(OffsetDateTime.now());
            log.info("Outbox row id={} SENT - transactionId={}", rowId, row.getTransaction().getId());
        } catch (Exception ex) {
            int retries = row.getRetryCount() == null ? 0 : row.getRetryCount();
            retries++;
            row.setRetryCount(retries);
            if (retries >= 3) {
                row.setStatus(OutboxStatus.FAILED);
                log.error("Outbox row id={} FAILED permanently after {} attempts - error: {}",
                        rowId, retries, ex.getMessage());
            } else {
                row.setStatus(OutboxStatus.PENDING);
                log.warn("Outbox row id={} send failed, will retry (attempt {}/3) - error: {}",
                        rowId, retries, ex.getMessage());
            }
        }
        notificationOutboxRepository.save(row);
    }
}
