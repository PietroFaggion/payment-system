package com.pietrofaggion.paymentsystem.scheduler;

import com.pietrofaggion.paymentsystem.entity.NotificationOutbox;
import com.pietrofaggion.paymentsystem.entity.OutboxStatus;
import com.pietrofaggion.paymentsystem.repository.NotificationOutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Claims a batch of PENDING outbox rows by transitioning them to PROCESSING
 * within a single short-lived transaction. The SKIP LOCKED hint ensures that
 * rows already locked by another instance are skipped, so each row is claimed
 * by exactly one instance.
 */
@Service
@RequiredArgsConstructor
public class OutboxClaimService {

    private final NotificationOutboxRepository notificationOutboxRepository;

    @Transactional
    public List<Long> claimPending(int batchSize) {
        List<NotificationOutbox> rows = notificationOutboxRepository
                .findPendingSkipLocked(OutboxStatus.PENDING, PageRequest.of(0, batchSize));

        rows.forEach(r -> r.setStatus(OutboxStatus.PROCESSING));
        notificationOutboxRepository.saveAll(rows);

        return rows.stream().map(NotificationOutbox::getId).toList();
    }
}
