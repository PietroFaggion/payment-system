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

    /**
     * Selects up to {@code batchSize} PENDING rows using {@code SELECT FOR UPDATE SKIP LOCKED},
     * transitions them to {@code PROCESSING}, and commits — all within a single short transaction.
     * <p>
     * Rows already locked by another application instance are skipped automatically, so each row
     * is claimed by exactly one replica even when multiple instances run concurrently. The
     * claimed IDs are returned for subsequent per-row processing by
     * {@link OutboxRowProcessor#process}.
     *
     * @param batchSize maximum number of rows to claim in one scheduler tick
     * @return IDs of the rows that were successfully transitioned to {@code PROCESSING}
     */
    @Transactional
    public List<Long> claimPending(int batchSize) {
        List<NotificationOutbox> rows = notificationOutboxRepository
                .findPendingSkipLocked(OutboxStatus.PENDING, PageRequest.of(0, batchSize));

        rows.forEach(r -> r.setStatus(OutboxStatus.PROCESSING));
        notificationOutboxRepository.saveAll(rows);

        return rows.stream().map(NotificationOutbox::getId).toList();
    }
}
