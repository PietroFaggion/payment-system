package com.pietrofaggion.paymentsystem.scheduler;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Drives the two-phase outbox delivery loop:
 * <ol>
 *   <li>Claim — {@link OutboxClaimService} atomically marks a batch of PENDING
 *       rows as PROCESSING using SELECT FOR UPDATE SKIP LOCKED, then commits.
 *       Rows claimed by this instance are invisible to other instances.</li>
 *   <li>Process — {@link OutboxRowProcessor} handles each claimed row in its
 *       own REQUIRES_NEW transaction, so a Kafka failure on one row never
 *       rolls back the state of other rows in the same batch.</li>
 * </ol>
 */
@Component
@RequiredArgsConstructor
public class OutboxScheduler {

    private static final int BATCH_SIZE = 50;

    private final OutboxClaimService outboxClaimService;
    private final OutboxRowProcessor outboxRowProcessor;

    /**
     * Executes one iteration of the two-phase outbox delivery loop.
     * <ol>
     *   <li><b>Claim</b> — delegates to {@link OutboxClaimService#claimPending} which
     *       atomically transitions up to {@value BATCH_SIZE} PENDING rows to PROCESSING
     *       using {@code SELECT FOR UPDATE SKIP LOCKED} and commits.</li>
     *   <li><b>Process</b> — delegates each claimed ID to {@link OutboxRowProcessor#process},
     *       which runs in its own independent {@code REQUIRES_NEW} transaction so that a Kafka
     *       failure on one row never rolls back the state of the others.</li>
     * </ol>
     * The delay between consecutive runs is controlled by {@code app.outbox.scheduler.delay-ms}
     * (defaults to 5 000 ms). Increasing it locally is useful to observe intermediate DB states.
     */
    @Scheduled(fixedDelayString = "${app.outbox.scheduler.delay-ms:5000}")
    public void processOutbox() {
        List<Long> claimedIds = outboxClaimService.claimPending(BATCH_SIZE);
        for (Long rowId : claimedIds) {
            outboxRowProcessor.process(rowId);
        }
    }
}
