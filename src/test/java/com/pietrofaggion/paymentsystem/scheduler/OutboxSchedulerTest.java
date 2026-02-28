package com.pietrofaggion.paymentsystem.scheduler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxSchedulerTest {

    @Mock
    private OutboxClaimService outboxClaimService;

    @Mock
    private OutboxRowProcessor outboxRowProcessor;

    @InjectMocks
    private OutboxScheduler outboxScheduler;

    @Test
    void processOutboxShouldDelegateEachClaimedRowToProcessor() {
        when(outboxClaimService.claimPending(anyInt())).thenReturn(List.of(1L, 2L, 3L));

        outboxScheduler.processOutbox();

        verify(outboxRowProcessor).process(1L);
        verify(outboxRowProcessor).process(2L);
        verify(outboxRowProcessor).process(3L);
    }

    @Test
    void processOutboxShouldDoNothingWhenNoRowsClaimed() {
        when(outboxClaimService.claimPending(anyInt())).thenReturn(List.of());

        outboxScheduler.processOutbox();

        verifyNoInteractions(outboxRowProcessor);
    }
}
