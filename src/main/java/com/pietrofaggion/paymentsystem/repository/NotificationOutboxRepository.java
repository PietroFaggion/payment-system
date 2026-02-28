package com.pietrofaggion.paymentsystem.repository;

import com.pietrofaggion.paymentsystem.entity.NotificationOutbox;
import com.pietrofaggion.paymentsystem.entity.OutboxStatus;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface NotificationOutboxRepository extends JpaRepository<NotificationOutbox, Long> {

    List<NotificationOutbox> findByStatus(OutboxStatus status);

    // SKIP LOCKED (-2) means rows already locked by another transaction are skipped
    // rather than waited on, preventing duplicate sends across multiple app instances.
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("SELECT o FROM NotificationOutbox o WHERE o.status = :status ORDER BY o.id")
    List<NotificationOutbox> findPendingSkipLocked(@Param("status") OutboxStatus status, Pageable pageable);
}