package com.pietrofaggion.paymentsystem.repository;

import com.pietrofaggion.paymentsystem.entity.NotificationOutbox;
import com.pietrofaggion.paymentsystem.entity.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface NotificationOutboxRepository extends JpaRepository<NotificationOutbox, Long> {

    List<NotificationOutbox> findByStatus(OutboxStatus status);
}