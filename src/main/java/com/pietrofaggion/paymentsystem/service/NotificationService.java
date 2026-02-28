package com.pietrofaggion.paymentsystem.service;

import com.pietrofaggion.paymentsystem.entity.Transaction;

public interface NotificationService {

    void send(Transaction transaction);
}
