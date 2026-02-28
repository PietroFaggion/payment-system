package com.pietrofaggion.paymentsystem.entity;

public enum OutboxStatus {
    PENDING,
    PROCESSING,
    SENT,
    FAILED
}