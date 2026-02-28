package com.pietrofaggion.paymentsystem.service;

public interface NotificationService {

    /**
     * Publishes a pre-serialized notification payload to Kafka.
     *
     * @param payload   the JSON string already stored in the outbox row
     * @param kafkaKey  the Kafka partition key (sender account ID as string)
     */
    void send(String payload, String kafkaKey);
}
