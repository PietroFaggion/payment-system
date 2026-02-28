# Payment System

Spring Boot payment service with:
- PostgreSQL for accounts/transactions/outbox persistence
- Kafka for notification delivery
- Transactional outbox pattern for reliable async publishing

## Prerequisites

- Docker + Docker Compose
- Java 21
- Maven 3.9+

## 1. Infrastructure Setup

From project root:

```bash
docker compose up -d
```

Expected services:
- `zookeeper`
- `kafka`
- `schema-registry`
- `kafka-init` (runs once and exits 0 after topic creation)
- `postgres`

Quick checks:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
curl http://localhost:8081/subjects
docker exec -it postgres psql -U payments_user -d payments_db -c "\dt"
```

Notes:
- Postgres host port is `5433` (mapped to container `5432`).
- Kafka topic `payment-notifications` is created by `kafka-init`.

## 2. Application Configuration

The app reads environment defaults from `src/main/resources/application.yml`:
- DB: `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_PORT`
- Kafka broker: `KAFKA_BROKER_PORT`

Default local values match `.env` used by Docker Compose.

## 3. Run the Application

From project root:

```bash
mvn spring-boot:run
```

If you use wrapper in your environment:

```bash
./mvnw spring-boot:run
```

Flyway runs automatically at startup and applies:
- `src/main/resources/db/migration/V1__init_schema.sql`

## 4. Main API

### Create payment

`POST /payments`

Request body:

```json
{
  "senderAccountId": 1,
  "receiverAccountId": 2,
  "amount": 25.0000,
  "currency": "EUR",
  "idempotencyKey": "idem-123"
}
```

Example:

```bash
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{"senderAccountId":1,"receiverAccountId":2,"amount":25.0000,"currency":"EUR","idempotencyKey":"idem-123"}'
```

## Implementation Overview

### Payment flow (synchronous)

Entry point:
- `src/main/java/com/pietrofaggion/paymentsystem/controller/PaymentController.java`

Core transactional logic:
- `src/main/java/com/pietrofaggion/paymentsystem/service/impl/PaymentServiceImpl.java`

Key behaviors:
- Idempotency check by `idempotencyKey`
- Pessimistic lock on sender account (`findByIdForUpdate`)
- Balance validation
- Sender debit + receiver credit
- Transaction persistence (`COMPLETED`)
- Outbox row creation (`PENDING`) with JSON payload

### Outbox delivery (asynchronous)

Scheduler:
- `src/main/java/com/pietrofaggion/paymentsystem/scheduler/OutboxScheduler.java`

Every 5s:
- Loads `PENDING` outbox rows
- Calls notification publisher
- On success: `SENT` + `sentAt`
- On failure: increments `retryCount`, marks `FAILED` when `retryCount >= 3`

Scheduling enabled in:
- `src/main/java/com/pietrofaggion/paymentsystem/config/AppConfig.java`

Scheduler single thread:
- `spring.task.scheduling.pool.size=1` in `src/main/resources/application.yml`

### Kafka publishing

Publisher service:
- `src/main/java/com/pietrofaggion/paymentsystem/service/impl/NotificationServiceImpl.java`

Producer config:
- `src/main/java/com/pietrofaggion/paymentsystem/config/KafkaConfig.java`

Behavior:
- Sends JSON string message to topic `payment-notifications`
- Kafka key is `senderAccountId` (keeps same-sender ordering/partition affinity)

### Persistence model

Entities:
- `src/main/java/com/pietrofaggion/paymentsystem/entity/Account.java`
- `src/main/java/com/pietrofaggion/paymentsystem/entity/Transaction.java`
- `src/main/java/com/pietrofaggion/paymentsystem/entity/NotificationOutbox.java`

Repositories:
- `src/main/java/com/pietrofaggion/paymentsystem/repository/AccountRepository.java`
- `src/main/java/com/pietrofaggion/paymentsystem/repository/TransactionRepository.java`
- `src/main/java/com/pietrofaggion/paymentsystem/repository/NotificationOutboxRepository.java`

### Error handling

Global exception mapping:
- `src/main/java/com/pietrofaggion/paymentsystem/exception/GlobalExceptionHandler.java`

Handles:
- Insufficient funds
- Account not found
- Validation errors
- Idempotency key unique constraint conflicts

## Tests

Integration:
- `src/test/java/com/pietrofaggion/paymentsystem/controller/PaymentControllerIntegrationTest.java`

Unit:
- `src/test/java/com/pietrofaggion/paymentsystem/service/impl/PaymentServiceImplTest.java`
- `src/test/java/com/pietrofaggion/paymentsystem/service/impl/NotificationServiceImplTest.java`
- `src/test/java/com/pietrofaggion/paymentsystem/scheduler/OutboxSchedulerTest.java`

Run tests:

```bash
mvn test
```
