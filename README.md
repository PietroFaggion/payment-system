# Payment System

Spring Boot payment service built for reliability and correctness.

## Stack

| Layer | Technology |
|---|---|
| Runtime | Java 21, Spring Boot 3 |
| Persistence | PostgreSQL + JPA/Hibernate, Flyway |
| Messaging | Apache Kafka |
| Docs | SpringDoc OpenAPI (Swagger UI) |
| Observability | Spring Boot Actuator |

## Features

### Transactional payment processing
Each payment is executed in a single database transaction: balance check, sender debit, receiver credit, and outbox row creation either all succeed or all roll back.

### Concurrency safety
Both accounts are pessimistically locked (`SELECT FOR UPDATE`) in ascending ID order before any balance is touched, preventing deadlocks and double-spending. The `Account` entity also carries an optimistic-lock version column as a secondary guard.

### Idempotency
Every request carries an `idempotencyKey`. A unique DB constraint ensures a duplicate request is rejected with HTTP 409 rather than applied twice.

### Transactional outbox pattern
A `NotificationOutbox` row is written atomically with the transaction record. A background scheduler delivers it to Kafka asynchronously, so a broker outage never blocks or fails a payment.

### Safe multi-instance outbox delivery
The scheduler claims rows with `SELECT FOR UPDATE SKIP LOCKED` and marks them `PROCESSING` before releasing the lock. Other app instances skip those rows, preventing duplicate Kafka messages when multiple replicas run concurrently.

### Retry and failure handling
Failed Kafka sends are retried up to three times (`retryCount` tracked per row). After three failures the row is marked `FAILED`. The producer is configured with `enable.idempotence=true` and `acks=all`.

### Validation
- Self-transfer rejected (sender ≠ receiver)
- Currency must match the sender account's currency
- Amount must be positive, all fields required
- Errors return a structured `ApiError` with an appropriate HTTP status

## Design Patterns

### Transactional Outbox
The payment record and the `NotificationOutbox` row are written in a single DB transaction. A background scheduler reads the outbox and delivers the Kafka message asynchronously, so a broker outage never blocks or rolls back a payment.

### Two-phase Outbox Delivery
The scheduler operates in two separate transactions to avoid holding locks across slow I/O:
1. **Claim** (`OutboxClaimService`) — `SELECT FOR UPDATE SKIP LOCKED` atomically marks a batch of rows `PROCESSING` and commits. Rows locked by another instance are automatically skipped.
2. **Process** (`OutboxRowProcessor`) — each row runs in its own `REQUIRES_NEW` transaction so a Kafka failure on one row never rolls back the others.

### Pessimistic Locking (deadlock-free)
Before any balance change, both accounts are locked with `SELECT FOR UPDATE` in ascending ID order. The consistent ordering eliminates the classic A→B / B→A deadlock when two concurrent transfers share the same account pair.

### Idempotency
A unique DB constraint on `idempotency_key` is the primary safety net against duplicate transactions. The application layer adds a second check: if the same key is reused with different parameters it returns HTTP `409 Conflict` to surface a potential fraudulent attempt rather than silently ignoring it.

### HTTP Basic Authentication
All payment endpoints are protected via Spring Security HTTP Basic. Credentials are externalised through environment variables (`SECURITY_USER`, `SECURITY_PASSWORD`). Actuator health/info and Swagger UI remain open to simplify monitoring and developer tooling.

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/payments` | Create a payment — returns `201 Created` with `Location` header |
| `GET` | `/payments/{id}` | Retrieve a payment by transaction ID |

Interactive docs: `http://localhost:8080/swagger-ui.html`
Health probe: `http://localhost:8080/actuator/health`

## Prerequisites

- Docker + Docker Compose
- Java 21
- Maven 3.9+

## Running locally

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Run the application (Flyway migrations apply automatically)
mvn spring-boot:run
```

Default ports: app `8080`, Postgres `5433`, Kafka `9092`.

## Tests

```bash
mvn test
```

Integration tests use an embedded H2 database (PostgreSQL mode) and embedded Kafka — no external services required.
