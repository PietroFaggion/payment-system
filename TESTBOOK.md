# Testbook

Step-by-step actions to manually exercise the payment system end to end.

---

## 1. Start infrastructure

```bash
docker compose up -d
```

Wait until all services are healthy (usually ~30 s):

```bash
docker compose ps
```

All services should show `healthy` or `exited 0` (kafka-init).

---

## 2. Verify infrastructure

```bash
# Kafka topic was created by kafka-init
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
# Expected: payment-notifications

# Postgres is reachable (DB is empty until the app runs Flyway in step 4)
docker exec -it postgres psql -U payments_user -d payments_db -c "SELECT 1;"
# Expected: 1
```

---

## 3. Start a Kafka console consumer

Open a dedicated terminal and leave it running. All Kafka messages produced by the app will appear here.

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payment-notifications \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "
```

---

## 4. Start the application

```bash
mvn spring-boot:run
```

Flyway applies migrations V1 (schema) and V2 (seed data) automatically.
The app starts on port 8080.

Check it is up:

```bash
curl http://localhost:8080/actuator/health
# Expected: {"status":"UP", ...}
```

Confirm Flyway ran and seed data is in place:

```bash
docker exec -it postgres psql -U payments_user -d payments_db -c "\dt"
# Expected: accounts, flyway_schema_history, notification_outbox, transactions

docker exec -it postgres psql -U payments_user -d payments_db -c "SELECT id, owner_name, balance, currency FROM accounts;"
```

Seed accounts:

| ID | Owner | Balance | Currency |
|---|---|---|---|
| 1001 | Alice Doe | 1500.00 | EUR |
| 1002 | Bob Roe | 750.00 | USD |
| 1003 | Charlie Poe | 300.00 | EUR |

---

## 5. Happy path — successful payment

Alice (EUR) sends 100 EUR to Charlie (EUR):

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 1001,
    "receiverAccountId": 1003,
    "amount": 100.00,
    "currency": "EUR",
    "idempotencyKey": "test-happy-001"
  }'
```

Expected:
- HTTP `201 Created`
- `Location: /payments/{id}` header
- Response body with `transactionId`, `status: COMPLETED`
- Within ~5 s, the Kafka consumer terminal shows a message with key `1001` and the JSON payload

---

## 6. Retrieve the payment

```bash
curl http://localhost:8080/payments/{transactionId}
# Replace {transactionId} with the ID from the previous response
```

Expected: HTTP `200 OK` with full transaction details.

---

## 7. Idempotency — safe retry (same parameters)

Replay the exact same request (same key, same parameters) to simulate a network retry:

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 1001,
    "receiverAccountId": 1003,
    "amount": 100.00,
    "currency": "EUR",
    "idempotencyKey": "test-happy-001"
  }'
```

Expected: HTTP `201 Created` — returns the **same** `transactionId` as the first call. Balance unchanged (no second deduction). No new Kafka message.

---

## 8. Idempotency — key reuse with different parameters

Attempt to reuse the same key but with a different amount (fraudulent/misuse attempt):

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 1001,
    "receiverAccountId": 1003,
    "amount": 999.00,
    "currency": "EUR",
    "idempotencyKey": "test-happy-001"
  }'
```

Expected: HTTP `409 Conflict` with message `"Idempotency key reused with different payment parameters"`. Original payment is unaffected.

---

## 9. Insufficient funds

Charlie (300 EUR at startup) tries to send more than he has:

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 1003,
    "receiverAccountId": 1001,
    "amount": 999.00,
    "currency": "EUR",
    "idempotencyKey": "test-funds-001"
  }'
```

Expected: HTTP `422 Unprocessable Entity` — no balance change, no Kafka message.

---

## 10. Currency mismatch

Alice (EUR account) sends with USD currency:

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 1001,
    "receiverAccountId": 1003,
    "amount": 50.00,
    "currency": "USD",
    "idempotencyKey": "test-currency-001"
  }'
```

Expected: HTTP `422 Unprocessable Entity`.

---

## 11. Self-transfer

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 1001,
    "receiverAccountId": 1001,
    "amount": 50.00,
    "currency": "EUR",
    "idempotencyKey": "test-self-001"
  }'
```

Expected: HTTP `400 Bad Request` — sender and receiver must be different.

---

## 12. Account not found

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 9999,
    "receiverAccountId": 1001,
    "amount": 10.00,
    "currency": "EUR",
    "idempotencyKey": "test-notfound-001"
  }'
```

Expected: HTTP `404 Not Found`.

---

## 13. Validation error

Missing required fields:

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{}'
```

Expected: HTTP `400 Bad Request` with field-level error details.

---

## 14. Observe the outbox pattern under Kafka failure

This step shows the payment succeeding immediately while Kafka is down, then the scheduler retrying and delivering once Kafka recovers.

**Tip:** the scheduler delay defaults to 5 s, which can be too fast to catch manually. Start the app with a slower delay so you have time to inspect the DB between runs:

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--app.outbox.scheduler.delay-ms=30000
```

**Steps:**

```bash
# 1. Pause Kafka (simulates a broker outage — the container is still "running" but not responding)
docker pause kafka

# 2. Create a payment — succeeds immediately (outbox decouples Kafka from the HTTP response)
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 1001,
    "receiverAccountId": 1003,
    "amount": 10.00,
    "currency": "EUR",
    "idempotencyKey": "test-outbox-demo"
  }'
# Expected: HTTP 201 Created — payment goes through despite Kafka being down

# 3. Check the outbox row — it should be PENDING (or PROCESSING during a scheduler tick)
docker exec -it postgres psql -U payments_user -d payments_db \
  -c "SELECT id, status, retry_count, sent_at FROM notification_outbox ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PENDING, retry_count=0 (before first scheduler run)

# 4. Wait for one or two scheduler ticks (~30 s if using the slow delay above)
#    Each failed attempt increments retry_count and keeps the row PENDING.
#    Watch it in the app log: "Kafka broker unavailable" errors will appear.

# 5. Check again — retry_count should have increased
docker exec -it postgres psql -U payments_user -d payments_db \
  -c "SELECT id, status, retry_count, sent_at FROM notification_outbox ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PENDING, retry_count=1 (or 2)

# 6. Unpause Kafka — the next scheduler tick will succeed
docker unpause kafka

# 7. Wait one more tick, then check — row is now SENT and message appears in the Kafka consumer terminal
docker exec -it postgres psql -U payments_user -d payments_db \
  -c "SELECT id, status, retry_count, sent_at FROM notification_outbox ORDER BY created_at DESC LIMIT 1;"
# Expected: status=SENT, sent_at populated
```

---

## 16. Inspect DB state

```bash
docker exec -it postgres psql -U payments_user -d payments_db
```

Useful queries:

```sql
-- Account balances
SELECT id, owner_name, balance, currency FROM accounts;

-- Transaction history
SELECT id, sender_account_id, receiver_account_id, amount, currency, status, idempotency_key
FROM transactions ORDER BY created_at DESC;

-- Outbox delivery status
SELECT id, transaction_id, status, retry_count, sent_at
FROM notification_outbox ORDER BY created_at DESC;
```

After a successful payment:
- `transactions.status` = `COMPLETED`
- `notification_outbox.status` = `SENT`, `sent_at` is populated

---

## 17. Swagger UI

Open in a browser:

```
http://localhost:8080/swagger-ui.html
```

All endpoints are documented and executable from the UI.

---

## 18. Run automated tests

No infrastructure needed — tests use embedded H2 and embedded Kafka:

```bash
mvn test
```
