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

## 7. Idempotency — duplicate request

Replay the same request with the same `idempotencyKey`:

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

Expected: HTTP `409 Conflict` — payment not applied a second time, no new Kafka message.

---

## 8. Insufficient funds

Charlie (300 EUR) tries to send more than he has:

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

## 9. Currency mismatch

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

## 10. Self-transfer

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

## 11. Account not found

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

## 12. Validation error

Missing required fields:

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{}'
```

Expected: HTTP `400 Bad Request` with field-level error details.

---

## 13. Inspect DB state

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

## 14. Swagger UI

Open in a browser:

```
http://localhost:8080/swagger-ui.html
```

All endpoints are documented and executable from the UI.

---

## 15. Run automated tests

No infrastructure needed — tests use embedded H2 and embedded Kafka:

```bash
mvn test
```
