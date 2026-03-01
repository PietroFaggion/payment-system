# Testbook

Step-by-step actions to manually exercise the payment system end to end.

> **Authentication**: all `/payments` endpoints require HTTP Basic authentication.
> Default credentials: `payments-user` / `payments-pass` (override with `SECURITY_USER` / `SECURITY_PASSWORD` env vars).
> The curl examples below use `-u payments-user:payments-pass`.
> Actuator health/info and Swagger UI are open without credentials.

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

Check it is up (no auth required):

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

## 5. Happy path - successful payment

Alice (EUR) sends 100 EUR to Charlie (EUR):

```bash
curl -i -X POST http://localhost:8080/payments \
  -u payments-user:payments-pass \
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
curl -u payments-user:payments-pass \
  http://localhost:8080/payments/{transactionId}
# Replace {transactionId} with the ID from the previous response
```

Expected: HTTP `200 OK` with full transaction details.

---

## 7. Idempotency - safe retry (same parameters)

Replay the exact same request (same key, same parameters) to simulate a network retry:

```bash
curl -i -X POST http://localhost:8080/payments \
  -u payments-user:payments-pass \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 1001,
    "receiverAccountId": 1003,
    "amount": 100.00,
    "currency": "EUR",
    "idempotencyKey": "test-happy-001"
  }'
```

Expected: HTTP `201 Created`,  returns the **same** `transactionId` as the first call. Balance unchanged (no second deduction). No new Kafka message.

---

## 8. Idempotency - key reuse with different parameters

Attempt to reuse the same key but with a different amount (fraudulent/misuse attempt):

```bash
curl -i -X POST http://localhost:8080/payments \
  -u payments-user:payments-pass \
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
  -u payments-user:payments-pass \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 1003,
    "receiverAccountId": 1001,
    "amount": 999.00,
    "currency": "EUR",
    "idempotencyKey": "test-funds-001"
  }'
```

Expected: HTTP `422 Unprocessable Entity`, no balance change, no Kafka message.

---

## 10. Currency mismatch

Alice (EUR account) sends with USD currency:

```bash
curl -i -X POST http://localhost:8080/payments \
  -u payments-user:payments-pass \
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
  -u payments-user:payments-pass \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 1001,
    "receiverAccountId": 1001,
    "amount": 50.00,
    "currency": "EUR",
    "idempotencyKey": "test-self-001"
  }'
```

Expected: HTTP `400 Bad Request`, sender and receiver must be different.

---

## 12. Account not found

```bash
curl -i -X POST http://localhost:8080/payments \
  -u payments-user:payments-pass \
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
  -u payments-user:payments-pass \
  -H "Content-Type: application/json" \
  -d '{}'
```

Expected: HTTP `400 Bad Request` with field-level error details.

---

## 14. Missing credentials

Omit the `-u` flag to verify the endpoint is protected:

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{}'
```

Expected: HTTP `401 Unauthorized`.

---

## 15. Observe the outbox pattern under Kafka failure

This step shows the payment succeeding immediately while Kafka is down, then the scheduler retrying and delivering once Kafka recovers.

**Tip:** the scheduler delay defaults to 5 s, which can be too fast to catch manually. Start the app with a slower delay so you have time to inspect the DB between runs:

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--app.outbox.scheduler.delay-ms=30000
```

**Steps:**

```bash
# 1. Pause Kafka (simulates a broker outage, the container is still "running" but not responding)
docker pause kafka

# 2. Create a payment, succeeds immediately (outbox decouples Kafka from the HTTP response)
curl -i -X POST http://localhost:8080/payments \
  -u payments-user:payments-pass \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountId": 1001,
    "receiverAccountId": 1003,
    "amount": 10.00,
    "currency": "EUR",
    "idempotencyKey": "test-outbox-demo"
  }'
# Expected: HTTP 201 Created, payment goes through despite Kafka being down

# 3. Check the outbox row, it should be PENDING (or PROCESSING during a scheduler tick)
docker exec -it postgres psql -U payments_user -d payments_db \
  -c "SELECT id, status, retry_count, sent_at FROM notification_outbox ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PENDING, retry_count=0 (before first scheduler run)

# 4. Wait for one or two scheduler ticks (~30 s if using the slow delay above)
#    Each failed attempt increments retry_count and keeps the row PENDING.
#    Watch it in the app log: "Kafka broker unavailable" errors will appear.

# 5. Check again, retry_count should have increased
docker exec -it postgres psql -U payments_user -d payments_db \
  -c "SELECT id, status, retry_count, sent_at FROM notification_outbox ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PENDING, retry_count=1 (or 2)

# 6. Unpause Kafka, the next scheduler tick will succeed
docker unpause kafka

# 7. Wait one more tick, then check, row is now SENT and message appears in the Kafka consumer terminal
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

Open in a browser (no login required):

```
http://localhost:8080/swagger-ui.html
```

All endpoints are documented and executable from the UI. Use the **Authorize** button (lock icon) and enter `payments-user` / `payments-pass` to authenticate requests within Swagger.

---

## 18. Run automated tests

No infrastructure needed, tests use embedded H2 and embedded Kafka:

```bash
mvn test
```

---

## 19. Concurrent request stress test (two-phase, 17 parallel requests)

This final test runs two concurrent batches back to back and verifies that every concurrency guarantee holds under mixed valid and invalid load.

### Phase 1 - 10 unique valid payments

All transfers are bidirectional across Alice (1001), Bob (1002), and Charlie (1003), hitting the pessimistic-lock ordering (ascending account ID) simultaneously.

| Check | What is validated |
|---|---|
| All 10 return HTTP 201 | No spurious errors under lock contention |
| Balance conserved | Total EUR unchanged (net effect = 0) |
| 10 COMPLETED in DB | No silent failures |

Net effect per account (all 10 succeed):

| Account | Net change |
|---|---|
| Alice (1001) | −135 EUR |
| Bob (1002) | +30 EUR |
| Charlie (1003) | +105 EUR |
| **System total** | **0 EUR** |

### Phase 2 - 7 concurrent requests: idempotency races + intentional failures

All 7 are fired simultaneously alongside each other.

**Idempotency group**, 4 requests share the key `test-concurrent-idem-01`:

| Request | Params | Expected |
|---|---|---|
| 3× Alice → Charlie, 30 EUR | identical | 201 (winner) or 409 (race loser) |
| 1× Alice → Charlie, **999 EUR** | same key, different amount | **409 Conflict** |

Invariant checked: **exactly 1 transaction** in DB for that key, no 5xx responses, at least one 201 and at least one 409 among the four.

**Failure group**, 3 requests that must be rejected:

| Request | Reason | Expected |
|---|---|---|
| Charlie → Alice, 9999 EUR | insufficient funds | 422 |
| Alice → Alice, 10 EUR | self-transfer | 400 |
| Account 9999 → Alice, 10 EUR | account not found | 404 |

Invariant checked: none of these create a transaction or modify any balance.

### Run the script

```bash
bash concurrent_stress_test.sh
```

The script:
1. Confirms the app is healthy and snapshots the total balance before starting
2. Fires Phase 1 (10 requests) in parallel, waits, then validates HTTP codes and DB state
3. Fires Phase 2 (7 requests) in parallel, waits, then validates each group independently
4. Runs global checks: balance conservation, no negative balances, outbox count matches COMPLETED count
5. Prints a final `ALL CONCURRENT-STRESS CHECKS PASSED` / `N CHECK(S) FAILED` verdict

### Expected output (happy path)

```
================================================================
  Concurrent Payment Stress Test - Phase 1 + Phase 2
================================================================

Total balance before test : 2550.0000 EUR

────────────────────────────────────────────────────────────────
  PHASE 1: 10 unique valid concurrent payments
────────────────────────────────────────────────────────────────

Firing 10 requests in parallel...
All Phase 1 requests returned.

--- Phase 1: HTTP response codes ---
  [PASS] test-concurrent-01  (1001 → 1003  50.00 EUR)  =>  HTTP 201
  ...
  [PASS] All 10 Phase 1 requests returned HTTP 201
  [PASS] All 10 Phase 1 transactions are in the DB

────────────────────────────────────────────────────────────────
  PHASE 2: 7 concurrent requests (idempotency races + failures)
────────────────────────────────────────────────────────────────

Firing 7 requests in parallel...
All Phase 2 requests returned.

--- Phase 2: Idempotency group (same key, 3 identical + 1 conflicting) ---
  [201] test-concurrent-idem-01 [valid-30]     30.00 EUR   =>  HTTP 201
  [409] test-concurrent-idem-01 [dup-30]       30.00 EUR   =>  HTTP 409 Conflict
  [409] test-concurrent-idem-01 [dup-30]       30.00 EUR   =>  HTTP 409 Conflict
  [409] test-concurrent-idem-01 [CONFLICT-999] 999.00 EUR  =>  HTTP 409 Conflict

  [PASS] At least one 201 in the idempotency group (a winner was created)
  [PASS] At least one 409 in the idempotency group (conflict/race properly rejected)
  [PASS] All idempotency-group responses are in {201, 409}, no server errors
  [PASS] Exactly 1 transaction in DB for test-concurrent-idem-01 (no duplicates)

--- Phase 2: Business-rule failure group ---
  [PASS] test-concurrent-funds-01   (Charlie → Alice 9999 EUR)  =>  HTTP 422 Insufficient funds
  [PASS] test-concurrent-self-01    (Alice → Alice  10 EUR)     =>  HTTP 400 Self-transfer
  [PASS] test-concurrent-notfound-01 (9999 → Alice  10 EUR)     =>  HTTP 404 Not Found
  [PASS] Failure group created 0 transactions in DB (correct)

────────────────────────────────────────────────────────────────
  GLOBAL CHECKS (both phases)
────────────────────────────────────────────────────────────────

  [PASS] Total EUR unchanged: 2550.0000 → 2550.0000
  [PASS] No account has a negative balance
  [PASS] Outbox entries (11) match COMPLETED transactions (11)

================================================================
  ALL CONCURRENT-STRESS CHECKS PASSED
================================================================
```

> **Note - idempotency group response split:**
> Under true concurrent load, **exactly one** request returns 201 (the one that wins the DB race
> and inserts the transaction) and the remaining three return 409. This happens because all requests
> arrive at the server at the same moment, race on the DB unique constraint, and only one INSERT
> succeeds, the others get a constraint violation before the service-level idempotency check can
> return an idempotent 201.
>
> If you see **more than one 201** (e.g. 2×201 + 2×409), it means the requests did not arrive truly
> simultaneously, bash spawns background processes one at a time in the loop, and on a fast machine
> the first curl can complete its full HTTP round-trip (connect → Spring → DB commit → response)
> before bash finishes spawning the remaining curls. When those later curls arrive, the transaction
> is already committed, params match, and the service correctly returns the idempotent 201.
>
> Both outcomes (1×201 or multiple 201s) are **correct**. The critical invariant, exactly 1
> transaction in the DB, holds in either case. For guaranteed simultaneous arrival use a
> `CyclicBarrier` (as the automated integration tests do) or GNU `parallel`. The bash script's
> value is end-to-end validation against real infrastructure, not a strict concurrency guarantee.

> **Note:** The balance figures above assume a clean database (seed data only).
> If prior testbook steps ran first, starting balances differ, but the script validates
> *delta conservation* (before vs after) so it passes regardless.

### Re-running the test

The script uses idempotency keys prefixed with `test-concurrent-`.
Delete those rows before re-running:

```bash
docker exec postgres psql -U payments_user -d payments_db -c "
  DELETE FROM notification_outbox
  WHERE transaction_id IN (
    SELECT id FROM transactions WHERE idempotency_key LIKE 'test-concurrent-%'
  );
  DELETE FROM transactions WHERE idempotency_key LIKE 'test-concurrent-%';
"
```
