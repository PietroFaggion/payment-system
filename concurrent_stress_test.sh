#!/usr/bin/env bash
# concurrent_stress_test.sh
#
# Two-phase concurrent stress test for the payment system.
#
# PHASE 1 - 10 unique valid payments fired simultaneously
#   Exercises deadlock-prevention (bidirectional lock ordering) and
#   proves that balance is conserved under maximum lock contention.
#
# PHASE 2 - 7 concurrent requests mixing idempotency races + intentional failures
#   4 requests share the same idempotency key:
#     · 3 identical (same params)  → all return 201 or 409; exactly 1 tx created
#     · 1 conflicting (different amount) → must return 409
#   3 requests exercise business-rule rejections under concurrent load:
#     · Insufficient funds         → 422
#     · Self-transfer              → 400
#     · Non-existent account       → 404
#   Validates that no 5xx occurs, no duplicate transactions are created,
#   and balance conservation still holds across both phases.
#
# Prerequisites:
#   - App running on localhost:8080
#   - PostgreSQL reachable via: docker exec postgres ...
#
# Usage:
#   bash concurrent_stress_test.sh
#
# To re-run cleanly, delete previous rows first:
#   docker exec postgres psql -U payments_user -d payments_db -c "
#     DELETE FROM notification_outbox WHERE transaction_id IN
#       (SELECT id FROM transactions WHERE idempotency_key LIKE 'test-concurrent-%');
#     DELETE FROM transactions WHERE idempotency_key LIKE 'test-concurrent-%';"

set -uo pipefail

BASE_URL="http://localhost:8080"
AUTH="payments-user:payments-pass"

FAILED=0   # global failure counter

# ── helpers ────────────────────────────────────────────────────────────────────

pass() { echo "  [PASS] $*"; }
fail() { echo "  [FAIL] $*"; FAILED=$((FAILED + 1)); }

db() {   # db <sql>  - runs a query and returns trimmed output
  docker exec postgres psql -U payments_user -d payments_db -t -c "$1" | tr -d '[:space:]'
}

db_pretty() {  # db_pretty <sql>  - runs a query with table formatting
  docker exec postgres psql -U payments_user -d payments_db -c "$1"
}

fire() {
  # fire <index> <tmpdir> <sender> <receiver> <amount> <currency> <key>
  local idx=$1 tmp=$2 sender=$3 receiver=$4 amount=$5 currency=$6 key=$7
  curl -s \
    -X POST "$BASE_URL/payments" \
    -u "$AUTH" \
    -H "Content-Type: application/json" \
    -d "{\"senderAccountId\":$sender,\"receiverAccountId\":$receiver,\"amount\":$amount,\"currency\":\"$currency\",\"idempotencyKey\":\"$key\"}" \
    -o "$tmp/resp_${idx}.json" \
    -w "%{http_code}" \
    > "$tmp/status_${idx}.txt" &
}

status_of() { cat "$1/status_${2}.txt" 2>/dev/null || echo "ERR"; }

# ── preflight ──────────────────────────────────────────────────────────────────

echo "================================================================"
echo "  Concurrent Payment Stress Test - Phase 1 + Phase 2"
echo "================================================================"
echo

HEALTH=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/actuator/health")
if [ "$HEALTH" != "200" ]; then
  echo "[ERROR] App is not running (GET /actuator/health => $HEALTH). Start with: mvn spring-boot:run"
  exit 1
fi

TOTAL_BEFORE=$(db "SELECT COALESCE(SUM(balance),0) FROM accounts WHERE id IN (1001,1002,1003);")
echo "Total balance before test : ${TOTAL_BEFORE} EUR"
echo

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1 - 10 unique valid concurrent payments
#
# All transfers are bidirectional across Alice(1001) / Bob(1002) / Charlie(1003),
# hitting the pessimistic-lock ordering (ascending account ID) simultaneously.
#
# Net effect per account (all 10 succeed):
#   Alice  (1001) : −50−50−30−30−40+20+20+25     = −135 EUR
#   Bob    (1002) : +30+30−25−15+10               =  +30 EUR
#   Charlie(1003) : +50+50+40−20−20+15−10         = +105 EUR
#   System total  :                                    0 EUR
# ══════════════════════════════════════════════════════════════════════════════
echo "────────────────────────────────────────────────────────────────"
echo "  PHASE 1: 10 unique valid concurrent payments"
echo "────────────────────────────────────────────────────────────────"
echo

#         idx  sender  receiver  amount    currency  idempotency-key
P1=(
  "0  1001 1003 50.00  EUR test-concurrent-01"
  "1  1001 1003 50.00  EUR test-concurrent-02"
  "2  1001 1002 30.00  EUR test-concurrent-03"
  "3  1001 1002 30.00  EUR test-concurrent-04"
  "4  1003 1001 20.00  EUR test-concurrent-05"
  "5  1003 1001 20.00  EUR test-concurrent-06"
  "6  1002 1001 25.00  EUR test-concurrent-07"
  "7  1002 1003 15.00  EUR test-concurrent-08"
  "8  1001 1003 40.00  EUR test-concurrent-09"
  "9  1003 1002 10.00  EUR test-concurrent-10"
)

pids1=()
echo "Firing ${#P1[@]} requests in parallel..."
for entry in "${P1[@]}"; do
  read -r idx sender receiver amount currency key <<< "$entry"
  fire "$idx" "$TMP" "$sender" "$receiver" "$amount" "$currency" "$key"
  pids1+=($!)
done
for pid in "${pids1[@]}"; do wait "$pid"; done
echo "All Phase 1 requests returned."
echo

echo "--- Phase 1: HTTP response codes ---"
P1_FAIL=0
for entry in "${P1[@]}"; do
  read -r idx sender receiver amount currency key <<< "$entry"
  s=$(status_of "$TMP" "$idx")
  if [ "$s" = "201" ]; then
    pass "$key  ($sender → $receiver  ${amount} ${currency})  =>  HTTP $s"
  else
    body=$(cat "$TMP/resp_${idx}.json" 2>/dev/null || echo "<no body>")
    fail "$key  ($sender → $receiver  ${amount} ${currency})  =>  HTTP $s | $body"
    P1_FAIL=$((P1_FAIL + 1))
  fi
done
echo
[ "$P1_FAIL" -eq 0 ] \
  && pass "All ${#P1[@]} Phase 1 requests returned HTTP 201" \
  || fail "$P1_FAIL / ${#P1[@]} Phase 1 request(s) did not return HTTP 201"

echo
echo "--- Phase 1: DB - transactions ---"
db_pretty "SELECT id, sender_account_id AS sender, receiver_account_id AS receiver,
                  amount, currency, status, idempotency_key
           FROM transactions
           WHERE idempotency_key LIKE 'test-concurrent-0%'
              OR idempotency_key LIKE 'test-concurrent-1%'
           ORDER BY idempotency_key;"

P1_COMPLETED=$(db "SELECT COUNT(*) FROM transactions
                   WHERE idempotency_key LIKE 'test-concurrent-0%'
                      OR idempotency_key LIKE 'test-concurrent-1%';")
[ "$P1_COMPLETED" = "10" ] \
  && pass "All 10 Phase 1 transactions are in the DB" \
  || fail "Only $P1_COMPLETED / 10 Phase 1 transactions found"

FAILED=$((FAILED + P1_FAIL))

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 - 7 concurrent requests: idempotency races + intentional failures
#
# Idempotency group (indices 0-3, all same key "test-concurrent-idem-01"):
#   idx 0-2: Alice→Charlie  30 EUR  (identical params - idempotent duplicates)
#   idx 3:   Alice→Charlie 999 EUR  (same key, DIFFERENT amount - conflict)
#   → Exactly ONE transaction must be created; all responses in {201, 409}
#   → At least one 201 (the winner) and at least one 409 (the conflict)
#
# Failure group (indices 4-6):
#   idx 4: Charlie→Alice 9999 EUR  (insufficient funds  → 422)
#   idx 5: Alice→Alice   10 EUR   (self-transfer        → 400)
#   idx 6: Account 9999→Alice      (account not found   → 404)
#   → None of these may create a transaction or modify any balance
# ══════════════════════════════════════════════════════════════════════════════
echo
echo "────────────────────────────────────────────────────────────────"
echo "  PHASE 2: 7 concurrent requests (idempotency races + failures)"
echo "────────────────────────────────────────────────────────────────"
echo

TMP2=$(mktemp -d)
trap 'rm -rf "$TMP" "$TMP2"' EXIT

#         idx  sender  receiver   amount    currency  idempotency-key
P2=(
  # ---- idempotency group (same key, 3 identical + 1 conflicting) ----
  "0  1001 1003   30.00  EUR test-concurrent-idem-01"   # valid  → 201
  "1  1001 1003   30.00  EUR test-concurrent-idem-01"   # dup    → 201 or 409
  "2  1001 1003   30.00  EUR test-concurrent-idem-01"   # dup    → 201 or 409
  "3  1001 1003  999.00  EUR test-concurrent-idem-01"   # CONFLICT (diff amount) → 409
  # ---- intentional business-rule failures ----
  "4  1003 1001 9999.00  EUR test-concurrent-funds-01"  # insufficient funds → 422
  "5  1001 1001   10.00  EUR test-concurrent-self-01"   # self-transfer      → 400
  "6  9999 1001   10.00  EUR test-concurrent-notfound-01" # account not found → 404
)

pids2=()
echo "Firing ${#P2[@]} requests in parallel..."
for entry in "${P2[@]}"; do
  read -r idx sender receiver amount currency key <<< "$entry"
  fire "$idx" "$TMP2" "$sender" "$receiver" "$amount" "$currency" "$key"
  pids2+=($!)
done
for pid in "${pids2[@]}"; do wait "$pid"; done
echo "All Phase 2 requests returned."
echo

# ── Phase 2: idempotency group (indices 0-3) ──────────────────────────────────
echo "--- Phase 2: Idempotency group (same key, 3 identical + 1 conflicting) ---"
IDEM_201=0
IDEM_409=0
IDEM_UNEXPECTED=0
IDEM_LABELS=("valid-30"  "dup-30"  "dup-30"  "CONFLICT-999")

for idx in 0 1 2 3; do
  s=$(status_of "$TMP2" "$idx")
  label="${IDEM_LABELS[$idx]}"
  amount_label=$([ "$idx" -lt 3 ] && echo "30.00 EUR" || echo "999.00 EUR")
  case "$s" in
    201) echo "  [201] test-concurrent-idem-01 [$label]  $amount_label  =>  HTTP 201"
         IDEM_201=$((IDEM_201 + 1)) ;;
    409) echo "  [409] test-concurrent-idem-01 [$label]  $amount_label  =>  HTTP 409 Conflict"
         IDEM_409=$((IDEM_409 + 1)) ;;
    *)   body=$(cat "$TMP2/resp_${idx}.json" 2>/dev/null || echo "<no body>")
         echo "  [ERR] test-concurrent-idem-01 [$label]  $amount_label  =>  HTTP $s | $body"
         IDEM_UNEXPECTED=$((IDEM_UNEXPECTED + 1)) ;;
  esac
done
echo

# At least one 201 (a winner was created)
[ "$IDEM_201" -ge 1 ] \
  && pass "At least one 201 in the idempotency group (a winner was created)" \
  || fail "No 201 in the idempotency group - transaction may not have been created"

# At least one 409 (the conflict or a race loser was rejected)
[ "$IDEM_409" -ge 1 ] \
  && pass "At least one 409 in the idempotency group (conflict/race properly rejected)" \
  || fail "No 409 in the idempotency group - idempotency enforcement may have failed"

# No unexpected status codes (no 5xx, no 422, no 400)
[ "$IDEM_UNEXPECTED" -eq 0 ] \
  && pass "All idempotency-group responses are in {201, 409} - no server errors" \
  || fail "$IDEM_UNEXPECTED unexpected status code(s) in idempotency group"

# Exactly 1 transaction in DB for this key - the most critical invariant
echo
echo "--- Phase 2: DB - idempotency group transaction count ---"
IDEM_TX_COUNT=$(db "SELECT COUNT(*) FROM transactions WHERE idempotency_key = 'test-concurrent-idem-01';")
[ "$IDEM_TX_COUNT" = "1" ] \
  && pass "Exactly 1 transaction in DB for test-concurrent-idem-01 (no duplicates)" \
  || fail "Expected 1 transaction for test-concurrent-idem-01, found $IDEM_TX_COUNT"

db_pretty "SELECT id, sender_account_id AS sender, receiver_account_id AS receiver,
                  amount, currency, status, idempotency_key
           FROM transactions
           WHERE idempotency_key = 'test-concurrent-idem-01';"

# ── Phase 2: failure group (indices 4-6) ──────────────────────────────────────
echo
echo "--- Phase 2: Business-rule failure group ---"

# idx 4 - insufficient funds
s4=$(status_of "$TMP2" "4")
[ "$s4" = "422" ] \
  && pass "test-concurrent-funds-01 (Charlie → Alice 9999 EUR)  =>  HTTP 422 Insufficient funds" \
  || fail "test-concurrent-funds-01 expected HTTP 422, got $s4"

# idx 5 - self-transfer
s5=$(status_of "$TMP2" "5")
[ "$s5" = "400" ] \
  && pass "test-concurrent-self-01  (Alice → Alice  10 EUR)     =>  HTTP 400 Self-transfer" \
  || fail "test-concurrent-self-01  expected HTTP 400, got $s5"

# idx 6 - account not found
s6=$(status_of "$TMP2" "6")
[ "$s6" = "404" ] \
  && pass "test-concurrent-notfound-01 (9999 → Alice  10 EUR)   =>  HTTP 404 Not Found" \
  || fail "test-concurrent-notfound-01 expected HTTP 404, got $s6"

# Failure group must not have created any transactions
echo
echo "--- Phase 2: DB - failure group must produce zero transactions ---"
FAIL_TX=$(db "SELECT COUNT(*) FROM transactions
              WHERE idempotency_key IN (
                'test-concurrent-funds-01',
                'test-concurrent-self-01',
                'test-concurrent-notfound-01'
              );")
[ "$FAIL_TX" = "0" ] \
  && pass "Failure group created 0 transactions in DB (correct)" \
  || fail "Failure group created $FAIL_TX unexpected transaction(s) in DB"

# ══════════════════════════════════════════════════════════════════════════════
# GLOBAL CHECKS (across both phases)
# ══════════════════════════════════════════════════════════════════════════════
echo
echo "────────────────────────────────────────────────────────────────"
echo "  GLOBAL CHECKS (both phases)"
echo "────────────────────────────────────────────────────────────────"

echo
echo "--- Account balances after both phases ---"
db_pretty "SELECT id, owner_name, balance, currency
           FROM accounts
           WHERE id IN (1001,1002,1003)
           ORDER BY id;"

echo "--- Balance conservation ---"
TOTAL_AFTER=$(db "SELECT COALESCE(SUM(balance),0) FROM accounts WHERE id IN (1001,1002,1003);")
[ "$TOTAL_BEFORE" = "$TOTAL_AFTER" ] \
  && pass "Total EUR unchanged: ${TOTAL_BEFORE} → ${TOTAL_AFTER}" \
  || fail "Balance mismatch!  Before: ${TOTAL_BEFORE}  After: ${TOTAL_AFTER}"

echo
echo "--- Negative balance safety check ---"
NEGATIVE=$(db "SELECT COUNT(*) FROM accounts WHERE balance < 0;")
[ "$NEGATIVE" = "0" ] \
  && pass "No account has a negative balance" \
  || fail "$NEGATIVE account(s) have a negative balance - pessimistic lock may have failed"

echo
echo "--- Outbox entries (one per COMPLETED transaction; expect SENT after ~5 s) ---"
db_pretty "SELECT o.id, t.idempotency_key, o.status, o.retry_count, o.sent_at
           FROM notification_outbox o
           JOIN transactions t ON t.id = o.transaction_id
           WHERE t.idempotency_key LIKE 'test-concurrent-%'
           ORDER BY t.idempotency_key;"

COMPLETED_TOTAL=$(db "SELECT COUNT(*) FROM transactions
                      WHERE idempotency_key LIKE 'test-concurrent-%'
                        AND status = 'COMPLETED';")
OUTBOX_TOTAL=$(db "SELECT COUNT(*)
                   FROM notification_outbox o
                   JOIN transactions t ON t.id = o.transaction_id
                   WHERE t.idempotency_key LIKE 'test-concurrent-%';")
[ "$OUTBOX_TOTAL" = "$COMPLETED_TOTAL" ] \
  && pass "Outbox entries ($OUTBOX_TOTAL) match COMPLETED transactions ($COMPLETED_TOTAL)" \
  || fail "Outbox count ($OUTBOX_TOTAL) ≠ COMPLETED count ($COMPLETED_TOTAL)"

# ══════════════════════════════════════════════════════════════════════════════
# FINAL VERDICT
# ══════════════════════════════════════════════════════════════════════════════
echo
echo "================================================================"
if [ "$FAILED" -eq 0 ]; then
  echo "  ALL CONCURRENT-STRESS CHECKS PASSED"
else
  echo "  $FAILED CHECK(S) FAILED - review the output above"
fi
echo "================================================================"

[ "$FAILED" -eq 0 ]
