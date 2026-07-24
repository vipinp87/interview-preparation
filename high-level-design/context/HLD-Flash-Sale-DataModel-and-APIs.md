# Flash Sale — Data Model & APIs

Companion to `HLD-Interview-Flash-Sale.pdf`. The transcript reasons through the *design* (CDN + waiting room, sharded atomic counters, two-phase reserve→pay, re-validate-before-charge, blast-radius isolation) but never writes down the **data model** or the **API contract** as artifacts. This fills both; `HLD-Flash-Sale-Architecture.png` / `.svg` carries the full visual architecture.

**One-line recap of the design:** a CDN plus a virtual waiting room absorb the ~170k/s read storm and flatten the ~65k/s write spike into an admit rate the backend can serve; sharded atomic Redis counters (pre-loaded with the 5,000 units) guarantee we never oversell, with a TTL-based `reservation:{userId}` hold implementing the second phase (reserve → pay → sold); payment is idempotent and re-validates the hold at charge time; a relational Orders DB behind Kafka is the async ACID source of truth; and the whole flash-sale service runs on its **own isolated cluster + datastore** so the drop can melt without touching the rest of the platform.

---

## 1. Data model

The system has **three stores**, each authoritative for a different thing — all living inside the **isolated flash-sale cluster**:

| Store | Authoritative for | Consistency |
|-------|-------------------|-------------|
| **Redis cluster** (sharded) | live scarcity — remaining stock + the reserve→pay→sold hold | strong / linearizable (per key) |
| **Kafka** (`order-events`) | the durable event log / replay for order fulfilment | durable, ordered per partition |
| **PostgreSQL / Aurora** | the permanent, ACID record of every order (money + shipment) | ACID (eventually written) |

### 1.1 Redis keyspace (the hot path — the *only* thing on the critical path)

| Key | Type | Init / Value | Purpose | Op on hot path |
|-----|------|--------------|---------|----------------|
| `stock:{k}` &nbsp;`k = 0..9` | Integer (counter) | each set to `500`; Σ = 5,000 | sharded inventory — one atomic counter per shard | `DECR` (reserve if result ≥ 0) |
| `reserved:{userId}` | String flag | `SET ... NX` | per-user dedup guard (one unit per user) | `SET NX` (proceed only if newly set) |
| `reservation:{userId}` | String, **`EX 180`** | `{shard, ts}` | **the 2nd-phase hold** — the checkout window; self-expiring | `SET … EX`; `GET` to re-validate; `DEL` on sold |
| `global:sold_out` | String flag | `"0"` → `"1"` when all shards empty | edge fast-path: reject at gateway without touching Redis | read at edge; set by aggregator |
| `sold:count` | Integer | `5000 − Σ stock:{k}`, refreshed every 1–2s | cosmetic "3,201 sold" counter (approximate) | read-only (never contends with real inventory) |

**Key design choices baked into the keyspace**
- **Two keys per reservation, not one.** `reserved:{userId}` is the *dedup guard* (idempotent "this user already got one"); `reservation:{userId}` is the *self-expiring hold* whose TTL == the checkout window. Splitting them lets the hold expire and return stock while still remembering the user reserved.
- **`SET NX` before `DECR`.** Dedup first, decrement second — a retry is free and a double-tap can't grab two units.
- **The TTL is the whole point of the second phase.** Without it, users who reserve but never pay permanently lock units and we "sell out" with unpaid holds — the classic ticketing bug. Redis key expiry returns the unit to the pool within milliseconds, with **no polling sweeper on the hot row** (see the transcript's §10 deep-dive on why a DB decrement can't do this).
- **Shard the counter (10 × 500)** to spread the 65k/s write spike across keys, with **sibling-probing** near the end so all 5,000 truly sell.
- **On `SOLD_OUT` we `DEL reserved:{userId}`** so a user who tapped into an empty shard hasn't burned their one chance.

#### Atomic sibling-probe (Lua) — check-and-decrement in one shot, no racy flicker
```lua
-- KEYS = a bounded handful of candidate stock shards to try
-- returns the 1-based index of the shard reserved from, or -1 if all empty
for i = 1, #KEYS do
  local v = tonumber(redis.call('GET', KEYS[i]) or '0')
  if v > 0 then
    redis.call('DECR', KEYS[i])
    return i            -- reserved atomically; no "DECR → see -1 → INCR back" race
  end
end
return -1
```

### 1.2 Kafka — topic `order-events`

- **Durable, replicated, retained** so it doubles as the replayable order/fulfilment stream. Partitioned (e.g. by `orderId`) for parallel draining.
- **Produced by the flash-sale service, not by Redis.** After Redis reserves and the Payment Service confirms the charge, *the service* — with payment success as the commit point — produces `order-confirmed` (at-least-once) before returning. So an order is never "charged in the gateway but has no record." (Nothing wires Redis directly to Kafka.)

```jsonc
// message value (key = orderId)
{
  "orderId":   "ord_01J8ZT...",
  "userId":    "u_88213771",
  "sku":       "iphone-2026",
  "shard":     4,                     // which stock shard it came from (reconciliation)
  "amount":    99900,                 // cents
  "currency":  "USD",
  "paymentRef":"pi_3Q...gateway",
  "confirmedAt":"2026-07-23T12:00:01.412Z",
  "idempotencyKey": "u_88213771:iphone-2026",
  "eventId":   "01J...ULID"           // idempotency key for the consumer upsert
}
```

### 1.3 PostgreSQL / Aurora — source of truth (written async by consumers)

```sql
-- Every confirmed order. 5,000 rows is trivial.
CREATE TABLE orders (
    order_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id        TEXT        NOT NULL,
    sku            TEXT        NOT NULL,
    shard          SMALLINT    NOT NULL,
    amount         INT         NOT NULL,
    currency       TEXT        NOT NULL DEFAULT 'USD',
    status         TEXT        NOT NULL DEFAULT 'CONFIRMED',  -- CONFIRMED | SHIPPED | REFUNDED
    payment_ref    TEXT        NOT NULL,                      -- gateway transaction id
    idempotency_key TEXT       NOT NULL,                      -- /checkout Idempotency-Key
    confirmed_at   TIMESTAMPTZ NOT NULL,
    event_id       TEXT        NOT NULL,                      -- Kafka eventId, idempotent upsert
    CONSTRAINT uq_orders_user_sku   UNIQUE (user_id, sku),    -- backstop: one unit per user per drop
    CONSTRAINT uq_orders_idem       UNIQUE (idempotency_key), -- makes /checkout idempotent
    CONSTRAINT uq_orders_event      UNIQUE (event_id)         -- makes the consumer write idempotent
);
CREATE INDEX idx_orders_status ON orders (status, confirmed_at);

-- Idempotency ledger for /checkout (so a retried Pay returns the SAME result).
CREATE TABLE idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    user_id         TEXT        NOT NULL,
    result_json     JSONB       NOT NULL,     -- the original response to replay
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Sale configuration / counters snapshot (single row).
CREATE TABLE sale (
    sale_id      TEXT PRIMARY KEY,
    sku          TEXT        NOT NULL,
    total_units  INT         NOT NULL,   -- 5,000
    shard_count  INT         NOT NULL,   -- 10
    opens_at     TIMESTAMPTZ NOT NULL,
    checkout_ttl_seconds INT NOT NULL DEFAULT 180
);
```

The consumer does an **idempotent upsert** keyed by `event_id` (`INSERT ... ON CONFLICT (event_id) DO NOTHING`), so at-least-once delivery / replay never creates duplicate orders. `UNIQUE(user_id, sku)` and `UNIQUE(idempotency_key)` are *correctness backstops* — the primary scarcity guard is Redis `DECR`/`SET NX`, which keeps the 65k/s uniqueness check off the database.

**Reserve-vs-sold gap:** a `RESERVED` unit is neither available nor sold — deliberately treated as "spent" for availability (so we never oversell) but not yet revenue. TTL expiry reconciles the gap by returning abandoned holds. **Reconciliation invariant:** `COUNT(orders WHERE status != 'REFUNDED') ≤ 5,000`, and `sold = 5,000 − Σ stock:{k} − (live RESERVED holds)`.

**Blast-radius isolation:** every store above runs in the **flash-sale cluster's own datastore**, separate from the main commerce stack. If the sale melts under load, the rest of the catalog is untouched — the isolation requirement made concrete.

---

## 2. API design

Reads are cacheable and served from the CDN. Write traffic passes the waiting room first and carries an **`Idempotency-Key: {userId}:{sku}`** so a retried/timed-out tap is free — it resolves to the same outcome and never charges twice or reserves two units.

### 2.1 `GET /v1/sale/{sku}` — product page / status (cacheable, approximate)

```jsonc
// 200 — fully cached at the CDN (1–2s TTL); ~0 origin load under the read storm
{ "sku": "iphone-2026", "status": "LIVE", "opensAt": "2026-07-23T12:00:00Z",
  "soldOut": false, "sold": 3201, "approximate": true }
```
Served entirely from the CDN cache. `sold` reads the pre-computed `sold:count` — **never** sums the live stock shards on the request path.

### 2.2 `POST /v1/reserve` — reserve one unit (the hot endpoint, phase 1)

Requires a valid **signed waiting-room token**. Idempotent per user.

**Request**
```http
POST /v1/reserve
Idempotency-Key: u_88213771:iphone-2026
Authorization: Bearer <waiting-room queue token>
Content-Type: application/json

{ "userId": "u_88213771", "sku": "iphone-2026" }
```

**Server logic (hot path):**
1. `SET reserved:{userId} 1 NX` → if it already exists → **`ALREADY_RESERVED`** (idempotent replay or a second tap).
2. Else `DECR stock:{userId % 10}` → if ≥ 0 → reserved.
3. Else bounded sibling-probe over the remaining shards (Lua) → if one found → reserved.
4. Else `DEL reserved:{userId}` (release the guard) → **`SOLD_OUT`**.
5. On reserve: write `reservation:{userId} EX 180` (start the checkout window) and return `RESERVED` + a checkout deadline.

**Responses** (HTTP `200` with a discriminated `result`; status codes in parens for REST purists):
```jsonc
// RESERVED  (200 / 201)
{ "result": "RESERVED", "reservationId": "rsv_9f2a", "sku": "iphone-2026",
  "expiresAt": "2026-07-23T12:03:00Z", "message": "Reserved — check out within 3 minutes" }

// ALREADY_RESERVED  (200 — idempotent replay or a genuine second tap)
{ "result": "ALREADY_RESERVED", "reservationId": "rsv_9f2a", "expiresAt": "..." }

// SOLD_OUT  (200, or 410 Gone)
{ "result": "SOLD_OUT", "message": "Sorry, sold out" }
```

| Failure | Status | Meaning |
|---------|--------|---------|
| No / invalid waiting-room token | `401` | must pass through the waiting room first |
| Rate limit (per IP / device) | `429` | bot / hammering; `Retry-After` header |
| Backend saturated (admission control) | `503` | `{ "result": "BUSY" }` — graceful valve |
| Sale not open / closed | `409` | outside `[opens_at, …]` |

> **Fast-path SOLD_OUT:** once `global:sold_out = 1`, the gateway/CDN short-circuits and returns `SOLD_OUT` at the edge **without touching Redis** — the graceful-degradation valve for the 1.995M who lose.

### 2.3 `POST /v1/checkout` — pay & confirm (phase 2, re-validate before charge)

The one place a TTL race could otherwise oversell or charge-without-item. **Order of operations gates the charge on the hold.**

**Request**
```http
POST /v1/checkout
Idempotency-Key: u_88213771:iphone-2026
Authorization: Bearer <session>

{ "userId": "u_88213771", "reservationId": "rsv_9f2a", "payment": { ... } }
```

**Server logic:**
1. If this `Idempotency-Key` is already in the ledger → **replay the stored result** (no re-charge).
2. `GET reservation:{userId}` — if missing / not owned by this user → **`RESERVATION_EXPIRED`** (no money moves). *(Optionally promote to a `PAYMENT_IN_PROGRESS` state with a fresh TTL so an actively-paying user's hold can't expire under them.)*
3. Only if the hold is still ours: `paymentSvc.capture(idempotencyKey)`.
4. On success: emit `order-confirmed` to Kafka (payment success = the commit point), store the idempotency result, `DEL reservation:{userId}` + mark the shard's unit `SOLD`; return confirmation. On failure: `INCR stock:{shard}` (release), tell the user.
5. **Safety net:** if the charge landed but the hold had flipped in the residual window → **auto-refund** + notify (never silently keep the money).

**Responses:**
```jsonc
// CONFIRMED  (200)
{ "result": "CONFIRMED", "orderId": "ord_01J8ZT...", "message": "Order confirmed! 🎉" }

// RESERVATION_EXPIRED  (200, or 409) — hold lapsed; NEVER charged
{ "result": "RESERVATION_EXPIRED", "message": "Your hold expired — the unit was released" }

// PAYMENT_FAILED  (402) — hold released, unit returned to pool
{ "result": "PAYMENT_FAILED", "message": "Payment failed — please try again" }
```

> **Why re-check right before capture:** the reservation TTL, the user's typing speed, and the gateway's response time are three independent clocks. The invariant is **never take money without a freshly validated hold** — re-checking `reservation:{userId} == userId` as the first step of `/checkout` is what actually prevents oversell and charge-without-item; a longer TTL and a payment-start renewal only make the race rarer. The unique constraint is the last-line DB backstop, but it should never be the thing that catches a bad charge.

### 2.4 `GET /v1/reservation/status?userId=` — check my reservation (idempotent read)

```jsonc
// 200
{ "userId": "u_88213771", "status": "RESERVED", "expiresAt": "2026-07-23T12:03:00Z" }
// or { "status": "SOLD" } / { "status": "NONE" } / { "status": "EXPIRED" }
```
Reads Redis (`reservation:{userId}`), falls back to Postgres (`orders`) for post-sale lookups. Kept off the inventory keys.

### 2.5 Admin / ops (internal, authenticated)

| Endpoint | Purpose |
|----------|---------|
| `GET /admin/reconcile` | run/report the `orders` vs Redis stock reconciliation (`orders ≤ 5,000`) |
| `GET /admin/inventory` | per-shard remaining + `sold_out` flag for the on-call dashboard |
| `POST /admin/sale/open` \| `/close` | flip the sale window manually |

---

## 3. Request lifecycle (happy path)

The **flash-sale service is the orchestrator**: it makes synchronous request/response calls to Redis (reserve) and the Payment Service (capture), and it is the Kafka **producer**. Redis never talks to Kafka. Everything below runs inside the **isolated flash-sale cluster**.

```
Client → CDN (read storm absorbed) → Waiting room (signed token) → Gateway (rate-limit, admit)
   → Flash-sale service
        ├─(1) ⇄ Redis:  SET NX  →  DECR stock  →  SET reservation EX 180   ...... sync → RESERVED
        │        "reserved, pay within 3 min"                                ...... user enters card
        ├─(2) ⇄ Redis:  GET reservation == userId (re-validate)             ...... BEFORE charge
        ├─(3) → Payment Service: capture(idempotencyKey)                    ...... only if hold still ours
        ├─(4) → returns "order confirmed" to the user                       ...... user is done here
        └─(5) → Kafka (order-events)                                        ...... async, produced BY the server
                    → Consumer workers
                          ├→ Orders DB (ACID source of truth, idempotent upsert)
                          └→ Email/SMS "order confirmed" + fulfilment
```

The user gets **"order confirmed"** the instant payment + reservation validate (step 3→4). The durable order write and notification (step 5) are produced to Kafka and catch up behind it — they never touch the user's latency, and a slow warehouse/SMS provider never gates checkout. The whole reserve path is memory-speed: Redis atomic ops, no synchronous DB write until the rarer, latency-tolerant order persist.
