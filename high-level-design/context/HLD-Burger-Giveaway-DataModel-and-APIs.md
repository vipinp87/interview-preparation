# 10-Million Burger Giveaway — Data Model & APIs

Companion to `HLD-Interview-Burger-Giveaway.pdf`. This fills the two gaps that PDF left open: the **data model** (Redis keyspace, Postgres schema, Kafka message) and the **API contract**. See `HLD-Burger-Giveaway-Architecture.png` / `.svg` for the full visual architecture.

**One-line recap of the design:** Redis is the fast, atomic, shardable front line that guarantees we never oversell and never double-grant; Postgres is the durable source of truth written asynchronously behind Kafka; a virtual waiting room + edge rate-limiting flatten the ~670k req/s spike so the backend only ever sees a rate it can serve.

---

## 1. Data model

The system has **three stores**, each authoritative for a different thing:

| Store | Authoritative for | Consistency |
|-------|-------------------|-------------|
| **Redis cluster** | live scarcity — remaining count & per-user "has claimed?" | strong / linearizable (per key) |
| **Kafka** (`burger-grants`) | the durable event log / replay + audit stream | durable, ordered per `userId` |
| **PostgreSQL** | the permanent, queryable record of every grant | ACID (eventually written) |

### 1.1 Redis keyspace (the hot path — the *only* thing on the critical path)

| Key | Type | Init / Value | Purpose | Op on hot path |
|-----|------|--------------|---------|----------------|
| `bucket:{k}` &nbsp;`k = 0..99` | Integer (counter) | each set to `100_000`; Σ = 10,000,000 | sharded inventory — one atomic counter per shard | `DECR` (grant if result ≥ 0) |
| `claimed:{userId}` | String flag | `SET ... NX` → stores claim code or `"1"` | per-user dedup guard (exactly-once) | `SET NX` (proceed only if newly set) |
| `active_buckets` | Set | member per bucket id still > 0 | steer empty-bucket probes to buckets that have stock | `SREM` when a bucket hits 0; `SRANDMEMBER` to pick |
| `global:sold_out` | String flag | `"0"` → `"1"` when all buckets empty | edge fast-path: reject at gateway without touching buckets | read at edge; set by aggregator |
| `promo:phase` | String | `bulk` → `tail` | switch from sharded counters to single tail counter | read on claim; flipped by aggregator |
| `tail:remaining` | Integer | populated at phase flip = Σ remaining | single global counter that drains the last stock exactly | `DECR` (only in tail phase) |
| `display:remaining` | Integer | refreshed every 1–2s | cosmetic homepage counter (approximate) | read-only (never contends with real inventory) |

**Key design choices baked into the keyspace**
- **Shard by `userId`** for both the guard (`claimed:{userId}`) and the bucket (`userId % 100`) — users spread across the keyspace, so there is no single hot key.
- **`SET NX` before `DECR`.** Dedup first, decrement second. This is what makes a retry free and prevents a double-grant.
- **On `SOLD_OUT` we `DEL claimed:{userId}`** so a user who tapped into a sold-out shard hasn't burned their one chance (alternatively: mark them `eligible-but-sold-out`).
- **Redis replicas** per shard; failover promotes a replica. Sibling-probing routes around a briefly-down shard; reconciliation recovers any stranded count.

#### Atomic sibling-probe (Lua) — check-and-decrement in one shot, no racy flicker
```lua
-- KEYS = a bounded handful (3–5) of candidate bucket keys to try
-- returns the 1-based index of the bucket granted from, or -1 if all empty
for i = 1, #KEYS do
  local v = tonumber(redis.call('GET', KEYS[i]) or '0')
  if v > 0 then
    redis.call('DECR', KEYS[i])
    return i            -- granted atomically; no "DECR → see -1 → INCR back" race
  end
end
return -1
```

### 1.2 Kafka — topic `burger-grants`

- **Partitioned by `userId`** (per-user ordering), durable, replicated, **retained** so it doubles as the replayable audit stream.
- **Produced by the stateless claim server, not by Redis.** Redis is a passive datastore: the claim server calls it synchronously (`SET NX` → `DECR`), and *the server* — after Redis returns `GRANTED` — produces the grant event to Kafka (at-least-once) before returning the code. So a grant is never "spent in Redis but has no record." (Nothing wires Redis directly to Kafka; that would require keyspace-notifications + a connector, which this design does not use.)

```jsonc
// message value (key = userId)
{
  "userId":   "u_88213771",
  "code":     "BRGR-7F3A-9K2Q",   // the claim code handed to the user
  "bucketId": 42,                  // which shard it came from (for reconciliation)
  "grantedAt": "2026-07-23T09:00:01.412Z",
  "eventId":  "01J...ULID"         // idempotency key for the consumer upsert
}
```

### 1.3 PostgreSQL — source of truth / audit (written async by consumers)

```sql
-- Every granted burger. 10M rows is trivial for Postgres.
CREATE TABLE grants (
    grant_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id      TEXT        NOT NULL,
    claim_code   TEXT        NOT NULL,
    bucket_id    SMALLINT    NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'GRANTED',  -- GRANTED | REDEEMED | EXPIRED
    granted_at   TIMESTAMPTZ NOT NULL,
    redeemed_at  TIMESTAMPTZ,
    notified_at  TIMESTAMPTZ,
    event_id     TEXT        NOT NULL,                    -- Kafka eventId, for idempotent upsert
    CONSTRAINT uq_grants_user UNIQUE (user_id),           -- backstop: one grant per user
    CONSTRAINT uq_grants_code UNIQUE (claim_code),
    CONSTRAINT uq_grants_event UNIQUE (event_id)          -- makes the consumer write idempotent
);
CREATE INDEX idx_grants_status_granted ON grants (status, granted_at);  -- expiry sweeper

-- Promo configuration / counters snapshot (single row).
CREATE TABLE promo (
    promo_id     TEXT PRIMARY KEY,
    total_cap    BIGINT      NOT NULL,   -- 10_000_000
    bucket_count INT         NOT NULL,   -- 100
    opens_at     TIMESTAMPTZ NOT NULL,
    closes_at    TIMESTAMPTZ NOT NULL,
    phase        TEXT        NOT NULL DEFAULT 'bulk'
);
```

The consumer does an **idempotent upsert** keyed by `event_id` (`INSERT ... ON CONFLICT (event_id) DO NOTHING`), so at-least-once delivery / replay never creates duplicate rows. The `UNIQUE(user_id)` is a *correctness backstop*, not the primary guard — the primary guard is Redis `SET NX`, which keeps the 670k/s uniqueness check off the database.

**Reconciliation invariant:** a batch job proves `COUNT(grants) ≤ 10,000,000` and equals `10,000,000 − Σ bucket:{k} − tail:remaining`. Redis is authoritative for the *count*; Postgres is authoritative for the *record*.

---

## 2. API design

All claim traffic carries an **`Idempotency-Key: {userId}`** header so a retried/timed-out tap is free — it resolves to the same outcome and never consumes a second burger. Every endpoint returns fast (target: few hundred ms at peak).

### 2.1 `POST /v1/claim` — the one hot endpoint

Claim a burger. Idempotent per user.

**Request**
```http
POST /v1/claim
Idempotency-Key: u_88213771
Content-Type: application/json
Authorization: Bearer <user token>       # waiting-room entry token / session

{ "userId": "u_88213771", "deviceId": "d_ab12", "promoId": "burger-2026" }
```

**Server logic (hot path):**
1. `SET claimed:{userId} <pending> NX` → if it already exists → **`ALREADY_CLAIMED`**.
2. Else `DECR bucket:{userId % 100}` → if ≥ 0 → **`GRANTED`**.
3. Else run the bounded sibling-probe Lua over `SRANDMEMBER active_buckets` candidates → if one found → **`GRANTED`**.
4. Else `DEL claimed:{userId}` (release the guard) → **`SOLD_OUT`**.
5. On `GRANTED`: produce the grant event to Kafka, then return the code. (Postgres write + notification happen async.)

**Responses** (all HTTP `200` with a discriminated `result`, so clients handle one shape — status codes in parens for REST purists):

```jsonc
// GRANTED  (200)
{ "result": "GRANTED", "code": "BRGR-7F3A-9K2Q", "message": "You got a burger! 🍔" }

// ALREADY_CLAIMED  (200 — idempotent replay or a genuine second tap)
{ "result": "ALREADY_CLAIMED", "code": "BRGR-7F3A-9K2Q", "message": "Your burger is on the way" }

// SOLD_OUT  (200, or 410 Gone)
{ "result": "SOLD_OUT", "message": "Sorry, all gone" }
```

| Failure | Status | Meaning |
|---------|--------|---------|
| No / invalid waiting-room token | `401` | must pass through the waiting room first |
| Rate limit (per IP / device) | `429` | bot / hammering; `Retry-After` header |
| Backend saturated (admission control) | `503` | `{ "result": "BUSY", "message": "We're busy, try again" }` — graceful valve |
| Promo not open / closed | `409` | outside `[opens_at, closes_at]` |

> **Fast-path SOLD_OUT:** once `global:sold_out = 1`, the API gateway/CDN short-circuits and returns `SOLD_OUT` at the edge **without touching Redis** — the graceful-degradation valve for the tail of the spike.

### 2.2 `GET /v1/claim/status?userId=` — check my claim (idempotent read)

```jsonc
// 200
{ "userId": "u_88213771", "status": "GRANTED", "code": "BRGR-7F3A-9K2Q", "grantedAt": "2026-07-23T09:00:01Z" }
// or { "status": "NONE" } / { "status": "SOLD_OUT" }
```
Reads Redis (`claimed:{userId}`), falls back to Postgres for post-promo lookups. Kept off the inventory keys.

### 2.3 `GET /v1/promo/remaining` — homepage counter (cosmetic, cached)

```jsonc
// 200 — served from display:remaining via CDN; approximate & eventually-consistent
{ "remaining": 3421187, "asOf": "2026-07-23T09:03:12Z", "approximate": true }
```
Reads the pre-computed `display:remaining` (refreshed every 1–2s). **Never** sums the live buckets on the request path — a cosmetic number must not contend with real inventory.

### 2.4 `POST /v1/redeem` — redeem a code (follow-up: pickup)

```jsonc
// Request:  { "code": "BRGR-7F3A-9K2Q", "storeId": "s_555" }
// 200:      { "result": "REDEEMED", "redeemedAt": "..." }
// 409:      { "result": "ALREADY_REDEEMED" }  |  { "result": "EXPIRED" }
```
Flips `grants.status = REDEEMED` (guarded by a conditional update so a code redeems at most once). Unredeemed grants past TTL (e.g. 24h) are swept back to inventory (`INCR bucket:{k}` + clear the user guard).

### 2.5 Admin / ops (internal, authenticated)

| Endpoint | Purpose |
|----------|---------|
| `GET /admin/reconcile` | run/report the `grants` vs Redis count reconciliation |
| `GET /admin/inventory` | per-bucket remaining + `phase` (bulk/tail) for the on-call dashboard |
| `POST /admin/phase` | manually flip `bulk → tail` if the aggregator hasn't |

---

## 3. Request lifecycle (happy path)

The **claim server is the orchestrator**: it makes a synchronous request/response call to Redis, and it is the Kafka **producer**. Redis never talks to Kafka.

```
Client (jittered) → Waiting room (entry token) → CDN/Gateway (rate-limit, admission)
   → Claim server
        ├─(1) ⇄ Redis:  SET NX  →  DECR bucket   ...... sync req/resp, returns GRANTED
        ├─(2) → returns code to the user           ...... user is done here
        └─(3) → Kafka (burger-grants)              ...... async, produced BY the server after GRANTED
                    → Consumer workers
                          ├→ Postgres grants (audit, idempotent upsert)
                          └→ Email/SMS "on the way"
```

The user has their code **the instant Redis says yes** (step 1→2). The durable write and notification (step 3) are produced by the server to Kafka and catch up behind it — they never touch the user's latency.
