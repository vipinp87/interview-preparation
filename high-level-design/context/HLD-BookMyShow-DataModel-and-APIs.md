# BookMyShow — Data Model & APIs

Companion to `HLD-Interview-BookMyShow.pdf`. The transcript reasons through the *design* (per-seat holds, all-or-nothing, re-validate-before-charge) but never writes down the **data model** or the **API contract** as artifacts. This fills both, and `HLD-BookMyShow-Architecture.png` / `.svg` carries the full visual architecture.

**One-line recap of the design:** Redis holds a per-seat, self-expiring lock (`SET NX EX`) so a small group of seats can be reserved for the minutes a human takes to pay, acquired all-or-nothing in sorted order; PostgreSQL is the ACID source of truth for confirmed bookings with `UNIQUE(show_id, seat_id)` as the unforgeable backstop against overselling; browse/search is cached and indexed separately because that traffic is huge but staleness-tolerant. Sharding everything hot by `showId` keeps one blockbuster's contention off every other screen.

---

## 1. Data model

The system has **three stores**, each authoritative for a different thing:

| Store | Authoritative for | Consistency |
|-------|-------------------|-------------|
| **Redis cluster** (sharded by `showId`) | live seat holds — "who may buy this seat right now" | strong / linearizable (per key) |
| **PostgreSQL** | the durable record of every confirmed booking, payment, and seat sale | ACID; `UNIQUE(show_id, seat_id)` |
| **Elasticsearch + cache** | the read model for browse/search (movies, theaters, showtimes, seat-map render) | eventual (fed async from Postgres) |
| **Kafka** (`booking-confirmed`) | the durable event log for notifications / analytics / index refresh | durable, ordered per `showId` |

### 1.1 Redis keyspace (the hot path — the *only* thing on the seat-hold critical path)

| Key | Type | Init / Value | Purpose | Op on hot path |
|-----|------|--------------|---------|----------------|
| `lock:{showId}:{seatId}` | String | `= userId`, written `NX EX 180` | the per-seat hold — atomic, self-expiring | `SET NX EX` to hold; `GET` to re-validate; `DEL` on confirm |
| `hold_count:{userId}:{showId}` | Integer | `INCR` per held seat, `EX` with the hold | anti-scalp cap — a user can't hold more than `maxSeats` in one show | `INCR` (reject if > cap) |
| `seatmap:{showId}` | Hash / cached blob | `seatId → AVAILABLE\|HELD\|CONFIRMED`, refreshed on a timer | the **display** snapshot the seat-map renders from (approximate) | read-only (never gates a hold) |
| `remaining:{showId}` | Integer | `total − count(HELD\|CONFIRMED)`, recomputed every 1–2s | cosmetic "seats left" badge | read-only (never touches the lock keys) |

**Key design choices baked into the keyspace**
- **Shard by `showId`.** Every lock/hold key for one show lives on the same Redis shard, so the worst contention (one blockbuster) is confined to one shard and unrelated shows never compete. It's a *sharded*, not global, contention problem.
- **`SET NX EX 180` is the whole hold.** `NX` makes acquisition atomic per seat; `EX` makes the hold **self-expiring** — an abandoned checkout frees the seat with zero cleanup code and no sweeper.
- **A hold is a hint about ownership, not a promise of a sale.** The lock says "you may *try* to buy this seat now." The sale is only real once Postgres commits the `CONFIRMED` row.
- **The seat-map *render* reads the cached snapshot; the *hold click* reads live.** Browse can be slightly stale (a "just taken" seat is a fine UX blemish); the acquire step is always a live, atomic `SET NX`.

#### All-or-nothing multi-seat acquire (sorted order → no deadlock/livelock)
```
acquireHold(showId, userId, seats):        # seats sorted by seatId — deterministic order
    acquired = []
    for seat in seats:
        if SET lock:{showId}:{seat} userId NX EX 180:
            acquired.append(seat)
        else:                               # someone else holds/owns this seat
            for s in acquired: DEL lock:{showId}:{s}   # roll back partial hold
            return FAIL(seat)               # → SEAT_UNAVAILABLE: [seat]  (tell the UI exactly which)
    return SUCCESS(acquired)
```
Locking in a **consistent sorted order** is what stops the classic deadlock where user A holds (A5, A6) while user B holds (A6, A5) in the opposite order. A crash mid-acquire self-heals via the `EX` TTL — no orphaned holds.

### 1.2 Kafka — topic `booking-confirmed`

- **Partitioned by `showId`** (per-show ordering), durable, replicated, retained so it doubles as the replayable audit/analytics stream.
- **Produced by the Booking Service, not by Redis or Postgres.** After the `CONFIRMED` row commits, the service emits `{bookingId, showId, userId, seats, amount, confirmedAt}` (at-least-once) before returning. Consumers do the slow, staleness-tolerant work off the critical path — a slow SMS gateway must never delay "Booking confirmed!"

```jsonc
// message value (key = showId)
{
  "bookingId":  "bk_01J8ZT...",
  "showId":     "show_5551",
  "userId":     "u_88213771",
  "seats":      ["A5", "A6", "A7"],
  "amount":     1350,
  "currency":   "INR",
  "confirmedAt":"2026-07-23T14:32:07.114Z",
  "eventId":    "01J...ULID"       // idempotency key for the consumer upsert
}
```

### 1.3 PostgreSQL — source of truth (sharded by region/theater)

```sql
-- One row per seat per show. UNIQUE(show_id, seat_id) is the unforgeable
-- anti-oversell backstop: even if the Redis logic had a bug, only ONE
-- booking can win the CONFIRMED write for a given seat.
CREATE TABLE show_seats (
    show_id     BIGINT      NOT NULL,
    seat_id     TEXT        NOT NULL,          -- 'A5', 'A6', ...
    tier        TEXT        NOT NULL,          -- SILVER | GOLD | RECLINER
    price       INT         NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'AVAILABLE',  -- AVAILABLE | CONFIRMED
    booking_id  BIGINT,                        -- set when CONFIRMED
    PRIMARY KEY (show_id, seat_id)             -- == the uniqueness backstop
);

-- A booking ties a group of seats together so they move as one unit.
CREATE TABLE bookings (
    booking_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id         TEXT        NOT NULL,
    show_id         BIGINT      NOT NULL,
    seats           TEXT[]      NOT NULL,        -- the group, all-or-nothing
    amount          INT         NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'PENDING_CONFIRM',
                                                 -- PENDING_CONFIRM | CONFIRMED | CANCELLED | FAILED
    payment_ref     TEXT,                        -- gateway transaction id
    event_id        TEXT,                        -- Kafka eventId, for idempotent consumer upsert
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at    TIMESTAMPTZ,
    CONSTRAINT uq_bookings_event UNIQUE (event_id)
);
CREATE INDEX idx_bookings_status ON bookings (status, created_at);  -- reconciliation sweep

-- Shows / theaters / seat-map layout (read-mostly; also feeds Elasticsearch).
CREATE TABLE shows (
    show_id     BIGINT PRIMARY KEY,
    movie_id    BIGINT      NOT NULL,
    screen_id   BIGINT      NOT NULL,
    starts_at   TIMESTAMPTZ NOT NULL,
    home_region TEXT        NOT NULL,          -- authoritative region; never sharded across regions
    total_seats INT         NOT NULL
);
```

**The confirm is a single, short, ACID transaction** (§ APIs 2.3): re-validate the Redis holds, then `INSERT bookings … ` + `UPDATE show_seats SET status='CONFIRMED'` guarded by the `PRIMARY KEY (show_id, seat_id)`. The second writer to the same seat loses on the unique constraint and fails loudly — never a silent oversell. A short **row lock held for milliseconds** here is correct; a DB lock held across human think-time (the naive alternative) is what exhausts the connection pool and deadlocks — see the transcript's §14 deep-dive.

**Multi-region:** a show has exactly one **home region** (where the theater physically is). Its seat-lock Redis shard and its `CONFIRMED` write both live in-region and are strongly consistent there — never eventual across regions, or two regions could each believe they sold seat A5. Browse serves from the nearest region's cache/replica/index.

**Saga / outbox for "charged but not confirmed":** write the booking as `PENDING_CONFIRM` *before* calling the gateway; flip to `CONFIRMED` in one txn when payment succeeds. If the app crashes in between, a reconciliation job compares the gateway's transaction log against booking states and completes the confirm — or, if the seats were legitimately re-sold in the gap, refunds and apologizes. Never leave a customer charged with nothing to show for it.

---

## 2. API design

Browse endpoints are cacheable and staleness-tolerant. The booking endpoints carry an **`Idempotency-Key`** so a retried/timed-out tap is free. Seat-map **rendering** is served from cache; the **hold** and **confirm** actions always read live state.

### 2.1 `GET /v1/shows/{showId}/seatmap` — render the seat grid (cacheable, approximate)

```jsonc
// 200 — served from the cached snapshot (seatmap:{showId}); refreshed on a timer
{
  "showId": "show_5551",
  "asOf":   "2026-07-23T14:31:59Z",
  "approximate": true,
  "seats": [
    { "seatId": "A5", "tier": "GOLD", "price": 450, "status": "AVAILABLE" },
    { "seatId": "A6", "tier": "GOLD", "price": 450, "status": "HELD" },
    { "seatId": "A7", "tier": "GOLD", "price": 450, "status": "CONFIRMED" }
  ]
}
```
Rendering the 200–300 seat grid doesn't need to be transactionally live — a stale "available" seat is a fine UX blemish, resolved atomically at the hold step. **Never** a live sum over the lock keys.

### 2.2 `POST /v1/holds` — acquire an all-or-nothing hold on N seats (the hot endpoint)

```http
POST /v1/holds
Idempotency-Key: u_88213771:show_5551:req_1
Content-Type: application/json
Authorization: Bearer <user token>

{ "userId": "u_88213771", "showId": "show_5551", "seats": ["A5","A6","A7"] }
```

**Server logic (hot path):** sort `seats`; for each, `SET lock:{showId}:{seat} userId NX EX 180`; on the first failure, `DEL` everything acquired and return `SEAT_UNAVAILABLE` naming the exact seat. Bump `hold_count:{userId}:{showId}`; reject over the cap.

**Responses** (HTTP `200` with a discriminated `result`; status codes in parens for REST purists):
```jsonc
// HELD  (200 / 201)
{ "result": "HELD", "holdId": "h_9f2a", "seats": ["A5","A6","A7"],
  "expiresAt": "2026-07-23T14:35:00Z", "message": "Seats held — pay within 3 minutes" }

// SEAT_UNAVAILABLE  (200, or 409 Conflict) — names exactly which seat was lost
{ "result": "SEAT_UNAVAILABLE", "seats": ["A6"], "message": "A6 was just taken — pick another" }
```

| Failure | Status | Meaning |
|---------|--------|---------|
| Over per-user hold cap (anti-scalp) | `429` | `hold_count` exceeded `maxSeats`; `Retry-After` |
| No / invalid session token | `401` | must be authenticated |
| Show not on sale / past start | `409` | outside the bookable window |

### 2.3 `POST /v1/bookings/{holdId}/confirm` — pay & confirm (re-validate before charge)

The one place a TTL race could otherwise oversell. **Order of operations gates the charge on the hold.**

**Server logic:**
1. For each seat: `GET lock:{showId}:{seat}` — if `!= userId` → **`ABORT_BEFORE_CHARGE`** (hold expired / stolen; no money moves).
2. Only if *all* holds are still ours: `paymentSvc.capture(idempotencyKey = bookingId)`.
3. In one Postgres txn: `INSERT bookings … CONFIRMED` + `UPDATE show_seats … CONFIRMED`, guarded by `PRIMARY KEY (show_id, seat_id)`.
4. `DEL lock:{showId}:{seat}` for each seat; emit `booking-confirmed` to Kafka; return the e-ticket.

```jsonc
// CONFIRMED  (200)
{ "result": "CONFIRMED", "bookingId": "bk_01J8ZT...", "seats": ["A5","A6","A7"],
  "eTicket": { "qr": "…", "url": "…" }, "message": "Booking confirmed! 🎟" }

// ABORT_BEFORE_CHARGE  (200, or 409) — hold lapsed; NEVER charged
{ "result": "ABORT_BEFORE_CHARGE", "seats": ["A6"],
  "message": "Your held seats expired — please re-select" }
```

> **Why re-check right before capture, not just before authorize:** if the TTL lapsed and User B grabbed the seat, User A's confirm fails the `GET lock == userId` check *before* `capture()` is ever called — no money moves. Postgres's unique constraint is the last-line backstop for the *database*, but it should never be the thing that catches a bad *charge* — by the time it fires, the money's already moved. If auth/capture are split and we'd already authorized, **void** rather than capture; if a gateway already captured, **auto-refund** on the same failure path and log for reconciliation.

### 2.4 `POST /v1/bookings/{bookingId}/cancel` — cancel a confirmed booking (policy-gated)

```jsonc
// 200: { "result": "CANCELLED", "refund": { "amount": 1350, "status": "INITIATED" } }
// 409: { "result": "NON_REFUNDABLE" }  // past the cancellation window
```
Flips `bookings.status = CANCELLED` and `show_seats.status = AVAILABLE` in one txn (subject to policy), returning the seats to inventory.

### 2.5 `GET /v1/shows/{showId}/remaining` — "X seats left" badge (cosmetic, cached)

```jsonc
// 200 — served from remaining:{showId} via CDN; approximate & eventually consistent
{ "showId": "show_5551", "remaining": 42, "asOf": "2026-07-23T14:32:00Z", "approximate": true }
```
Computed on a 1–2s timer as `total − count(HELD|CONFIRMED)` (or off the Kafka events). **Never** reads the live per-seat lock keys — a cosmetic counter must not contend with real seat inventory.

### 2.6 Admin / ops (internal, authenticated)

| Endpoint | Purpose |
|----------|---------|
| `GET /admin/reconcile` | run/report the gateway-txn-log vs `bookings` state reconciliation (complete or refund stuck `PENDING_CONFIRM`) |
| `GET /admin/shows/{showId}/inventory` | per-seat live status for the on-call dashboard |
| `POST /admin/shows/{showId}/hold/{seatId}/release` | manually release a stuck hold |

---

## 3. Request lifecycle (happy path — a 3-seat booking)

The **Booking Service is the orchestrator**: synchronous request/response to Redis for the hold, a re-validate-then-capture handshake with the Payment Service, one short ACID confirm to Postgres, then it is the Kafka **producer**. Redis and Postgres never talk to Kafka.

```
Client → Gateway (auth, rate-limit, content-aware route) → Booking Service
   ├─(1) ⇄ Redis:  SET NX EX 180 × N seats (sorted)   ...... sync, all-or-nothing → HELD
   │        "held, pay within 3 min"                    ...... user enters card
   ├─(2) ⇄ Redis:  GET lock == userId × N              ...... re-validate BEFORE charge
   ├─(3) → Payment Svc: capture(idempotencyKey)        ...... only if all holds still ours
   ├─(4) → Postgres: INSERT booking + seats CONFIRMED  ...... one ACID txn, UNIQUE backstop
   │        DEL locks; return e-ticket                  ...... user is done here
   └─(5) → Kafka (booking-confirmed)                    ...... async, produced BY the server
                → Notification workers  → e-ticket + QR (email/SMS)
                → Analytics / BI        → seats-sold, revenue
                → Search-index updater  → refresh "seats-left" badge
```

The user gets **"Booking confirmed!"** the instant Postgres commits (step 4). Email, analytics, and the badge refresh (step 5) catch up behind it, off the critical path — the same pattern as the burger/flash-sale audit log. The whole hold path is typically **under 30–50 ms** even at peak: Redis is in-memory, the hot key set is tiny (a few hundred seats), and no synchronous DB write happens until the rarer, latency-tolerant confirm.
