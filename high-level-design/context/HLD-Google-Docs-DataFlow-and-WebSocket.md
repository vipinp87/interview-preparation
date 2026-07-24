# Google Docs — Data Flow, WebSocket & the Redis Pub/Sub Question

Companion to `HLD-Interview-Google-Docs.pdf`. The transcript nails the *algorithm* (operations, OT vs CRDT, snapshots). This companion adds what an interviewer usually probes next: **walk the data, hop by hop**, with tiny concrete examples — and it answers a specific, easy-to-get-wrong question head-on:

> **Can the WebSocket layer use Redis Pub/Sub? And can Redis Pub/Sub drop packets?**

Short answer up front, then the detail: **Yes, Redis Pub/Sub is a normal choice for fan-out between WebSocket servers — and yes, it can absolutely drop messages (it is at-most-once / fire-and-forget).** That's *fine* for presence (cursors), and *unsafe on its own* for document edits. The fix is not "make pub/sub reliable"; it's "number every op and re-read the gaps from a durable log." See `HLD-Google-Docs-DataFlow.png` / `.svg` for the picture.

---

## 1. The mental model in one breath

Three planes, deliberately kept apart:

| Plane | Carries | Guarantee | Where it lives |
|-------|---------|-----------|----------------|
| **Local echo** | *your own* keystroke | instant, 0 ms, no network | the browser buffer |
| **Edit sync** | everyone's ops, ordered | strong convergence (every replica ends byte-identical) | WebSocket → **sequencer** → durable **op log**; fan-out over pub/sub |
| **Presence** | cursors, selections, "is typing" | none — lossy on purpose | a separate ephemeral pub/sub channel |

The whole trick of the system is that the **fast path is allowed to be lossy** because the **slow path (the ordered op log) is the truth**, and a **sequence number** on every op lets a client notice "I missed one" and go re-read it.

---

## 2. Follow one keystroke — the end-to-end data flow

**Setup**

- Doc reads `Hello` — canonical **version 12**.
- **Alice**, **Bob**, **Carol** all have it open.
- Alice + Bob are on **WS server #1**; Carol is on **WS server #2**.
- **Alice types `!`** — trace ①→⑧ below (numbers match the diagram).

---

**① Local echo** — *0 ms, no network*

- Alice's editor inserts `!` into *her own* buffer the instant she types → she sees `Hello!`.
- Typing must **never** wait for a round trip (the hard requirement) → the client applies its own op *first*.
- The op it creates:

```jsonc
{ "type": "insert", "pos": 5, "text": "!",
  "baseVersion": 12,          // version Alice's client was at when she typed
  "clientOpId": "alice-7" }   // Alice's local id for this op (for ack matching)
```

**② Send over the WebSocket**

- Written as one WebSocket text frame onto Alice's already-open socket → WS #1.
- No new connection, no HTTP handshake — the socket has been open since she opened the doc.

**③ WS server → sequencer**

- WS #1 is only a socket-holder + fan-out relay; it does **not** decide order.
- It forwards the op to the **per-doc sequencer** — the one server that owns ordering for this doc (sticky-routed / consistent-hashed on `docId`).

**④ Sequencer — the real work** *(the linearization point)*

- **OT-transform** the op against any ops sequenced since `baseVersion 12` (here: none → unchanged).
- **Assign canonical `seq = 13`** — the moment the edit "officially happened" (like *payment success* in flash-sale). Order is now fixed forever.
- Hand off to ⑤ and ⑥.

**⑤ Append to the durable op log**

- `op#13 = insert(5,"!")` → Kafka / Cassandra, partitioned by `docId`.
- The **source of truth**; doubles as version history.
- Persisting happens **off the typing path** — collaborators don't wait on this disk write.

**Ack → Alice** *(rides back on ⑤)*

- Sequencer echoes `{seq: 13, clientOpId: "alice-7"}` down Alice's socket.
- Alice's client sees its *own* `clientOpId` → "server confirmed my already-applied op" → marks it committed, **does not re-apply** (else `Hello!!`).
- This "recognize my own echo" is exactly an idempotency key.

**⑥ Publish to the fan-out bus**

- Sequencer `PUBLISH`es the transformed op — **carrying `seq: 13`** — to Redis Pub/Sub channel `doc:{docId}`.

**⑦ Fan-out to the other clients**

- Every WS server subscribed to `doc:{docId}` receives it:
  - WS #1 → pushes `seq 13` to Bob → Bob's buffer becomes `Hello!`.
  - WS #2 → pushes `seq 13` to Carol → Carol's buffer becomes `Hello!`.
- This bus is the **only** way WS #2 hears an op that arrived at WS #1. No bus → Carol never sees Alice's edit.

**⑧ Gap recovery** *(the safety net)*

- Each client tracks its **last applied seq**.
- Carol at `12` suddenly receives `15` → she knows `13, 14` were **dropped**.
- She issues a **catch-up read** to the op log ("send ops `13..now`"), applies them in order → back in sync.
- **This is what makes a lossy fan-out safe.**

> Collaborators see the edit within one network RTT on the happy path (⑥⑦), and **always** converge even when the fast path drops a message (⑧). Local echo (①) never waited on any of it.

---

## 3. The durable channel — what runs at each hop, and how data flows through it

"Durable" names a **guarantee** (never lost · ordered · persisted · part of history), **not a transport**. It's a small stack of pieces, and it matters *which* piece establishes the durability.

### 3.1 The stack (and which layer actually owns durability)

| Layer | Job on the durable channel | Tech | What flows through it |
|-------|----------------------------|------|-----------------------|
| **Transport** | carry ops client ⇄ server, low-latency, push | **WebSocket** | op frames tagged *must-ack* |
| **Ordering / commit** | assign canonical per-doc `seq`, run OT | **Per-doc sequencer** — stateful service, sticky-routed / consistent-hashed on `docId` (or a per-doc leader) | one op at a time → gets `seq N` |
| **Durability + history** ⭐ | the append-only, ordered, never-lost record | **Kafka** partitioned by `docId` *(or Cassandra)* | `op#N` appended in order; replayable |
| **Fast current state** | materialized doc for fast open | **S3-style object store** *(or DynamoDB / Postgres row)* keyed by `docId` | `snapshot@op#K` blobs |
| **Offline durability** | queue local ops while disconnected | **IndexedDB** (browser-local) | queued ops tagged with `baseVersion` |
| **Fan-out** *(accelerator, not durability)* | push a sequenced op to other WS servers fast | **Redis Pub/Sub** *(or Redis Streams / Kafka for replay)* | best-effort copies of `op#N` |

> ⭐ **Durability is established at the sequencer's `seq` + the Kafka/Cassandra op log — nowhere else.** WebSocket only *transports*, Redis Pub/Sub only *accelerates*, the snapshot store only *serves fast*. An op is durable the instant it's appended to the log; losing the WebSocket frame or the Pub/Sub broadcast can never un-commit it.

### 3.2 Write path — how an op *becomes* durable (and why order matters)

- **1. WebSocket frame** — the op arrives at its WS server, which forwards it to the sequencer.
- **2. Sequencer** — OT-transform, then assign `seq = N`.
- **3. Append to Kafka** (partition = `docId`) — **this is the commit point; the op is now durable + ordered.**
- **4. *Only after* the append** — ack the client, then `PUBLISH` to the Redis fan-out.

The ordering is the whole point: the **durable write happens before the broadcast**, so a dropped Pub/Sub message (or a WS server crash) can never lose a committed op — it's already in the log, and the missing client re-reads it by `seq` gap (⑧).

### 3.3 Read / open path — how a doc loads fast without replaying all history

- **1.** Load the latest **snapshot** from S3/DynamoDB (e.g. `snapshot@op#12000`).
- **2.** Replay the **small tail** `op#12001..N` from Kafka on top.
- **3.** Doc is open in bounded time — regardless of whether it has 100 or 500,000 ops of history.

Version history / point-in-time restore falls out of the same two pieces: nearest snapshot before the timestamp + replay ops up to it.

### 3.4 Offline path — durability with no network at all

- **Offline:** ops append to the browser's **IndexedDB** queue, each tagged with the `baseVersion` it was made against.
- **Reconnect:** the client resends the queued ops; the sequencer OT-transforms them against everything sequenced since `baseVersion`, appends to the Kafka log, and broadcasts — identical path to live editing, just a longer transform backlog.

---

## 4. How a WebSocket server receives data — Redis Pub/Sub (push) vs Kafka (pull)

A WS server gets ops through **two different mechanisms with different shapes**:

- **Redis Pub/Sub → live push.** The server *subscribes* and Redis pushes each op to it as it's published.
- **Kafka / Cassandra op log → pull by range.** The server *reads a bounded range of ops by `seq`* for initial load and gap catch-up. It does **not** live-subscribe to Kafka per document.

Keep that split in mind: **Redis pushes the live feed; the log is pulled on demand.**

### 4.1 What the WS server holds in memory

- `docId → { set of local client sockets }` — the fan-out routing table.
- Per socket: `lastSeqSent` — how far that client is caught up.
- One dedicated **Redis connection in SUBSCRIBE mode** (a subscribed connection can't run normal commands, so it's separate from any command connection).
- A **Kafka consumer** (or **Cassandra session**) used only for range reads — never for live per-doc delivery.

### 4.2 Live receive — from Redis Pub/Sub (push)

- When the **first** local client for `docId` connects, the server runs `SUBSCRIBE doc:{docId}`.
- Redis then **pushes** every `PUBLISH` on that channel to the server; the client library fires an on-message callback with `(channel, payload)`.
- The callback fans out to the local sockets:

```text
on_message(channel = "doc:{docId}", payload = { seq, op }):
    sockets = routing_table[docId]
    for s in sockets:
        if seq == s.lastSeqSent + 1:        # contiguous → deliver
            s.send_ws_frame({ seq, op }); s.lastSeqSent = seq
        elif seq > s.lastSeqSent + 1:        # GAP → pub/sub dropped one
            trigger_catch_up(s, from = s.lastSeqSent + 1, to = seq)   # → §4.4
        # seq <= lastSeqSent → duplicate/echo, ignore
```

- When the **last** local client for `docId` disconnects, the server runs `UNSUBSCRIBE doc:{docId}` (stop paying to receive a doc nobody here is watching).

### 4.3 Initial load — when a client first opens the doc (pull)

- **1.** WS server loads the latest **snapshot** from S3 / DynamoDB (`snapshot@op#K`).
- **2.** WS server **range-reads the tail** `op#K+1 .. N` from the op log and folds it on top.
- **3.** Sends the client the materialized doc + `currentSeq = N`; sets `lastSeqSent = N`.
- **4.** `SUBSCRIBE doc:{docId}` (if this server isn't already subscribed) → live updates begin.

### 4.4 Catch-up read — from Kafka / Cassandra (pull, by range)

Triggered by a `seq` gap (⑧) or a reconnect. The op log is read as a **bounded range by `seq`**, not a subscription:

- **Cassandra** (keyed by `(doc_id, seq)`): `SELECT op FROM oplog WHERE doc_id=? AND seq >= ? AND seq <= ?` — a trivial ordered range scan. *This is exactly why keying the log by `(docId, seq)` is attractive.*
- **Kafka** (partition = `hash(docId)`): seek the partition to the **offset** of `op#from` and read forward to `op#to`. This needs a `seq → offset` index (or a compacted/keyed topic), which makes random by-`seq` catch-up more awkward on raw Kafka than on Cassandra.
- The fetched ops are applied in order and sent down the socket; `lastSeqSent` advances to close the gap.

### 4.5 Why Redis = live-push and Kafka = pull — and the alternatives

| Bus | Mode | Good for | Durable? | Latency |
|-----|------|----------|----------|---------|
| **Redis Pub/Sub** | push (`SUBSCRIBE`) | the **live** fan-out hop | no (at-most-once) | sub-ms |
| **Kafka / Cassandra log** | pull (range by `seq`/offset) | initial load + **gap catch-up** | yes | ms+ |
| **Redis Streams** | push-ish (`XREAD BLOCK`) **+** pull (`XREAD` from id) | both in one system | yes (bounded) | low |
| **Kafka consumed directly** | push (consumer subscription) | live **and** durable, no Redis | yes | higher |

- **Default:** Redis Pub/Sub for the live push (§4.2) + the log pulled for load/catch-up (§4.3–4.4). Fast where it must be, durable where it must be.
- **Consume Kafka directly for live:** each WS server is a Kafka consumer of the partitions holding its docs → durable + replayable live delivery with no Redis, but you pay consumer-group rebalancing, higher latency, and partition management. Reasonable at smaller fan-out.
- **Redis Streams:** replaces Pub/Sub as the live bus and folds catch-up in — a reconnecting server just `XREAD`s from its last-seen id, so §4.2 and §4.4 become one mechanism. Presence still stays on plain Pub/Sub.

> The through-line: **the WS server is a router.** It receives ops by *pushed subscription* (Redis, fast/lossy) and by *pulled range read* (the log, durable/exact), reconciles them with each socket's `lastSeqSent`, and writes WebSocket frames down to the browsers. The `seq` number is the glue that lets the two receive paths cooperate without duplicating or losing an op.

---

## 5. Why a WebSocket (and not HTTP polling)?

Editing is **bidirectional and continuous**: the server must *push* you other people's ops the instant they happen, and you must push yours the instant you type. A persistent WebSocket gives:

- **Server push** — someone else's op arrives without you asking. HTTP can't do this without long-polling hacks.
- **No per-message HTTP overhead** — a keystroke op is ~20 bytes; wrapping each in a fresh HTTP request is absurd.
- **Low, predictable latency** — polling every 200 ms both *feels* laggy and wastes requests when nobody's typing.

One socket per client per doc session, held open the whole time, carrying three multiplexed message kinds: **ops** (durable, ordered), **acks**, and **presence** (ephemeral).

---

## 6. The main event: can WebSocket use Redis Pub/Sub, and can it drop packets?

### 6.1 Why a bus is needed at all
At any real scale you run **many WebSocket server instances** behind a load balancer, and the clients of one document are **spread across several of them** (Alice+Bob on WS #1, Carol on WS #2). An op that lands on WS #1 must reach the sockets held by WS #2. So you need a **message bus between the WS servers**. Redis Pub/Sub is a classic, low-latency choice: each WS server `SUBSCRIBE`s to `doc:{docId}`; the sequencer (or the receiving WS server) `PUBLISH`es; Redis fans it out to all subscribers in-memory, sub-millisecond.

### 6.2 Yes — Redis Pub/Sub can drop your message
Redis Pub/Sub is **at-most-once, fire-and-forget**. There is *no* persistence, *no* per-subscriber queue, *no* ack, *no* replay. A published message is delivered only to subscribers **connected at that instant**, and even then delivery isn't guaranteed. Concretely, a message is lost when:

| # | Scenario | What happens |
|---|----------|--------------|
| a | A WS server **isn't subscribed at publish time** (just restarted, mid-reconnect, network partition) | it never receives that op — Redis does not hold it for later |
| b | A subscriber is **slow** and its output buffer fills | Redis enforces `client-output-buffer-limit pubsub`; when exceeded, Redis **force-disconnects** the subscriber, dropping everything queued |
| c | **Redis fails over / restarts** | in-flight messages are gone; a replica promoted on failover never had them (pub/sub isn't replicated like keyspace data) |
| d | A transient **network blip** between publisher/Redis/subscriber | the message evaporates silently — you won't even get an error |

So: **you must assume the fan-out layer loses messages.** This is not a Redis misconfiguration to fix; it's the defined semantics of Pub/Sub.

### 6.3 Why that's totally fine for presence
Presence (cursor position, selection, "is typing") is **latest-value-wins and disposable**. If Carol misses three of Alice's cursor moves, nobody notices — the fourth update repaints Alice's cursor in the right place. Presence is *never persisted, never ordered against edits, never caught up.* Here, the drop property is a **feature**: no durability machinery, no memory growth, self-healing by construction. Redis Pub/Sub (or the WS server's own in-memory fan-out) is the *right* tool precisely because it's cheap and lossy.

### 6.4 Why it's unsafe *alone* for document edits — and how to make it safe
For edits, a dropped op is a disaster: that client is now **permanently missing a change** and will **diverge** — the one thing OT/CRDT exist to prevent. But the fix is **not** to make pub/sub reliable. The fix is to stop *trusting delivery* and instead make loss *detectable and recoverable*:

1. **Number every op.** The sequencer stamps a **monotonic per-doc `seq`** on every broadcast op (④).
2. **Clients track "last applied seq."** Ops are contiguous: after `12` you expect `13`.
3. **Detect the gap.** If a client receives `seq 15` while sitting at `12`, it *knows* `13,14` were dropped — the sequence number turned an invisible loss into a visible, precise gap.
4. **Recover from the durable log.** The client issues a **catch-up read** — "send me ops `13..now`" — from the op log (or asks its WS server to backfill), applies them in order, and converges (⑧).
5. **Client→server direction is acked + resent.** Alice's op isn't considered committed until she gets the `{seq}` ack; unacked ops are re-sent on reconnect (this is the same path as offline editing).

Net effect: **pub/sub provides *speed* (best-effort, instant), the seq# + op log provide *correctness* (at-least-once, ordered, convergent).** The lossy fast path is a cache/accelerator in front of the authoritative log — never the system of record.

### 6.5 If you'd rather the *bus* handle recovery: Redis Streams (or Kafka)
You can push the gap-recovery down into the transport instead of a separate op-log fetch:

| Option | Delivery | Replay / catch-up | Ordering | Cost / notes |
|--------|----------|-------------------|----------|--------------|
| **Redis Pub/Sub** | at-most-once (**drops**) | none | per-publish only | cheapest, lowest latency; perfect for presence, needs seq#+log for ops |
| **Redis Streams** | at-least-once | **yes** — `XREAD`/`XAUTOCLAIM` from last-seen id; consumer groups | ordered per stream | a reconnecting WS server reads *from where it left off*; must `XADD MAXLEN`/trim to cap memory |
| **Kafka** | at-least-once | yes — offset replay, long retention | ordered per partition | heavier + higher latency than Redis; usually already your durable op log, so you may fan out straight from it |

A very common production shape: **Redis Pub/Sub for the low-latency edit fan-out *and* the presence channel, with the Kafka/Cassandra op log + seq# as the correctness backstop.** If you dislike the separate catch-up path, swap the edit fan-out to **Redis Streams** so "resume from last id" is built in. Presence stays on plain Pub/Sub either way — it neither needs nor wants replay.

---

## 7. Worked example — a dropped op, recovered

```
Carol is at seq 12.  Alice makes two quick edits → sequencer assigns seq 13, seq 14.

  seq 13  PUBLISH doc:{id}  ──▶ WS#2 delivers to Carol   ✅ Carol now at 13
  seq 14  PUBLISH doc:{id}  ──✗  (Carol's WS#2 was mid-reconnect — Redis dropped it)
  seq 15  PUBLISH doc:{id}  ──▶ WS#2 delivers to Carol   ⚠ Carol receives 15 while at 13

Carol's client: "expected 14, got 15 → GAP."
  → catch-up read to op log: GET ops(14..15)
  → apply op#14, then op#15 in order
  → Carol now at seq 15, converged.  No edit lost, no divergence.
```
Notice what did the work: **the sequence number** (made the loss *visible*) and **the durable log** (made it *recoverable*). Redis Pub/Sub did its job — fast in the happy case — and its failure was caught, not catastrophic.

---

## 8. Presence data flow (the deliberately-lossy twin)

```
Alice moves her cursor to offset 8
   │  (throttled to ~10/s; no local "echo" needed — it's her own cursor)
   ▼
WebSocket frame {type: "presence", cursor: 8, sel: [8,8]}  → WS #1
   ▼
PUBLISH presence:{docId}  (Redis Pub/Sub — SEPARATE channel from doc:{docId})
   ▼
every WS server delivers latest-value to its sockets → Bob & Carol repaint Alice's caret
   ✗ if any tick is dropped: ignored — the next tick (≤100ms later) repaints correctly
```
Same Redis Pub/Sub, opposite philosophy: **no seq#, no log, no catch-up.** Keeping presence on its own channel is what protects edit latency — a storm of cursor ticks never competes with the ordered, must-not-lose edit pipeline.

---

## 9. Where each guarantee is anchored (quick reference)

| Concern | Mechanism | Consistency |
|---------|-----------|-------------|
| Your own keystroke shows instantly | optimistic local apply (①) | immediate, local |
| Everyone converges to identical bytes | sequencer's canonical order + OT transform (④) | strong, eventual |
| An op is never lost | durable op log + client ack/resend + seq-gap catch-up (⑤⑧) | at-least-once → exactly-once effect |
| Others' edits arrive fast | Redis Pub/Sub fan-out (⑥⑦) | best-effort (accelerator) |
| Cursors/selections | separate ephemeral pub/sub | none (lossy by design) |
| Fast open of an old doc | snapshot + replay recent tail | — |

**The single sentence to say out loud:** *"Redis Pub/Sub is a fine fan-out for the WebSocket tier, but it's at-most-once — it can drop an op — so I never rely on it for correctness. Every op carries a per-doc sequence number and is appended to a durable ordered log; clients detect a seq gap and re-read the missing ops from that log. Pub/sub buys latency, the log buys correctness. Presence rides the same kind of pub/sub channel but deliberately tolerates loss, because a stale cursor is repainted by the next update."*
