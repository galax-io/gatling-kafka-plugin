# Phase 1 Data Model: Request-Reply Reliability Hardening

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md) | **Research**: [research.md](research.md)

All state here is in-process and run-scoped; nothing is persisted or serialized. This document
records the state each fix owns, what changes about it, and the transitions that must hold. Spec
entity names are used throughout, with the implementing type named once in parentheses.

---

## 1. Pending-request record (`KafkaMessageTracker.sentMessages` value) — #191

The record that one request has been sent and is awaiting a reply. Today the map value **is** the
`MessagePublished` message; after #191 it is a small wrapper so the request can be registered before
its acknowledgement timestamp exists.

| Field | Source | Notes |
|---|---|---|
| `published` | `MessagePublished`, unchanged field list | `matchId`, `replyTimeout`, `checks`, `session`, `next`, `requestName`, `onComplete`, and `sentTimestamp` carrying the handoff time |
| `registeredAt` | the tracker, on `MessagePublished` | The clock `TimeoutScan` measures against. Never absent — a request whose ack never lands must still time out |
| `ackedAt` | `MessageAcked` | Absent until the producer acknowledges. **This is the timestamp reported to the stats engine**, which is what preserves FR-017 |
| `heldReply` | `MessageConsumed` arriving before `MessageAcked` | At most one. Present only in the reply-before-ack window |

**Keyed by** the Base64 encoding of the match identifier, exactly as today
(`makeKeyForSentMessages`). Two in-flight requests producing the same identifier remain
indistinguishable — existing behaviour, explicitly preserved by the spec's edge cases.

**Ownership**: the actor's private single-threaded state. It stays a `mutable.HashMap`; research R3
establishes that ordering, not concurrency, was the defect.

### State transitions

```text
                    MessagePublished
                   (before the send)
                          │
                          ▼
                   ┌─────────────┐
                   │ REGISTERED  │  registeredAt set, ackedAt empty
                   └──────┬──────┘
              MessageAcked│        │MessageConsumed
                          ▼        ▼
                 ┌────────────┐  ┌──────────────┐
                 │   ACKED    │  │  REPLY HELD  │  heldReply set
                 └─────┬──────┘  └──────┬───────┘
          MessageConsumed│               │MessageAcked
                          ╲             ╱
                           ▼           ▼
                      ┌──────────────────┐
                      │    COMPLETED     │  removed; checks run; response logged
                      └──────────────────┘
                              ▲
                              │  SendFailed / TimeoutScan / ConsumerFailure
                              │  (removed; KO logged)
```

**Invariants**

- **P1.** The record exists before `sender.send` is called for its request. This is FR-001; every
  other guarantee about reply delivery rests on it.
- **P2.** Exactly one terminal event fires per record — completion, send failure, timeout, or
  consumer failure — and each invokes `onComplete` exactly once, so the channel's in-flight count is
  balanced against acquisition.
- **P3.** A response is logged with `ackedAt` as its start, never with `registeredAt`. A record in
  `REPLY HELD` logs nothing until the ack lands.
- **P4.** `TimeoutScan` measures `now - registeredAt`, so a record stuck in `REGISTERED` or
  `REPLY HELD` because its ack never arrived still times out on schedule.
- **P5.** A reply matching no record is discarded silently — unchanged, and FR-002 forbids buying
  FR-001 by weakening it.

---

## 2. Reply-tracking registration (`KafkaMessageTrackerPool.TrackerEntry`) — #166

The per-(reply topic, matching rule) bookkeeping. Gains nothing structurally; what changes is that
its removal now stops what hangs off it.

| Field | State | Notes |
|---|---|---|
| `actor` | unchanged | The tracker |
| `refCount` | unchanged | Requests in flight. Incremented at acquisition, decremented by `onComplete` |
| `idleSince` | unchanged | `clock.nowMillis` when `refCount` last reached zero |

**What changes**: on removal — by the idle sweep, by the consumer-failure broadcast, or at pool
shutdown — the entry's actor is sent `Stop`. The `Cancellable` itself stays inside the tracker, which
created it; research R2 records why hoisting it into the entry was rejected.

### Lifecycle

```text
  acquire (first)          acquire/release            idle grace elapses
        │                        │                            │
        ▼                        ▼                            ▼
   ┌─────────┐   refCount>0 ┌─────────┐  refCount→0  ┌──────────────┐    ┌─────────┐
   │ CREATED │─────────────►│ IN USE  │─────────────►│    IDLE      │───►│ STOPPED │
   └─────────┘              └────▲────┘              └──────┬───────┘    └─────────┘
                                 │   acquire before grace   │            entry removed,
                                 └──────────────────────────┘            Stop sent, scan
                                                                         cancelled, die
```

**Invariants**

- **E1.** `Stop` is sent only after the entry has been removed from `trackers`, and only from outside
  a `ConcurrentHashMap` compute lambda — following the precedent `sweepIdleTrackers` already sets for
  `removeTopicSubscription`.
- **E2.** `Stop` is sent only when `refCount <= 0` has held for a full idle grace, so a stopped
  tracker never holds outstanding pending-request records. This is what makes it safe to stop the
  very actor that would otherwise time those requests out.
- **E3.** Every path that drops an entry stops it: the idle sweep, the consumer-failure broadcast
  (which clears the whole map), and pool shutdown. A path that drops without stopping is the leak.
- **E4.** After `Stop`, the tracker's `Cancellable` is cancelled and its behaviour is `die`. Nothing
  it held — stats engine, clock, matcher closures — is reachable from the actor system's scheduler
  any more.
- **E5.** A stale `MessageConsumed` reaching a stopped tracker is dropped by Gatling with one INFO
  line. Bounded to the sweep-versus-poll window, because the topic is unsubscribed immediately after.

---

## 3. Shared reply-receiving machinery (`DynamicKafkaConsumer`) — #143

No new state. One field becomes injectable and one loop gains a precondition.

| Aspect | Today | After |
|---|---|---|
| Initialization wait | `private val initializationTimeout = 90.seconds` on the companion | Same default, supplied through an additive `apply` overload so tests can shorten it |
| `initLatch.await` result | discarded | Honoured and logged, so "a topic arrived" and "the wait expired" are distinguishable |
| Poll precondition | none | Skipped whenever `subscription()` and `assignment()` are both empty; the interval is slept instead |

### Loop states

```text
   ┌────────────────┐  first topic requested   ┌──────────┐
   │  WAITING       │─────────────────────────►│ POLLING  │
   │  (no topic)    │                          └────┬─────┘
   └───────▲────────┘                               │
           │  wait expires with nothing requested   │  subscription kept non-empty
           │  → keep looping, do not poll           │  by updateSubscription (#165)
           └───────────────────────────────────────-┘
```

**Invariants**

- **C1.** `consumer.poll` is never called while the consumer holds neither a subscription nor an
  assignment. This is the only call that throws
  `IllegalStateException: Consumer is not subscribed to any topics or assigned any partitions`.
- **C2.** A loop turn that skips the poll still runs `updateSubscription()`, so a topic requested
  during the wait — or long after it expired — is picked up on the next turn.
- **C3.** The existing "never unsubscribe down to nothing" guard is untouched. C1 and that guard are
  complementary: one covers a subscription that was never established, the other one that shrank.
- **C4.** Expiry of the initialization wait produces no `onFailure` call and no
  `markConsumerFailed`, so the pool's `consumerFailure` latch is never set by it.

---

## 4. Echo responder (`KafkaGatlingTest`, test-only) — #196

Not part of the published plugin. A stand-in for the system under test, modelled on
`KafkaConcurrencyLoadTest`.

| Aspect | Value |
|---|---|
| Consumes | `myTopic1`, `myTopic2` — **not** `myTopic4` |
| Produces | `myTopic1 → test.t1`, `myTopic2 → test.t2` |
| Key | echoed byte-for-byte — `scnRR` matches by key |
| Value | echoed byte-for-byte — `scnRR2` matches by value and checks `bodyBytes`; `scnRR` checks `jsonPath` |
| Headers | a response timestamp, added — the only thing the responder writes |
| Lifecycle | started in `before` behind a readiness probe; closed in `after`, and on a `before` failure |

**Invariants**

- **R1.** The responder replies only to a request it received. No scenario's reply comes from another
  scenario's publish — which is what makes SC-007 (delete the produce-only scenarios; request-reply
  still passes) a meaningful check.
- **R2.** Key and value are byte-identical between request and reply. Any rewrite breaks matching,
  the checks, or both.
- **R3.** `myTopic4` has no consumer, so `scnRRwo` times out by construction rather than by timing.
- **R4.** A responder send failure is logged loudly. A silently-stopped responder is
  indistinguishable from the plugin losing replies, and would fail the run with nothing pointing at
  the cause.

---

## Cross-cutting: what does *not* change

- `KafkaProtocolMessage` — still the single wire representation; #196's timestamp uses its existing
  `headers` field.
- `KafkaMatcher` — still the single matching contract.
- `MessagePublished`'s field list, `acquireTracker`, `releaseTracker`, and both primary constructors.
- The published Scala DSL, the `javaapi` facade, protocol defaults, and reported response-time
  semantics (FR-016, FR-017).
