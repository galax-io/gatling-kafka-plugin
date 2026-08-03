# Phase 1 Data Model: Non-blocking Reply-Tracker Acquisition

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md)

No persisted data; all entities are in-memory runtime state inside the `client` layer. Existing
entities are listed where their lifecycle participates in the change.

## Entities

### PendingSubscription (changed representation)

Queue element in `DynamicKafkaConsumer` awaiting consumer readiness for one topic request.

| Field | Type | Notes |
|-------|------|-------|
| `topic` | `String` | Reply (consumer) topic requested for subscription |
| `readiness` | `CompletableFuture[Void]` | Replaces today's `CountDownLatch`; one per request, never shared across requests (N concurrent first-users of a topic = N queue entries, as today) |

**Validation rules**: created only after a consumer-failure fast check; carries no timeout (the
caller owns deadlines — FR-003 keeps timeout policy in the pool/protocol layer).

**State transitions**:

```text
Queued ──(updateSubscription: topic already in subscription, not being removed)──▶ Completed
Queued ──(subscribe() → onPartitionsAssigned)───────────────────────────────────▶ Completed
Queued ──(markConsumerFailed)──────────────────────────────▶ Failed(consumer failure, cause)
Queued ──(consumer close / run-loop exit drains queue)─────▶ Failed(consumer closed)
```

Completion is idempotent (`complete`/`completeExceptionally` after completion is a no-op), which
absorbs the race between assignment, failure, and shutdown.

### AcquisitionRequest (new, transient)

One request-reply operation's attempt to obtain a tracker, held as closure state on the slow
path in `KafkaMessageTrackerPool.acquireTracker`.

| Field | Type | Notes |
|-------|------|-------|
| `consumerTopic` / `producerTopic` | `String` | Identity of the exchange |
| `matcher` | `KafkaMatcher` (by reference) | Same reference-identity semantics as today's `MatcherRef` |
| `timeout` | `FiniteDuration` | Protocol reply timeout; bounds the readiness wait |
| `timeoutTask` | `ScheduledFuture[_]` | Cancelled when readiness completes first; fires `completeExceptionally(RuntimeException("Timed out waiting for consumer assignment to topic 'T' after t"))` otherwise |
| `onReady` | `ActorRef[TrackerMessage] => Unit` | Action's continuation: record sent timestamp, register `MessagePublished` |
| `onFailure` | `Throwable => Unit` | Action's continuation: existing KO reporting path |

**States**:

```text
FastPath(found) ──────────────────────────────▶ Ready        (onReady inline, caller thread)
SlowPath ──▶ AwaitingReadiness ──(complete)───▶ Ready        (onReady on setup executor)
             AwaitingReadiness ──(timeout)────▶ TimedOut     (onFailure on setup executor)
             AwaitingReadiness ──(cons. fail)─▶ Failed       (onFailure on setup executor)
```

Exactly one of `onReady`/`onFailure` is invoked, exactly once (guaranteed by
`CompletableFuture.whenCompleteAsync` single-fire semantics).

### TrackerEntry (existing, unchanged)

`(actor: ActorRef[TrackerMessage], refCount: AtomicInteger)` in the
`consumerTopic → (MatcherRef → TrackerEntry)` two-level `ConcurrentHashMap`.

- Insert-or-increment stays inside `trackers.compute` (bin-lock atomicity vs `releaseTracker`
  unchanged).
- On the slow path the increment happens in the readiness continuation — i.e. only once tracking
  is actually ready. A timed-out acquisition therefore never touches a refcount (nothing to roll
  back; FR-006's no-residual-reservation guarantee).
- `releaseTracker` (decrement, remove at zero, queue unsubscribe) is untouched — its defects are
  #165/#166 scope.

### SetupExecutor (new)

Pool-owned `ScheduledThreadPoolExecutor(1)`, daemon, thread name
`gatling-kafka-tracker-setup`.

**Lifecycle**: created with `KafkaMessageTrackerPool`; `shutdownNow()` added to the existing
`actorSystem.registerOnTermination` block (alongside consumer close), satisfying the spec's
clean-shutdown edge case. Tasks must be non-blocking; nothing on it may call broker APIs.

## Relationships

```text
KafkaRequestReplyAction ──acquireTracker(onReady,onFailure)──▶ KafkaMessageTrackerPool
KafkaMessageTrackerPool ──requestTopicSubscription──▶ DynamicKafkaConsumer (PendingSubscription)
DynamicKafkaConsumer ──complete/fail readiness──▶ (setup executor) ──▶ TrackerEntry upsert ──▶ onReady
KafkaMessageTracker (actor) ◀─MessagePublished── onReady closure   [unchanged message protocol]
```

`KafkaProtocolMessage`, `KafkaMatcher`, and the tracker actor message protocol
(`MessagePublished` / `MessageConsumed` / `ConsumerFailure` / `TimeoutScan`) are **not** modified
— single wire representation and matching contract preserved (constitution Principle III).
