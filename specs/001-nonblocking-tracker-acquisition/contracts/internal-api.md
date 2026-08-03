# Internal API & Threading Contract

**Feature**: [../spec.md](../spec.md) | **Plan**: [../plan.md](../plan.md)

The plugin's *published* contract (Scala DSL, `javaapi`, protocol defaults, wire formats) is
intentionally unchanged — see spec FR-008. This document is the contract between the three
internal collaborators the feature touches. It is normative for implementation and review.

## 1. `DynamicKafkaConsumer`

```scala
/** Requests that `topic` join the consumer subscription.
  *
  * Non-blocking: enqueues the request and returns immediately. The returned future completes
  * when the consumer is assigned partitions for a subscription including `topic` (or the topic
  * is already subscribed), and fails if the consumer has failed or is closed. The future carries
  * no deadline — callers own timeout policy.
  */
def requestTopicSubscription(topic: String): CompletableFuture[Void]

// removed: def addTopicForSubscription(newTopic: String, assignTimeout: FiniteDuration): Boolean
def removeTopicSubscription(topic: String): Unit   // unchanged
```

**Guarantees**:

- G1. Never blocks the calling thread (queue add + possible `initLatch.countDown` only).
- G2. If the consumer already failed, returns an already-failed future with the existing
  `IllegalStateException("Kafka consumer failed; dynamic consumer can no longer be used")` cause
  chain — it does **not** throw.
- G3. Completion may occur on the **consumer poll thread** (rebalance listener /
  already-subscribed branch) or the closing thread. Work attached by callers MUST be dispatched
  off-thread (`whenCompleteAsync` with an explicit executor) unless O(µs) and non-blocking.
- G4. On consumer failure (`markConsumerFailed`) every queued future fails with the failure
  cause. On close / run-loop exit, remaining queued futures fail exceptionally ("consumer
  closed") — no future is left forever-pending (spec edge case: shutdown mid-preparation).
- G5. Queue/coalescing semantics vs `removeTopicSubscription` are unchanged from today,
  including the #164 no-op-subscribe defect (documented, out of scope).

## 2. `KafkaMessageTrackerPool`

```scala
/** Obtains (or creates) the tracker for (consumerTopic, matcher-identity) without blocking.
  *
  * Exactly one of `onReady` / `onFailure` is invoked, exactly once.
  */
def acquireTracker(
    producerTopic: String,
    consumerTopic: String,
    messageMatcher: KafkaMatcher,
    responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
    timeout: FiniteDuration,
)(onReady: ActorRef[KafkaMessageTracker.TrackerMessage] => Unit, onFailure: Throwable => Unit): Unit

// removed: def tracker(...): ActorRef[KafkaMessageTracker.TrackerMessage]  (blocking)
def releaseTracker(consumerTopic: String, messageMatcher: KafkaMatcher): Unit   // unchanged
```

**Guarantees**:

- G6. `acquireTracker` never blocks the calling thread. Fast path (entry exists): refcount
  incremented and `onReady` invoked **inline on the caller's thread** before the method returns.
  Slow path: returns immediately; continuations run on the setup executor.
- G7. Pool-poisoned state (`consumerFailure` set) reports through `onFailure` — synchronously on
  the calling thread — never by throwing.
- G8. Timeout produces `onFailure` with a `RuntimeException` whose message names the topic and
  the timeout (existing text: `Timed out waiting for consumer assignment to topic 'T' after t`).
  A timed-out acquisition has touched no refcount and created no `TrackerEntry`; a later
  `acquireTracker` for the same topic issues a fresh readiness request (FR-003).
- G9. Refcount/registration atomicity is unchanged: insert-or-increment under
  `trackers.compute`; concurrent slow-path winners converge on one `TrackerEntry` (second
  completes as increment). `onComplete`-driven `releaseTracker` pairing is preserved 1:1 with
  successful acquisitions.
- G10. The setup executor is single-threaded, daemon, owned by the pool, shut down via
  `registerOnTermination`; tasks scheduled on it MUST be non-blocking and MUST NOT call broker
  APIs.

## 3. `KafkaRequestReplyAction` (caller obligations)

- G11. The producer-callback body performs only: debug logging guard, `requestMatch`, and
  `acquireTracker` wiring. No waiting of any kind on the delivery-callback thread (FR-001/002).
- G12. `onReady` body: record `sentTimestamp = clock.nowMillis`, then
  `tracker ! MessagePublished(..., onComplete = releaseTracker(...))` — identical timestamp
  semantics to today (FR-005). It may execute inline on the producer I/O thread (fast path) or on
  the setup executor (slow path); both are non-blocking actor tells.
- G13. `onFailure` body: the existing KO path — `statsEngine.logResponse(..., KO, ...)` with the
  failure message, `next ! session.markAsFailed` — timing spans request start → failure
  detection. `StatsEngine` and actor tells are thread-safe from any thread; no release call is
  needed (G8: nothing was reserved).
- G14. Send-failure path (`onFailure` of `KafkaSender.send`) is byte-for-byte unchanged; it can
  never hold a tracking reservation (acquisition starts only after a successful ack) — FR-006.

## Thread-role summary

| Thread | May do | Must never do |
|--------|--------|---------------|
| Producer I/O (ack callback) | fast-path CHM ops, refcount incr, actor tells, enqueue readiness request, schedule timeout | park/await, `Future.get`, broker calls |
| Consumer poll thread | complete/fail readiness futures (O(µs)) | run acquisition continuations inline, call `producer.send`, tracker bookkeeping |
| Setup executor (1 daemon thread) | create+register tracker actors, `onReady`/`onFailure` continuations, timeout firing | block, call broker APIs, run user checks |
| Gatling VU / event-loop threads | initiate `sender.send` | acquire trackers synchronously |
