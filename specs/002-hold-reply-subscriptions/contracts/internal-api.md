# Internal API & Lifetime Contract

**Feature**: [../spec.md](../spec.md) | **Plan**: [../plan.md](../plan.md)

The plugin's *published* contract (Scala DSL, `javaapi`, protocol defaults, wire formats) is
unchanged — spec FR-011. No signature in this document changes; what changes is when a release
fires. This supersedes the lifetime clauses of 001's
[internal-api.md](../../001-nonblocking-tracker-acquisition/contracts/internal-api.md) (G9's
release-pairing); everything else from 001 carries forward.

## 1. `KafkaMessageTrackerPool`

```scala
def acquireTracker(...)(onReady, onFailure): Unit          // signature unchanged
def releaseTracker(consumerTopic: String, m: KafkaMatcher): Unit  // signature unchanged, semantics changed
private[client] def sweepIdleTrackers(): Unit              // new
@volatile private[kafka] var idleGraceMillis: Long         // new, internal
```

- **H1.** `acquireTracker` increments the entry's in-flight count; `releaseTracker` decrements it.
  The count measures requests in flight and nothing else.
- **H2.** Reaching zero records `idleSince` and releases nothing. This is the whole of the #165 fix:
  "nothing in flight" is not "never needed again".
- **H3.** `sweepIdleTrackers` runs on the setup executor every `idleSweepIntervalMillis` and removes
  entries whose count is zero and whose idle time is at least `idleGraceMillis`. Every removal
  re-checks both conditions inside `computeIfPresent`, so a concurrent acquisition either increments
  first (entry survives) or finds it gone (slow path) — never both.
- **H4.** A topic left with no entries is unsubscribed by the sweep, subject to H8.
- **H5.** The fast path re-reads `consumerFailure` after the map lookup and before handing the
  tracker over, so a request acquired just before a failure broadcast reports the real cause instead
  of waiting out its reply timeout.
- **H6.** `idleGraceMillis` is `private[kafka]`, deliberately not a constructor parameter (binary
  compatibility) and not a protocol option (Principle I). Production never writes it.
- **H7.** Teardown is unchanged: the existing `registerOnTermination` chain closes the consumer,
  cancels the sweep, drains the setup executor, and the actor system stops the trackers.

## 2. `DynamicKafkaConsumer`

```scala
def requestTopicSubscription(topic: String): CompletableFuture[Void]   // unchanged (001 G1–G5)
def removeTopicSubscription(topic: String): Unit                        // unchanged
```

- **H8.** `updateSubscription` never unsubscribes down to an empty set. A consumer with neither a
  subscription nor an assignment throws on its next poll and fails the pool for the run (#143). Idle
  release makes emptying the set routine rather than rare, so the last subscription is kept: one idle
  topic per pool costs a fetch that returns nothing, where the crash is terminal.
- **H9.** Otherwise add/remove coalescing, readiness parking and failure draining are 001's, verbatim.

## 3. `KafkaMessageTracker` / `KafkaRequestReplyAction`

- **H10.** `MessagePublished.onComplete` is retained and still wired to `releaseTracker`. It is the
  signal the idle clock is built on.
- **H11.** An unmatched `MessageConsumed` is logged and discarded: no stats entry, no `next` tell.
- **H12.** Timestamp semantics are unchanged (FR-010).

## Thread roles

| Thread | May do | Must never do |
|--------|--------|---------------|
| Producer I/O (ack callback) | fast-path lookup, actor tells, enqueue readiness | park/await, broker calls, release |
| Consumer poll thread | complete readiness, deliver records, apply subscription changes | run continuations inline, release |
| Setup executor | continuations, timeouts, **the idle sweep** | block, call broker APIs |
| Tracker actor | match, check, log, tell `next`, `onComplete` | unsubscribe directly |
