# Internal API & Ordering Contract

**Feature**: [../spec.md](../spec.md) | **Plan**: [../plan.md](../plan.md) | **Research**: [../research.md](../research.md)

The plugin's *published* contract — the Scala DSL, `javaapi`, protocol defaults, wire formats — is
unchanged (spec FR-016). This document covers the internal signatures this feature touches and the
guarantees each fix must hold, with the red/green condition for every one.

It carries forward [001's contract](../../001-nonblocking-tracker-acquisition/contracts/internal-api.md)
(non-blocking acquisition) and [002's](../../002-hold-reply-subscriptions/contracts/internal-api.md)
(idle-released channels). Where this document adds a clause to a guarantee those established, it says
so; nothing here retracts either.

---

## 1. `DynamicKafkaConsumer` — #143

```scala
// unchanged
def requestTopicSubscription(topic: String): CompletableFuture[Void]
def removeTopicSubscription(topic: String): Unit
def close(): Unit
def run(): Unit

// additive overload — production callers keep using the existing one
def apply[K, V](
    settingsMap: Map[String, AnyRef],
    topics: Set[String],
    onRecord: ConsumerRecord[K, V] => Unit,
    onFailure: Exception => Unit,
    initializationTimeout: FiniteDuration,   // new; defaults to 90.seconds via the existing apply
): DynamicKafkaConsumer[K, V]
```

| | Guarantee | Red before | Green after |
|---|---|---|---|
| **C1** | `poll` is never called while the consumer holds neither a subscription nor an assignment. | With no topic requested, the run loop throws `IllegalStateException: Consumer is not subscribed to any topics or assigned any partitions` once the initialization wait expires. | The loop sleeps the poll interval instead and reports no failure. |
| **C2** | A loop turn that skips the poll still applies pending subscription changes. | n/a — the loop has already died. | A topic requested after the wait expired is subscribed on the next turn and its readiness future completes. |
| **C3** | Expiry of the initialization wait sets no failure state. | `onFailure` fires and `markConsumerFailed` latches `consumerFailure`. | No `onFailure`, no `consumerFailure`; `subscriptionUnavailable` stays `None`. |
| **C4** | The wait's outcome is distinguishable in the logs. | The 90 s expiry is silent; the eventual error names nothing about it. | A debug line records that the wait expired with no reply topic requested. |
| **C5** | The "never unsubscribe to nothing" guard from 002 is preserved unchanged. | — | `KafkaIntegrationSpec`'s unsubscribe test still passes untouched. |

---

## 2. `KafkaMessageTrackerPool` — #143, #166

```scala
// unchanged — primary constructor, and both acquisition entry points
class KafkaMessageTrackerPool(
    consumerSettings: Map[String, AnyRef],
    actorSystem: ActorSystem,
    statsEngine: StatsEngine,
    clock: Clock,
)
def acquireTracker(...)(onReady, onFailure): Unit
def releaseTracker(consumerTopic: String, messageMatcher: KafkaMatcher): Unit
private[client] def sweepIdleTrackers(): Unit
@volatile private[kafka] var idleGraceMillis: Long

// additive secondary constructor — tests only; production uses the primary
def this(
    consumerSettings: Map[String, AnyRef],
    actorSystem: ActorSystem,
    statsEngine: StatsEngine,
    clock: Clock,
    initializationTimeout: FiniteDuration,
)
```

| | Guarantee | Red before | Green after |
|---|---|---|---|
| **P1** | A pool whose initialization wait expires with no topic requested still serves the first topic requested afterwards. | `acquireTracker` fails with `Kafka consumer failed; tracker pool can no longer be used`. | The topic is subscribed, assigned, and the tracker handed over. |
| **P2** | A protocol declaring reply settings for a run that performs no request-reply logs no consumer failure. | The failure is logged around the 90 s mark, on a pool nobody uses. | Nothing is logged; shutdown is clean. |
| **P3** | Every path that drops a `TrackerEntry` stops its tracker: the idle sweep, the consumer-failure broadcast, and pool shutdown. | Only the map entry is dropped; the tracker keeps firing `TimeoutScan` once per second for the rest of the run. | The tracker is stopped and its scan cancelled on all three paths. |
| **P4** | `Stop` is sent after the entry is removed, and outside any `ConcurrentHashMap` compute lambda. | — | Follows the precedent `sweepIdleTrackers` already sets for `removeTopicSubscription`. |
| **P5** | Background scan tasks alive at any moment equal the channels currently held. | Equals the number of channels *ever* created. | Equals the number held; zero when none are held. |
| **P6** | Acquisition/release semantics from 002 are unchanged — reaching zero starts an idle clock, it does not release. | — | `TrackerLifetimeSpec` passes unchanged apart from the workaround removal in §4. |

---

## 3. `KafkaMessageTracker` — #166, #191

```scala
sealed trait TrackerMessage
final case class MessagePublished(                      // field list UNCHANGED
    matchId: Array[Byte],
    sentTimestamp: Long,                                // now the handoff time; MessageAcked supersedes it
    replyTimeout: Long,
    checks: List[KafkaCheck],
    session: Session,
    next: Action,
    requestName: String,
    onComplete: () => Unit = () => (),
) extends TrackerMessage
final case class MessageConsumed(received: Long, message: KafkaProtocolMessage) extends TrackerMessage
final case class ConsumerFailure(errorMessage: String) extends TrackerMessage

final case class MessageAcked(matchId: Array[Byte], sentTimestamp: Long) extends TrackerMessage  // new, #191
final case class SendFailed(matchId: Array[Byte], errorMessage: String) extends TrackerMessage   // new, #191
case object Stop extends TrackerMessage                                                          // new, #166
```

| | Guarantee | Red before | Green after |
|---|---|---|---|
| **T1** | A `MessageConsumed` whose record has no ack yet is held on the record, not discarded. | `sentMessages.remove(key)` returns `None`; the reply is dropped and the request times out. | The reply is held and completed when `MessageAcked` arrives. |
| **T2** | A response is logged with the ack timestamp as its start, never the registration timestamp. | — | Reported latency matches today's semantics exactly (FR-017). |
| **T3** | `TimeoutScan` measures from registration, not from the ack. | — | A request whose ack never lands still times out at `replyTimeout`. |
| **T4** | `SendFailed` removes the record, logs KO, and invokes `onComplete` exactly once. | The action logs KO directly; nothing releases the channel, because nothing was acquired. | The channel's in-flight count returns to balance. |
| **T5** | Exactly one terminal event per record, each invoking `onComplete` once. | — | No double-release, no leaked in-flight count. |
| **T6** | A reply matching no record is discarded silently — no failure, no mismatch. | (already holds) | Still holds; `KafkaMessageTrackerSpec`'s existing test passes unchanged. |
| **T7** | `Stop` cancels the periodic scan and returns `die`. | The `Cancellable` is discarded at creation; nothing can cancel it. | The scan stops; the actor drops anything that arrives later. |
| **T8** | `Stop` is idempotent and safe on a tracker that never armed a scan. | — | A tracker that only ever saw `replyTimeout <= 0` stops cleanly. |

---

## 4. `KafkaRequestReplyAction` — #191

```scala
override def sendKafkaMessage(
    requestNameString: String,
    protocolMessage: KafkaProtocolMessage,
    session: Session,
): Unit   // signature unchanged; call order inverted
```

**Order after the change** — this *is* the fix:

```text
1. acquireTracker(...)                            // non-blocking, as 001 established
2.   onFailure → report KO. The record is NOT sent.        ← behaviour change, see plan Complexity Tracking
3.   onReady   → tracker ! MessagePublished(...)           // the record exists here
4.               sender.send(protocolMessage)(             // …and only now can a reply exist
5.                 ack   → tracker ! MessageAcked(id, now)
6.                 error → tracker ! SendFailed(id, msg)
7.               )
```

| | Guarantee | Red before | Green after |
|---|---|---|---|
| **A1** | `MessagePublished` is enqueued before `sender.send` is called. | It is enqueued from the ack callback, after the send. | Program order guarantees it; the MPSC mailbox preserves it against the consumer thread's later enqueue (research R3). |
| **A2** | A reply can never be processed before the request it answers. | Reproducible with a local echo responder and a fast round trip. | Structurally impossible: the reply is causally downstream of a send that is causally downstream of the enqueue. |
| **A3** | Acquisition failure reports KO with the topic and cause, and publishes nothing. | KO is reported, but the record was already published. | Same KO, same message, same response-time span; no publish. |
| **A4** | Send failure reports KO **and** releases the channel. | No channel was held at send time, so nothing needed releasing. | `SendFailed` does both; the in-flight count returns to zero. |
| **A5** | Response-time semantics are unchanged in both directions. | — | Success spans ack → reply received; failure spans request start → failure detection. |
| **A6** | The produce-only action is untouched. | — | `KafkaRequestAction` never acquires a tracker; no diff. |

**Known risk, to be stated in the PR**: on the slow path the send now runs on the pool's
single-threaded setup executor. `KafkaProducer.send` can block up to `max.block.ms` on metadata
resolution or a full accumulator, which would stall concurrent acquisitions. Bounded in practice —
the slow path runs once per reply topic — but it is a real change in which thread does the send.

---

## 5. `KafkaGatlingTest` and the broker definitions — #196

Test-only; no published surface.

| | Guarantee | Red before | Green after |
|---|---|---|---|
| **G1** | Every request-reply scenario is answered by a responder that received its request. | Replies come from sibling produce-only scenarios at fixed `nothingFor` delays. | Deleting every produce-only scenario leaves all request-reply scenarios passing (SC-007). |
| **G2** | Key and value are byte-identical end to end. | — | `jsonPath("$.m").is("dkf")` and `bodyBytes.is("tstBytes")` pass unchanged. |
| **G3** | A response timestamp is available through a header. | No round-trip metadata exists. | The reply carries it; key and value are untouched. |
| **G4** | `scnRRwo` times out on `myTopic4`, which the responder does not consume. | It shares `myTopic2` with `scnRR2`; an echo there would answer it and destroy the only timeout coverage. | It has its own request topic and still times out. |
| **G5** | The assertion fails in both directions. | `global.failedRequests.count.lte(1)` passes when the by-design timeout silently stops failing. | `count.is(1)` plus `details("Request Reply Bytes wo").failedRequests.count.is(1)`. |
| **G6** | `myTopic4` exists in both broker definitions. | — | Present in `docker-compose.kafka.yml` and in `KAFKA_CREATE_TOPICS` in `.github/workflows/ci.yml`. |
| **G7** | Whether `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0` is still needed is answered by running. | Assumed necessary; never retested since PR #190. | Run both ways; the result is stated in the PR either way (SC-009). |

---

## 6. Test-side contract change

`TrackerLifetimeSpec.send` currently re-publishes the reply in a loop, with a comment naming #191 as
the reason. Once #191 lands that loop covers for nothing and must be simplified — a re-published reply
masks a dropped one, so leaving it in place would hide exactly the regression this feature exists to
prevent. This is part of the #191 commit, not a follow-up.
