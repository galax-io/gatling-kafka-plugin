# Phase 0 Research: Reply Correlation Correctness

**Feature**: `004-reply-correlation-correctness` | **Date**: 2026-08-07 | **Plan**: [plan.md](./plan.md)

All findings verified against the working tree at branch point, not against issue text. Where an
issue's description has been overtaken by later work, that is called out — two of them had.

---

## R1 — How an absent key is represented on the wire and in correlation

**Decision**: Carry an absent key as `null` all the way to `ProducerRecord`, and stop folding `null`
into `Array.emptyByteArray` in the tracker's `matchKeyFor`. `null` and empty become distinct.

**Rationale**:

- `KafkaProtocolMessage.key` is already `Array[Byte]` and already carries `null` on the consume side:
  `KafkaProtocolMessage.from` copies `consumerRecord.key()` verbatim, which is `null` for a keyless
  record. The type needs no change — the produce side simply was not using the nullability the
  consume side has always relied on. This is what keeps Principle I satisfied without a deprecation
  cycle.
- `KafkaProtocolMessage.toProducerRecord` passes `key` straight into `ProducerRecord`, so `null`
  reaches Kafka's partitioner and the round-robin path for keyless records is restored with no
  further change. That is the whole of User Story 4.
- The tracker's `MatchKey` needs no new logic to separate the two. `java.util.Arrays.hashCode(null)`
  is `0` while `Arrays.hashCode(Array.emptyByteArray)` is `1`, and `Arrays.equals(null, Array.empty)`
  is `false`. Deleting the substitution is sufficient and adds no branch to a hot path.
- The codebase already models this distinction where it is only cosmetic: `describeBytes`
  (`package.scala:46-58`) renders `"null"` for an absent value and `"bytes(len=0)"` for an empty one.
  Correlation was the one place that collapsed them.

**Alternatives considered**:

- *Keep the empty-array substitution and special-case `""` in the tracker* — the shape #167 suggests.
  Rejected: it fixes correlation but leaves the partitioner defeated, so User Story 4 would need a
  second, separate change to the same line.
- *Wrap the key in `Option[Array[Byte]]` on `KafkaProtocolMessage`* — cleaner in Scala terms, but it
  is a published case-class signature change, so a `!:` major and a deprecation cycle under Principle
  I, for a defect that needs neither.

---

## R2 — Where a request with no correlation identity is failed

**Decision**: In `KafkaRequestReplyAction.sendKafkaMessage`, immediately after
`val id = matcher.requestMatch(protocolMessage)` and **before** `trackers.acquireTracker`, using the
existing local `reportFailure(...)`.

**Rationale**:

- `reportFailure` already exists in that method and already does the whole terminal job: `logResponse`
  with KO, then `next ! session…markAsFailed`. It is used for the acquisition-failure and
  no-consumer-settings paths. Reusing it means no new failure shape.
- It does not weaken the "once registered, every exit goes through the tracker" invariant that #191
  established. That invariant governs exits *after* registration; this exit is strictly before both
  registration and the send, in the same position as the existing acquisition-failure exit.
- Nothing is published, which matches the deliberate choice already recorded at
  `KafkaRequestReplyAction.scala:144-147`: publishing a request whose reply can never be received is
  the state #143 exists to prevent.

**An empty identity is deliberately *not* failed here.** Only `null` is. An empty key is a value the
scenario supplied; if two in-flight requests share it they collide, and the displacement failure added
for #191 (`KafkaMessageTracker.scala:177-185`) reports that accurately as a reused match id. This is
the FR-001 distinction with each case getting the message that fits it:

| Identity | When reported | Message |
|---|---|---|
| Absent (`null`) | At issue time, before send | no identity for the configured matching strategy |
| Empty, unique in flight | Normal correlation | — |
| Empty or otherwise reused while in flight | On collision | match id reused while a request was still in flight |

**Alternatives considered**:

- *Fail inside the tracker on registration* — rejected: the message would already be on the wire, and
  the tracker cannot un-send it.
- *Reject at build time* — cannot be complete, since `attributes.key` is `Option[Expression[K]]` and an
  expression can resolve to nothing per session. Kept available as an addition for the statically
  detectable case (no key expression configured at all), not as a replacement.

---

## R3 — Resolving the fetch position on the poll thread

**Decision**: In `completeAssignedReadiness`, for each assigned partition of a topic awaiting
readiness, call `consumer.position(tp, timeout)` before completing that topic's readiness futures. On
timeout or failure, fail **only that topic's** futures; do not call `markConsumerFailed`.

**Rationale**:

- `position()` is legal from both call sites. `completeAssignedReadiness` is invoked from
  `onPartitionsAssigned` — which the Kafka client runs on the poll thread inside `poll()`, and where
  `position`/`seek` are explicitly supported — and from the tail of `updateSubscription()`, which
  `run()` calls on the same thread. The method already documents "Runs on the consumer thread only,
  since it reads the consumer's assignment."
- `position()` resolves the fetch position, performing a `ListOffsets` round trip when none is cached.
  That is precisely the work that currently happens *after* readiness is declared, and moving it in
  front of the completion is what makes "ready" mean "a reply published from now on will be seen".
- The bounded overload (`position(TopicPartition, Duration)`) exists so a slow or unavailable
  coordinator cannot park the single poll thread indefinitely. That thread timestamps every reply for
  every topic on the consumer, so an unbounded block there is a whole-run stall.
- Failing only the awaiting topic is deliberate. `markConsumerFailed` latches
  `consumerFailure` and fails every pending and future subscription for the rest of the run — the
  terminal state #143 was fixed to prevent. A `ListOffsets` timeout on one topic must not reproduce it.

**Cost**: once per topic per assignment, not per record. The per-reply path is untouched.

**Alternatives considered**:

- *Poll once with a short timeout and treat a completed poll as positioned* — indirect, and a poll that
  returns no records does not prove a position was resolved for the topic being awaited.
- *`seekToEnd` then `position`* — `seekToEnd` is lazy and only sets a pending reset, so it moves the
  problem rather than resolving it; it would also override a legitimate committed offset for a group
  that has one.
- *Set `auto.offset.reset=earliest` by default* — changes a published default (Principle I), and
  replays the entire reply topic history into a load run.

---

## R4 — Does removing the rebalance tuning give a red-before gate? **No.**

**Finding (contradicts #193's premise).** #193 says the readiness gap is "masked, not fixed, by
`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0` in both broker definitions". The working tree says
otherwise. `docker-compose.kafka.yml:25-33` carries a comment recording that the setting was retained
**for speed, not correctness**, and that the simulation was verified green with Kafka's 3000 default:

> KafkaGatlingTest no longer *depends* on it. […] Since #196 gave the simulation a real echo responder
> and #191 made the reply channel exist before the request is sent, an immediate reply is always
> received. Verified by running the simulation three times against this broker with the value at
> Kafka's 3000 default: green each time.

**Decision**: Still remove the setting (FR-022 requires it, and keeping a correctness-shaped workaround
that nobody depends on invites re-masking later) — but treat the removal as **hygiene, not proof**. The
red-before gate for User Story 3 is the new `PositionedReadinessSpec`.

**Why the two facts are compatible**: the rebalance delay changes *when* the first assignment happens.
The defect lives in the window *between* assignment and position resolution, which the delay neither
widens nor narrows. The simulations pass at 3000 because their replies happen to arrive after position
resolution, not because the window is closed.

**Consequence for the spec**: SC-005 reads "where the same verification against the current behaviour
fails". That is satisfied by `PositionedReadinessSpec`, not by the tuning removal. Recorded here so the
task list does not assert a red-before that will not appear, and flagged to the user.

---

## R5 — Making positioned readiness observable, decisively

**Decision**: A Testcontainers integration spec, `PositionedReadinessSpec`, that **measures the size of
the gap** rather than racing to land inside it. Against a fresh topic and consumer group with
`auto.offset.reset=latest`:

1. Start a producer emitting a **continuous numbered stream** to the topic, and let it run before the
   subscription is requested, so the log-end offset is already moving.
2. Request the topic subscription and block on the returned readiness future.
3. The instant readiness completes, read the sequence number the producer has reached — call it `S`.
4. Let the consumer receive; take the sequence number of the **first record actually delivered** — `F`.
5. Assert `F <= S`. One assertion, no tolerance, no repetition.

**Rationale**: readiness completing means "everything published from now on will be seen". `S` is
exactly "now". So `F <= S` *is* the contract, expressed directly.

Pre-fix, readiness completes inside `onPartitionsAssigned` while position resolution is still pending;
the position then resolves to the log-end offset at *resolution* time, which is well past `S` because
the stream never stopped. `F > S`, and the difference is every record produced during the window —
tens of records over milliseconds, not a knife-edge. Post-fix the position is resolved before the
future completes, so `F <= S` always.

**This replaces an earlier statistical design** (publish one marker after readiness, repeat 25 times,
hope to land in the window). That version could go green pre-fix by luck, and a check that passes by
luck proves nothing. The continuous stream converts the race into a measured interval: the assertion is
decisive in both directions on a single run.

**Reporting**: on failure, report `F - S` — the number of replies that would have been silently
skipped. That number is the defect, stated in the units a reader cares about.

**Alternatives considered**:

- *Single marker published after readiness, N iterations, zero tolerance* — the original design.
  Rejected: statistical, slow, and green-by-luck pre-fix is possible. Superseded by the above.
- *Inject a `MockConsumer` and assert `position()` is called before the future completes* — also
  deterministic, but `DynamicKafkaConsumer` builds its `KafkaConsumer` internally, so it needs a new
  injection seam on a published class (Principle III) and asserts consumer lifecycle against a stub,
  which Principle II names explicitly. Not needed now that the broker-level assertion is decisive.
- *Assert on consumer metrics / log-end offset via AdminClient* — proves where the position ended up,
  not that it was resolved before readiness was announced. Measures the wrong edge.

---

## R6 — Absent payload: failure, not empty value

**Decision**: `msg.value == null` produces a `Validation` failure naming the absent payload, in all
three currently unguarded preparers. A **present-but-empty** value keeps today's behaviour exactly.

**Rationale**:

- The shape already exists in the same file. `xmlPreparer` and `avroPreparer`
  (`KafkaMessagePreparer.scala:53-62`) wrap the identical work in `safely(ErrorMapper) { … }`. Bringing
  `stringBodyPreparer`, `bytesBodyPreparer` and `jsonPathPreparer` to that shape is consistency, not a
  new pattern — which is also the strongest evidence the omission was an oversight.
- Returning `""` for an absent payload would make `bodyString.is("")` pass on a tombstone, silently
  equating "the service sent nothing" with "the service sent an empty string". A check that cannot
  distinguish those is worse than one that fails.
- `stringBodyPreparer` and `bytesBodyPreparer` already branch on `value.length > 0` to return `""` /
  `Array.emptyByteArray`, so the empty case is explicitly handled today and must not regress (FR-010).

**Second layer**: `completeMatched` (`KafkaMessageTracker.scala:153-163`) is `try`/`finally` with no
`catch`. Even with the three preparers fixed, any throwing check strands the virtual user — no KO, no
continuation. Adding a `catch` that reports KO and continues makes FR-008 true for all checks rather
than for the three being repaired. Both layers are needed: the preparer fix gives a *good* message, the
catch guarantees a *terminal* one.

---

## R7 — Observing partition placement

**Decision**: Create one explicitly multi-partition topic in `docker-compose.kafka.yml`'s `topic-init`,
publish keyless messages to it from a Gatling scenario, and read placement back with an `AdminClient` /
consumer assertion after the run.

**Rationale**: every topic in `topic-init` is currently created `--partitions 1`, and
`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` means an unlisted topic is auto-created with the broker
default of 1 partition. Without an explicit multi-partition topic there is nothing for the defect to be
visible against — the assertion would pass trivially both before and after.

The assertion must observe placement directly (per-partition end offsets, or consuming and grouping by
partition). The defective behaviour raises no error: all messages arrive, on one partition.

---

## R8 — Wiring `KafkaConcurrencyLoadTest` into CI

**Decision**: Add it to the existing Gatling step in `ci.yml`.

**Rationale**:

- It is already exactly what FR-019 asks for and does not exist elsewhere: 30 concurrent virtual users,
  a ramp, a dedicated echo responder, real DSL/action pipeline. Its header states it is "Not wired into
  CI; run manually", and it already carries a **zero** reply-loss budget with the comment that every
  known source of loss is closed.
- Every request-reply scenario in `KafkaGatlingTest` injects `atOnceUsers(1)`. A one-user-at-a-time
  profile cannot observe a reply attributed to the wrong user, so `KafkaGatlingTest` alone can never
  satisfy FR-019 no matter what assertions it gains.
- Adding it to the existing step rather than a new job keeps one Compose stack and one coverage run.

**Cost**: its injection window is ~110s plus setup, against the existing 120s `maxDuration` for
`KafkaGatlingTest`. Roughly doubles the Gatling step. Accepted — it is the only concurrent coverage
there is.

**Alternative considered**: *raise `KafkaGatlingTest`'s request-reply scenarios to multiple users
instead*. Rejected as a replacement: that simulation's assertions are built around exactly-one
by-design failure (`scnRRwo`), and multiplying users multiplies that expected count, weakening the
pinned-count convention FR-024 depends on. Worth doing **as well**, for the keyless scenarios only,
where the expected count stays derivable.

---

## R9 — Observed during baseline: a first-run reply-timeout flake matching the US3 signature

**Recorded 2026-08-07 while establishing the T003 baseline.** Not a decision — an observation that
bears on §R4 and on how much the Gatling simulations can be trusted as a US3 gate.

`KafkaGatlingTest` was run twice against the same broker (a Compose stack that had been up for three
days, with `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0` still in place):

| Run | Result | Failed events | Errors | Duration |
|---|---|---|---|---|
| 1st, immediately after `compose up -d` re-ran `topic-init` | **FAIL** | 3 (expected 1) | 2 × `Reply timeout after 5000 ms`, 1 × `Reply timeout after 60000 ms` | 65 s |
| 2nd, same stack, nothing changed | **PASS** | 1 (as designed) | 1 × `Reply timeout after 5000 ms` | 10 s |

Both `Request Reply String` and `Request Reply Bytes` timed out in the first run. The echo responder
was demonstrably alive — `awaitResponderReady` probes every route and throws otherwise, and it passed —
so those two requests timed out against a responder that was answering. That is precisely the User
Story 3 signature.

**What this does and does not establish:**

- It does **not** prove the assignment-vs-position gap caused it. A first join to a consumer group after
  a long idle period involves coordinator state this observation cannot see into, and n=1.
- It does show the defect class is reachable in practice, which §R4 could not demonstrate from the
  compose comment alone. §R4's conclusion stands — removing the rebalance tuning is still not a gate —
  but the underlying gap is less theoretical than that section implies.
- It confirms **the Gatling simulations cannot be the US3 gate in either direction**: they are
  timing-dependent enough to fail spuriously on a cold path and pass on a warm one. `PositionedReadinessSpec`
  (§R5) measuring `F <= S` remains the only decisive check.

**Consequence for the work**: none of the plan changes. It reinforces T023 as the gate and is a caution
against reading a green simulation run as evidence that US3 is fixed.

## Resolved unknowns

| Unknown from Technical Context | Resolution |
|---|---|
| Absent-key representation | `null` end to end; `MatchKey` separates it from empty for free — R1 |
| Where to fail an uncorrelatable request | Before `acquireTracker`, via the existing `reportFailure` — R2 |
| Is `position()` safe on the poll thread | Yes, from both call sites; bounded, and fails only the awaited topic — R3 |
| Does dropping the broker tuning prove anything | **No** — hygiene only; the gate is `PositionedReadinessSpec` — R4 |
| How to prove positioned readiness | Continuous stream; assert `F <= S` in one decisive run — R5 |
| Absent vs empty payload semantics | Absent → failure; empty → unchanged; plus a terminal catch — R6 |
| How to observe partition spread | Explicit multi-partition topic + direct placement assertion — R7 |
| Where concurrent coverage comes from | Wire the existing `KafkaConcurrencyLoadTest` into CI — R8 |

**No NEEDS CLARIFICATION markers remain.**
