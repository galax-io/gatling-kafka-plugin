# Phase 0 Research: Run-Scoped Reply Channels

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md) | **Date**: 2026-08-04

No `NEEDS CLARIFICATION` markers existed in the Technical Context; research resolved the six
design unknowns below. Code references are to the current branch base (`f8a4c6b`).

## R1. Lifetime model: release on idleness, not on completion

**Decision**: Keep the refcount and `releaseTracker`, but stop treating "refCount reached zero" as
"tear the channel down". Zero starts an idle clock; a pool-owned sweep releases a channel only after
it has had nothing in flight for a grace period (default 30 s).

**Rationale**:
- `refCount` accurately measures *requests in flight*. It does not measure *future use*. Releasing at
  zero conflates the two, and in a sequential scenario that inference is wrong after every single
  request — the channel is destroyed milliseconds before it is needed again. That, precisely, is #165.
- The opposite reading — "used once, therefore needed forever" — is the same error mirrored, and it
  re-opens #78 (see R2).
- Idleness separates the two statements. A sequential scenario re-acquires within milliseconds and
  keeps its channel; a reply topic derived per virtual user goes idle after its single request and is
  reclaimed.
- The release moves off the request path entirely: it happens on the pool's setup executor, not in the
  producer callback that `onComplete` runs on.

**Alternatives considered**:
- *Hold for the whole run* (the first draft of this feature): fixes #165 and reverts #78. Rejected —
  see R2.
- *Release at refcount zero* (the pre-existing behaviour): fixes #78 and is #165. Rejected.

## R2. Why "hold for the run" was wrong, and how it was caught

**The first version of this feature deleted the release machinery entirely.** That fixed #165 and
silently reverted commit `0ae53a1` ("fix: release tracker and unsubscribe topic after dynamic reply
request completes (#120)", *Fixes #78*), which has been in `main` since v0.22.10. Its message
describes exactly the behaviour deletion restores:

> KafkaMessageTrackerPool accumulated one tracker actor per unique consumerTopic forever, and
> DynamicKafkaConsumer only ever grew its subscription set. Under dynamic reply-topic patterns this
> caused unbounded memory growth and progressively more expensive rebalances.

`replyTopic` is `Expression[String]` in Scala and `Function<Session,String>` in the Java facade, so
per-user reply topics are first-class API, not an exotic case. The consequence is not linear-and-benign:
each new topic re-subscribes and rebalances, and past roughly 10-20k distinct topics the consumer
group's metadata record exceeds `message.max.bytes`, which trips `markConsumerFailed` — a one-way
latch that fails every subsequent acquisition for the rest of the run.

**How the research missed it**: this document originally rejected the idle-TTL option that issue #165
itself offered ("Hold subscriptions for the simulation's duration, **or** add an idle TTL / grace
period before unsubscribing"), citing #193's target design, and argued a TTL "reintroduces the same
churn at its boundary". That argument is wrong — at the grace boundary a channel is released once, not
once per request. More importantly, no one asked why the code being deleted existed. `git log -S
releaseTracker` names #78 in one command.

**Consequence for the constitution check**: Principle II's real-broker requirement is what surfaced
this in the end — the new idle-release test immediately hit issue #143 (the consumer crashes when the
last subscription is removed), which the hold-forever version had hidden rather than fixed.

## R3. `MessagePublished.onComplete` stays

**Decision**: Keep it. It is how a completing request tells the pool that one in-flight count has
gone, which is exactly the signal the idle clock is built on.

**Rationale**: the first draft removed it as dead weight once release was gone. With release
restored on an idle trigger, it has a live production caller again
(`KafkaRequestReplyAction` → `releaseTracker`). What changed is only what `releaseTracker` does with
the signal.

## R4. `DynamicKafkaConsumer.removeTopicSubscription` stays, but never empties the subscription

**Decision**: Keep the removal machinery. Add one constraint: `updateSubscription` never
unsubscribes down to an empty set.

**Rationale**: a consumer with no subscription and no assignment throws
"Consumer is not subscribed to any topics" on its next poll, which fails the pool for the rest of the
run — issue #143. Idle release makes emptying the set a routine event rather than a rare one, so the
last subscription is deliberately kept: one idle topic per pool costs a fetch that returns nothing,
where the crash is terminal.

This was not predicted; it was found by the new idle-release test, which failed on its second request
with `Kafka consumer failed; tracker pool can no longer be used`. It narrows #143's trigger rather
than closing the issue — #143 also covers the defensive behaviour a consumer should have when it is
genuinely left with nothing, which stays open.

**Affected tests**: `KafkaIntegrationSpec`'s unsubscribe test now uses two topics — it asserts that
removal stops delivery for the removed topic *and* that the remaining subscription keeps delivering,
which is the new contract. `DynamicKafkaConsumerSpec`'s queue test is unchanged.

## R5. Reproducing per-request churn red-first against a real broker

**Decision**: New integration spec `TrackerLifetimeSpec` (Testcontainers), driving the **real
`KafkaRequestReplyAction`** — not the pool directly — with the 001 stall technique
(`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` set to seconds). Sequence: request A → produce reply →
matched (OK in `RecordingStatsEngine`); request B on the same topic pair; assertions:

1. the pool's registration for `(topic, matcher)` is the **same instance** before and after A
   completes (reflection over the `trackers` map with reference-equality compare — type-agnostic,
   so the assertion compiles against both pre- and post-change map shapes);
2. B's wall clock from action invocation to its OK response is far below the broker's rebalance
   delay (pre-change: A's completion released the tracker and unsubscribed; the group emptied, so
   B's re-subscribe pays the *full initial rebalance delay again* — the per-request rebalance of
   issue #165, reproduced by the broker itself, not injected);
3. both A and B are OK, and exactly two responses were logged.

The same spec carries the two adjacent behavioral witnesses: an unmatched third-party message
produced onto the held reply topic yields zero logged responses and no KO (FR-008 / SC-005), and
a request after an idle gap longer than A's duration still reuses the held registration (US2).

**Rationale**:
- **The action must be in the loop.** The release that causes the churn is wired inside the
  action's `onComplete` closure (`KafkaRequestReplyAction.scala:81`). A pool-level test cannot
  trigger it without naming `releaseTracker`/`onComplete` — which do not exist post-change, so
  such a test body cannot compile in both worlds. Driving the real action keeps the test body on
  surfaces this feature does not change (action constructor, sender, stats engine), so the
  identical test is red pre-change and green post-change — Principle IV's requirement, honestly.
- Long-term, the test guards the actual invariant ("completion of a request never tears down the
  reply channel") against *any* future wiring, not one method name.
- Broker-real per Principle II: the rebalance cost asserted on is produced by a real group
  emptying and re-forming, the exact production failure mode.

**Harness note** (verified against `gatling-core-3.13.5.jar`): `CoreComponents` is a plain 8-arg
constructor `(actorSystem, eventLoopGroup, controller, throttler, statsEngine, clock, exit,
configuration)` and `GatlingConfiguration.loadForTest()` is public on the companion object. The
request-reply path reads only `statsEngine` and `clock`, so the remaining members can be null /
`None`. Tests call `action.sendKafkaMessage(name, message, session)` directly, bypassing EL
resolution, so `KafkaAttributes` needs only `checks`.

**Assertion (1) is the primary red witness, and it is timing-independent.** Reading the pool's
registration after request 1 completes returns null pre-change (the entry was removed) and the
same instance post-change. It needs no broker timing at all, so it cannot flake.

**Assertion (2) needs a deliberate idle gap, because of the #164 coalescing hazard.** Pre-change,
the removal is queued by `releaseTracker` but applied only on the consumer's next
`updateSubscription`. If request 2's subscription request lands in the *same* cycle, add and
remove coalesce into an unchanged topic set: `subscribe()` is skipped, readiness resolves
instantly from the still-live assignment, and the churn is invisible — the test would pass against
the defect. A gap longer than the 1 s poll timeout (~3 s) guarantees the removal is applied and
the group is genuinely empty, so re-subscribing pays the full initial rebalance delay again. The
gap is not scaffolding: it is spec US2's idle-gap scenario, so the same test serves both stories.

**Alternatives considered**:
- *Pool-level test with reflective release-if-present*: post-change it degenerates into "acquire
  twice returns the same entry", which passes pre-change too if nothing releases — guards nothing.
  Rejected.
- *Assert on the CI Gatling simulation's report timings*: nondeterministic, coarse, and reachable
  only through the full compose stack. Rejected as the red/green witness (it remains SC-008's
  regression net).

## R6. End-of-run release (FR-009) — verify, don't build

**Decision**: No new shutdown mechanism. FR-009 is satisfied by lifecycle that already exists and
is not weakened by holding entries:

- The pool is created per consumer-config fingerprint per run and evicted on actor-system
  termination (`KafkaProtocol.scala:80-92`), with LIFO ordering guaranteeing the pool's own
  consumer-close hook (registered in the pool constructor,
  `KafkaMessageTrackerPool.scala:125-148`) runs first — consumer closed, executors drained, then
  eviction, so a subsequent simulation constructs a fresh pool.
- Held tracker actors belong to Gatling's per-run `ActorSystem` and terminate with it; the
  periodic timeout-scan timers belong to the actor scheduler and die with the system (their
  *within-run* accumulation is #166, untouched here).
- Shutdown-while-establishing is 001's drain semantics, untouched: pending readiness fails
  exceptionally on close (`DynamicKafkaConsumerSpec` "close fails readiness futures").

**SC-006 witness**: the integration suites already run many consecutive
pool-construct/pool-teardown cycles inside one JVM (`withPoolAndSender` per test); cross-run
leakage of a subscription or receiving thread would surface there as cross-test interference. No
additional two-simulation test is added; this is recorded as the standing witness rather than a
new artifact.

**Alternatives considered**: an explicit `releaseAll()` API on the pool — redundant with
`registerOnTermination` teardown and a second lifecycle entry point to keep consistent; rejected.
