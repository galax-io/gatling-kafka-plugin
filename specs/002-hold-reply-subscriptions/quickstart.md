# Quickstart: Validating Run-Scoped Reply Channels

**Feature**: [spec.md](spec.md) | Contracts: [contracts/internal-api.md](contracts/internal-api.md)

## Prerequisites

- JDK 17+, sbt, Docker running (Testcontainers pulls the Kafka image on first run)
- For the full CI-equivalent Gatling line: `docker compose -f docker-compose.kafka.yml up -d`

## Red first (constitution IV)

The regression spec lands before the fix and must fail against pre-fix code:

```bash
sbt "testOnly org.galaxio.gatling.kafka.integration.TrackerLifetimeSpec"
```

Expected **pre-fix** failures (the broker's `group.initial.rebalance.delay.ms` is raised, so every
re-establishment costs seconds — see research R5):

- "registration survives request completion": fails — after the first reply is matched, the
  action's completion wiring releases the tracker and the map entry is gone (spec SC-007).
- "second request reuses the channel without re-establishment": fails — the group emptied on
  unsubscribe, so request 2 pays the full initial rebalance delay again (the per-request
  rebalance of issue #165, reproduced by the broker itself).
- "a second scenario does not disturb the first": fails — the shared reply consumer stops polling
  for the whole of each rebalance the second scenario triggers, so the first scenario's replies are
  detected seconds after they arrived (spec SC-003).

The red run is slow by design: at 50 sequential requests against a 5 s induced establishment cost
it spends ~4 minutes re-establishing. `munitTimeout` is set to 10 minutes for that reason. The
post-change run of the same tests takes seconds.

## Green (post-fix)

Same command. Expected assertions, mapping to spec success criteria:

| Assertion | Spec |
|-----------|------|
| The `(topic, matcher)` registration is the same instance across sequential requests; establishment happened once | SC-001, FR-001/002/004 |
| Second request's send→OK wall clock under 1500 ms against a 5 s rebalance delay | SC-002, FR-003, US1 |
| With a second scenario looping on its own topic pair, the first scenario's median response time stays within 1.5× its solo baseline | SC-003, FR-007, US1 |
| Request after an idle gap longer than the previous request's duration reuses the held channel | US2 (asserted inside US1's test 2, where the gap is also what makes the pre-change red deterministic) |
| 100 requests across overlapping virtual users with pauses: zero failures attributable to reply-channel availability | SC-004, US2 |
| A third-party message produced onto the held reply topic yields zero logged responses and no KO | SC-005, FR-008 |
| All tracked requests report OK with round-trip-only timings | FR-010 |

Unit-level pin-downs (no broker, allowed scope):

```bash
sbt "testOnly org.galaxio.gatling.kafka.client.KafkaMessageTrackerSpec"
```

- unmatched `MessageConsumed` discards silently (FR-008)
- completion has no side effects beyond stats + `next` (post-`onComplete` shape)

## Full verification (must all pass unchanged — SC-008, FR-011)

```bash
sbt scalafmtCheckAll scalafmtSbtCheck compile test
```

```bash
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"
```

CI-equivalent Gatling simulations against the Compose stack (sequential request-reply under load —
the throughput witness for US1):

```bash
sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" test coverageOff coverageReport
```

## What changed where (for reviewers)

- `client/KafkaMessageTrackerPool.scala` — map value `ActorRef` (was `TrackerEntry`+refcount);
  plain-read fast path; `releaseTracker` removed (contract H5–H9)
- `client/DynamicKafkaConsumer.scala` — subscribe-and-hold; `removeTopicSubscription`,
  `topicsToRemove`, and `updateSubscription` removal handling removed (H1–H4)
- `client/KafkaMessageTracker.scala` — `MessagePublished.onComplete` removed; completion is
  stats + `next` only (H10–H12)
- `actions/KafkaRequestReplyAction.scala` — no completion callback (H13–H14)
- Tests: `TrackerLifetimeSpec` (new, red-first); `KafkaMessageTrackerSpec`,
  `TrackerAcquisitionIsolationSpec` (probe migration off `onComplete`);
  `DynamicKafkaConsumerSpec`, `KafkaIntegrationSpec` (removal-capability tests deleted);
  `TrackerRefCountSpec` (deleted — the algorithm it mirrors no longer exists; its surviving
  concern, concurrent get-or-create convergence, is broker-tested in
  `TrackerAcquisitionIsolationSpec`)

Detailed task breakdown arrives with `/speckit-tasks` → `tasks.md`.
