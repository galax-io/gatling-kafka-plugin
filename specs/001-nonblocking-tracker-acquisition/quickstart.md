# Quickstart: Validating Non-blocking Reply-Tracker Acquisition

**Feature**: [spec.md](spec.md) | Contracts: [contracts/internal-api.md](contracts/internal-api.md)

## Prerequisites

- JDK 17+, sbt, Docker running (Testcontainers pulls the Kafka image on first run)
- For the full CI-equivalent Gatling line: `docker compose -f docker-compose.kafka.yml up -d`

## Red first (constitution IV)

The regression spec lands before the fix and must fail against pre-fix code:

```bash
sbt "testOnly org.galaxio.gatling.kafka.integration.TrackerAcquisitionIsolationSpec"
```

Expected **pre-fix**: the "healthy topic unaffected by a stalled topic" assertion fails — the
healthy request's completion time is dragged to ≈ the poisoned topic's full reply timeout because
the producer callback thread is parked (spec SC-001/SC-003).

## Green (post-fix)

Same command. Expected assertions, mapping to spec success criteria:

| Assertion | Spec |
|-----------|------|
| Healthy-topic request-reply completes in normal time (≪ reply timeout) while the poisoned topic's preparation is pending | SC-001, US1 |
| Delivery confirmations keep flowing during the entire stall window (follow-up sends confirm promptly) | SC-002, FR-002 |
| Poisoned-topic request KOs at ≈ reply timeout with an error naming the topic and duration | SC-004, US2 |
| After the KO: a healthy request still OK; a fresh poisoned-topic request attempts preparation again (KOs again, not instantly poisoned) | FR-003 |
| Concurrent first use of one new topic: channel prepared once, all requests proceed | FR-007 |

## Full verification (must all pass unchanged — SC-005, FR-008)

```bash
sbt scalafmtCheckAll scalafmtSbtCheck compile test
```

```bash
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"
```

CI-equivalent Gatling simulations against the Compose stack (latency-semantics witness for
FR-005 — request-reply timings in the report stay in line with current releases):

```bash
sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" test coverageOff coverageReport
```

## What changed where (for reviewers)

- `client/DynamicKafkaConsumer.scala` — readiness futures replace latches;
  `requestTopicSubscription` (contract G1–G5)
- `client/KafkaMessageTrackerPool.scala` — `acquireTracker(onReady, onFailure)` + setup executor
  (G6–G10)
- `actions/KafkaRequestReplyAction.scala` — non-blocking callback wiring (G11–G14)
- Tests: `TrackerAcquisitionIsolationSpec` (new), migrations in `DynamicKafkaConsumerSpec`,
  `KafkaMessageTrackerPoolSpec`, `TrackerRefCountSpec`, `KafkaIntegrationSpec`

Detailed task breakdown arrives with `/speckit-tasks` → `tasks.md`.
