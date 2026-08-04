# Quickstart: Verifying Request-Reply Reliability Hardening

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md) | **Contracts**: [contracts/internal-api.md](contracts/internal-api.md)

How to run the verification for each of the four issues, and what each run should show. Every check
below has a **red** condition — what it must do against pre-change code — because all four are bug
fixes and Principle IV requires the test to fail first.

## Prerequisites

- JDK 17+ and sbt.
- Docker running. Testcontainers pulls `confluentinc/cp-kafka:7.9.5`; the Gatling simulations need
  the Compose stack.
- Git hooks enabled once per clone:

```bash
bash scripts/install-hooks.sh
```

## Default gate (run before every push)

```bash
sbt scalafmtCheckAll scalafmtSbtCheck compile test
```

## Compose stack, for anything Gatling

```bash
docker compose -f docker-compose.kafka.yml up -d
```

---

## #143 — request-reply after a late start

**Unit level** — the poll guard, no broker needed (`poll` raises before any network I/O):

```bash
sbt "testOnly org.galaxio.gatling.kafka.client.DynamicKafkaConsumerSpec"
```

**Integration level** — the whole path, against a real broker:

```bash
sbt "testOnly org.galaxio.gatling.kafka.integration.ConsumerStartupSpec"
```

Build a pool with a short initialization wait (the additive constructor from research R7), request no
topic, let the wait expire, then request one.

- **Red**: `acquireTracker` fails with `Kafka consumer failed; tracker pool can no longer be used`,
  and the consumer logged `Consumer is not subscribed to any topics or assigned any partitions`.
- **Green**: the topic is subscribed and assigned, the tracker is handed over, and no consumer
  failure appears anywhere in the log. Covers **C1–C4**, **P1–P2**, and SC-003/SC-004.

Do not shorten the wait by mutating the companion constant — it is JVM-wide and would leak into other
tests. Use the overload.

---

## #196 — the CI simulation proves a round trip

```bash
sbt "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest"
```

- **Green**: exactly one failure, and it is `Request Reply Bytes wo`. Both assertions hold —
  `global.failedRequests.count.is(1)` and the `details(...)` one.
- **Red for G5**: with the old `lte(1)`, deliberately breaking the timeout scenario (point it at a
  topic the responder *does* serve) still passes. With the new assertion it fails.

**The check that proves replies are no longer coincidental (SC-007)** — comment out every
produce-only scenario (`scn`, `scn2`, `scnAvro4s`, `scnwokey`) from `setUp` and run again:

- **Red**: `scnRR` and `scnRR2` fail — their "replies" were the sibling scenarios' publishes.
- **Green**: both still pass. Restore `setUp` afterwards; this is a manual check, not a committed
  variant.

**The rebalance-delay question (SC-009)** — run once with
`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0` as today, then once with it removed from
`docker-compose.kafka.yml`. State the outcome in the PR either way. Expect it to still be needed:
readiness resolves on assignment, position resolution comes later, and `auto.offset.reset` defaults
to `latest` — that gap is #193 point 3 and is not fixed here.

---

## #166 — nothing outlives its channel

```bash
sbt "testOnly org.galaxio.gatling.kafka.integration.TrackerLifetimeSpec"
sbt "testOnly org.galaxio.gatling.kafka.client.KafkaMessageTrackerSpec"
```

Drive a channel through acquire → release → idle grace, then assert the tracker's periodic scan has
stopped and the entry is gone. Then repeat across at least 20 sequential reply topics.

- **Red**: scan tasks accumulate — one per channel ever created — and each keeps firing once per
  second on the actor system's single shared scheduler thread.
- **Green**: live scan tasks equal channels currently held; zero held means zero firing. Covers
  **P3–P5**, **T7–T8**, and SC-005/SC-006.

A useful manual cross-check: run the CI simulation with the tracker logger at DEBUG and confirm
`Releasing idle tracker` is followed by no further `TimeoutScan` activity for that topic.

---

## #191 — no reply is ever dropped

**Unit level** — the two-phase join, deterministic and broker-free:

```bash
sbt "testOnly org.galaxio.gatling.kafka.client.KafkaMessageTrackerSpec"
```

Send `MessagePublished`, then `MessageConsumed`, then `MessageAcked` — in that order.

- **Red**: the reply is discarded (`sentMessages.remove` returns `None`) and the request later times
  out.
- **Green**: the reply is held and completed when the ack lands, logged with the **ack** timestamp as
  its start. Covers **T1–T3**.

**Integration level** — the real race, forced rather than waited for:

```bash
sbt "testOnly org.galaxio.gatling.kafka.integration.ReplyRegistrationRaceSpec"
```

An in-process echo responder answering far faster than the reply timeout, driving the real
`KafkaRequestReplyAction`, with enough requests to exercise the window repeatedly.

- **Red**: some requests fail with `Reply timeout after Xms` even though the responder answered every
  one — the signature of the defect.
- **Green**: zero reply-timeout failures. Covers **A1–A2** and SC-001/SC-002.

**Load level** — the measured oracle:

```bash
sbt "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaConcurrencyLoadTest"
```

This is the harness whose `KnownReplyLossBudget` documents the baseline: 0–2 KO of ~6,760 across five
runs on current code, 14–17 of ~6,500 before #165. Its own comment says **"Tighten to 0 once #191
lands"** — do that in the #191 commit and confirm five consecutive green runs before claiming SC-001.

**Also part of the #191 commit**: simplify `TrackerLifetimeSpec.send`, which re-publishes the reply in
a loop specifically to work around #191. Leaving it would mask a regression.

---

## Full CI gate

What `.github/workflows/ci.yml` runs, against the Compose stack:

```bash
sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" test coverageOff coverageReport
```

And the API-construction check, which must keep passing untouched (SC-010):

```bash
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"
```

---

## Per-commit checklist

Each of the four commits is green on its own and carries the milestone plus `Closes #NNN`.

| Order | Commit | Must be green |
|---|---|---|
| 1 | `fix(client): do not poll a consumer with nothing to receive on (#143)` | default gate + `ConsumerStartupSpec` |
| 2 | `test(examples): answer request-reply with a real echo responder (#196)` | default gate + `KafkaGatlingTest` on Compose |
| 3 | `fix(client): stop the tracker and its timeout scan on release (#166)` | default gate + `TrackerLifetimeSpec` |
| 4 | `fix(request-reply): register the pending request before sending (#191)` | default gate + `ReplyRegistrationRaceSpec` + `KafkaConcurrencyLoadTest` at budget 0 |

The #191 PR additionally needs maintainer approval for the behaviour change recorded in the plan's
Complexity Tracking table, and a README Migration Guide note under v1.1.0.
