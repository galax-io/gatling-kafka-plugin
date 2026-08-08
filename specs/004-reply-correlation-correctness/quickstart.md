# Quickstart & Validation Guide: Reply Correlation Correctness

**Feature**: `004-reply-correlation-correctness` | **Date**: 2026-08-07

How to run the verification for this feature and what each command proves. Design detail lives in
[plan.md](./plan.md), [research.md](./research.md) and [contracts/behavior-contract.md](./contracts/behavior-contract.md).

---

## Prerequisites

- JDK 17+ (Temurin, as in CI) and sbt
- Docker, for both Testcontainers and the Compose stack
- Git hooks enabled once per clone:

```bash
bash scripts/install-hooks.sh
```

---

## Fast loop — no broker needed

Unit specs and formatting. This is the loop to stay in while editing the preparers.

```bash
sbt scalafmtAll scalafmtSbt
```

```bash
sbt scalafmtCheckAll scalafmtSbtCheck compile
```

---

## Default verification

The repo's standard gate. `sbt test` includes the Testcontainers integration specs, so Docker must be
running — this is where `KeylessCorrelationSpec` and `PositionedReadinessSpec` execute.

```bash
sbt scalafmtCheckAll scalafmtSbtCheck compile test
```

---

## Targeted specs, per user story

**US1 — correlation identity** (unit: `null` and empty are distinct match keys):

```bash
sbt "testOnly org.galaxio.gatling.kafka.client.KafkaMessageTrackerSpec"
```

**US1 — correlation identity** (integration: concurrent keyless request-reply, real broker):

```bash
sbt "testOnly org.galaxio.gatling.kafka.integration.KeylessCorrelationSpec"
```

**US2 — absent payloads** (unit: every preparer agrees, no Kafka interaction):

```bash
sbt "testOnly org.galaxio.gatling.kafka.checks.KafkaMessagePreparerSpec"
```

**US3 — positioned readiness** (integration: the red-before gate for this story — see
[research.md](./research.md) §R4 for why removing the broker tuning is *not* the gate):

```bash
sbt "testOnly org.galaxio.gatling.kafka.integration.PositionedReadinessSpec"
```

---

## Full CI gate — Compose stack

The Gatling simulations need Kafka, Zookeeper and Schema Registry.

**1. Start the stack** (`topic-init` creates the fixed topics, including the new multi-partition one
for US4):

```bash
docker compose -f docker-compose.kafka.yml up -d
```

**2. Examples still construct against the current API** (the Principle I gate — a failure here is an
API break to reconsider, not a check to relax):

```bash
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"
```

**3. The Gatling simulations under coverage**, exactly as CI runs them. `KafkaConcurrencyLoadTest` is
added to this line by this feature (R8) — it is the only concurrent request-reply coverage that exists:

```bash
sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaConcurrencyLoadTest" test coverageOff coverageReport
```

**4. Tear down**:

```bash
docker compose -f docker-compose.kafka.yml down -v
```

---

## Proving each story red-before, green-after

Constitution Principle IV and FR-025 require the ordering to be **demonstrated**, not assumed. For each
story: stash the production change, run the command, see it fail, restore, see it pass.

| Story | Command | Expected before the fix |
|---|---|---|
| US1 | `testOnly …integration.KeylessCorrelationSpec` | replies misattributed or lost across concurrent keyless users |
| US1 | `Gatling / testOnly …KafkaConcurrencyLoadTest` | reply losses above the zero budget |
| US2 | `testOnly …checks.KafkaMessagePreparerSpec` | NPE from the three unguarded preparers |
| US2 | `Gatling / testOnly …KafkaGatlingTest` | tombstone scenario hangs; users never complete |
| US3 | `testOnly …integration.PositionedReadinessSpec` | `F > S` — records produced during the window are skipped |
| US4 | `Gatling / testOnly …KafkaGatlingTest` | every keyless message on a single partition |

**How to read a US3 failure**: the spec asserts `F <= S`, where `S` is the producer's sequence number
at the moment readiness completed and `F` is the first record actually delivered. A failure reports
`F - S` — the number of replies that would have been silently skipped in a real run. One run is
decisive in both directions; there is no iteration count to raise and no flake budget to spend.

**US3 non-caveat**: removing `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` from the broker definitions will
**not** turn CI red pre-fix. `docker-compose.kafka.yml:25-33` records that the simulations were already
verified green with Kafka's 3000 default. Remove it because FR-022 requires it and because a
correctness-shaped workaround nobody depends on invites re-masking — not as evidence.

---

## What "done" looks like

- [ ] `sbt scalafmtCheckAll scalafmtSbtCheck compile test` green
- [ ] `ExampleSmokeValidation` green — no README or example simulation broke
- [ ] All three Gatling simulations green against the Compose stack
- [ ] Each of US1–US4 has a check demonstrated red-before and green-after (SC-009)
- [ ] `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` absent from `docker-compose.kafka.yml` and `ci.yml` (SC-010)
- [ ] `KafkaConcurrencyLoadTest` runs in `ci.yml` (FR-019)
- [ ] `README.md` Migration Guide covers C1, C3 and C4 (FR-017)
- [ ] Three semantic commits, one per issue, each green on its own, each with `Closes #NNN` and the
      `v1.2.0 Reply correlation correctness` milestone

---

## Troubleshooting

**Testcontainers cannot start** — Docker must be running and able to pull `confluentinc/cp-kafka`. The
integration specs are part of `sbt test`, not a separate task, so this blocks the default gate.

**Port already in use** — the Compose stack binds 2181, 9092, 9093, 9094. Tear down a previous stack
with `docker compose -f docker-compose.kafka.yml down -v`.

**`PositionedReadinessSpec` is green pre-fix** — that is not flake, it is a finding. The spec measures
an interval rather than racing into a window, so it does not pass by luck. Check that the producer
stream is actually running before the subscription is requested; if the stream is idle at that moment,
`S` and `F` collapse together and the assertion becomes vacuous.

**A simulation hangs instead of failing** — that is the US2 symptom itself. Check whether the run is
producing KOs at all; a stalled virtual user produces none, which is why every US2 assertion pairs a
failure count with evidence that users completed.
