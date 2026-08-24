# Quickstart: Validating Measurement Truth in Request-Reply

**Feature**: `008-measurement-truth` | **Date**: 2026-08-24

How to prove this feature works. Clause references (`C1`…`C12`) are from
[contracts/behavior-contract.md](./contracts/behavior-contract.md).

## Prerequisites

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Kafka, Zookeeper and Schema Registry. The unit specs bring their own broker via Testcontainers and
need only Docker running; the Gatling harnesses need this stack.

## The gate

```bash
sbt scalafmtCheckAll scalafmtSbtCheck compile test
```

Every commit must be green under this on its own (Constitution V). Then the broker-backed layer:

```bash
sbt "Gatling / test"
```

Runs `KafkaGatlingTest`, `KafkaFailureModesGatlingTest`, `KafkaJavaapiMethodsGatlingTest` and
`KafkaConcurrencyLoadTest`.

## What proves what

| Clause | Where it is proved | How to run just that |
|---|---|---|
| C1 rejection under a derived name | `KafkaRequestReplyActionSpec` | `sbt "testOnly *KafkaRequestReplyActionSpec"` |
| C1 nothing reaches the broker | `KeylessCorrelationSpec` (Testcontainers) | `sbt "testOnly *KeylessCorrelationSpec"` |
| C2 acquisition failure under a derived name, with its real wait | `KafkaRequestReplyActionSpec` | as C1 |
| C3 rejections stay in the failed-request total | `KafkaFailureModesGatlingTest` assertions | `sbt "Gatling / testOnly *KafkaFailureModesGatlingTest"` |
| C4 the declared name reports only sent requests | `KafkaFailureModesGatlingTest` assertions | as C3 |
| C6 build-time refusal, all three languages | a new unit spec over the shared builder | `sbt "testOnly *ConsumerSettingsRequiredSpec"` |
| C7 no regression on the measured path | `KafkaGatlingTest`, `KafkaConcurrencyLoadTest`, `KafkaJavaapiMethodsGatlingTest`, the nine examples | `sbt "Gatling / test"`, then the example projects below |
| C8–C11 uncorrelatable-reply counting and the timeout clause | `KafkaMessageTrackerSpec` | `sbt "testOnly *KafkaMessageTrackerSpec"` |
| C9 end to end against a real tombstone | `KafkaFailureModesGatlingTest`, new value-correlated scenario | as C3 |
| C12 header correlation matches a tombstone | `KafkaFailureModesGatlingTest`, new header-correlated scenario | as C3 |

Spec names for the new files are the intended ones; `/speckit-tasks` fixes them.

## The published examples

Not in this build. Publish under the sentinel version first, then run each consumer project with its
own native task:

```bash
sbt 'set ThisBuild / version := "0.0.0-EXAMPLES-SNAPSHOT"' publishM2
```

```bash
cd examples/scala && sbt "Gatling / test"
```

```bash
cd examples/java && mvn -q verify
```

```bash
cd examples/kotlin && ./gradlew gatlingRun
```

All nine must pass unchanged. None of them exercises a rejection and none uses `details(...)`, so any
failure here is a genuine regression on the measured path (C7), not an expected consequence.

## Reading the result by hand

After `sbt "Gatling / test"`, open the failure-modes run's HTML report from `target/gatling/`.

**What to look for**

1. The requests table carries rows for `Request Reply Keyless Key [rejected: no correlation id]`
   separate from any row for the declared name. (C1, C4)
2. `Request Reply Keyless Key` itself contributes no response-time samples, because every one of its
   requests was rejected. (C4)
3. The errors table still carries the matcher-and-remedy text, and the failure count in the console
   summary is what the harness asserts. (C3)
4. The reply-timeout failure on the value-correlated tombstone scenario names the uncorrelatable
   replies rather than reading as a bare timeout. (C9)
5. Every metric shows as `total` / `ok` / `ko`. The `total` column still blends rejections into the
   run-wide figure — that is the documented limit (C5), not a defect to chase.

## The one-off acceptance step

Do this by hand once, and record it in the PR — the same evidence style
`007-multilang-example-ci-coverage` used for its deliberate-break demonstration.

1. Revert the derived-name change locally and re-run `KafkaFailureModesGatlingTest`. The
   `details("… [rejected: no correlation id]")` assertion must fail to resolve. If it passes, the
   assertion is not testing what it claims.
2. Restore it, then remove the build-time refusal and start a simulation with no `consumeSettings`.
   Confirm the run produces one failed request per virtual user — the behaviour C6 replaces.
3. Point the value-correlated tombstone scenario at a responder that answers normally. Its timeout
   clause must disappear, confirming C9's zero-count branch.

## What this feature does not fix

`global.responseTime` still includes rejection samples. Gatling feeds the run-wide digest without
reference to request name, and its assertion API has no successful-only scope for response time.
Assert on `details("⟨name⟩").responseTime` for latency and on `failedRequests` / `successfulRequests`
for correctness. The Migration Guide says exactly this, and C5 makes saying it a requirement.
