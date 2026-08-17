# Quickstart: Verifying the v2.0.0 Cleanup Sweep

**Feature**: `006-v2-cleanup-sweep` | **Date**: 2026-08-09

How to prove each story did what it claims. Every command below is runnable from the repository root.
Nothing here is implementation code — see `tasks.md` for that.

---

## Prerequisites

- JDK 17+ (Temurin), sbt, Docker
- The Compose stack for the Gatling simulations:

```bash
docker compose -f docker-compose.kafka.yml up -d
```

- Git hooks enabled once per clone (formats on commit):

```bash
bash scripts/install-hooks.sh
```

## The standard gate — run after every story

Each of the five commits must be green on its own under:

```bash
sbt scalafmtCheckAll scalafmtSbtCheck compile test
```

`checkPublishedPom` runs as part of `Test / test`, so this command also gates the dependency contract.

The full CI equivalent, which needs the Compose stack up:

```bash
sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaConcurrencyLoadTest" test coverageOff coverageReport
```

---

## US1 — Every reachable entry point works

**1. No `send` without a topic survives.** Every `send` must be reachable only after `.topic(...)` or
`requestReply()...replyTopic(...)`:

```bash
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"
```

Expected: every README and example simulation still constructs. A failure here is an API break to
reconsider, not a check to relax.

**2. The inherited dependency set shrank to three.** Generate the POM and read the `compile`/`runtime`
scopes:

```bash
sbt makePom
```

Expected in `target/scala-2.13/*.pom`: `scala-library`, `kafka-clients`, `avro` — and **no**
`kafka-streams-scala`. Baseline measured before the change was those three plus `kafka-streams-scala`.

**3. The POM contract passes without the deprecation allowance:**

```bash
sbt checkPublishedPom
```

Expected: success, reporting three declared inherited dependencies, all Central-resolvable and
justified. Rule DR-4 and its check are gone; C1, C2, C3 and C5 still run.

**4. Failure reporting is unchanged.** The response-code *field* is gone; the reported failure *type*
is not:

```bash
sbt "testOnly org.galaxio.gatling.kafka.integration.* -- --tests=*failure*"
```

Expected: failures still name their cause (`TimeoutException` and friends). `KafkaLoggingSpec` asserts
the trace line's new exact text — updated, not relaxed to a substring match.

## US2 — Dead code gone, guard live

**1. Watch the guard go red first** (this is the red half of red-green; do it before the cleanup):

```bash
sbt -Dsbt.color=false 'set root / scalacOptions += "-Wunused:imports,privates,locals,patvars"' compile Test/compile
```

Expected before the sweep: **23 findings** — 22 unused imports across 12 files, plus one unused private
type in `checks/AvroBodyCheckBuilder.scala`. That count is the measured baseline from research R2. A
different count means the sources moved and the extra findings need verdicts before deletion.

**2. Green after the sweep**, with the flag now committed in `build.sbt` and `-Xfatal-warnings` in
force:

```bash
sbt clean compile Test/compile
```

Expected: success, zero warnings.

**3. The guard actually bites.** Add an unused import to any source file, then:

```bash
sbt compile
```

Expected: the build **fails** and names the import. Revert.

**4. No suppressions bought the green** (SC-003):

```bash
grep -rn "nowarn" src/ build.sbt project/
```

Expected: no matches.

**5. `idleSweep.cancel(false)` is still there** — verdict C1 refused this deletion:

```bash
grep -n "idleSweep" src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTrackerPool.scala
```

Expected: both the `scheduleAtFixedRate` assignment and the `cancel(false)` in the termination hook.

## US3 — Freeze artifacts simplified

**1. Classpath isolation still holds, at the new boundary:**

```bash
sbt "testOnly org.galaxio.gatling.kafka.classpath.PlainClasspathIsolationSpec"
```

Expected: all cases pass, including the positive control. The re-pointed case must assert the new
boundary — initialising an entry point succeeds, *summoning* `avroSerde` under the denying loader
fails — rather than accepting either outcome. If the positive control ever passes while the rest are
trivially satisfied, the suite is proving nothing; that is what it exists to prevent.

**2. Avro still works end to end** (needs the Compose stack, including Schema Registry):

```bash
sbt "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest"
```

Expected: the Avro request-reply scenarios pass. This is what proves the `implicit def` resolves — an
implicit ambiguity would already have failed at compile time.

**3. `send` returns the concrete builder.** Covered by compilation: `ExampleSmokeValidation` plus the
example simulations exercise the implicit conversion to Gatling's `ActionBuilder`.

## US5 — Kotlin examples compile

Not compiled by this build, by decision. Verify the way a Kotlin user would — in a scratch project
**outside** this repository:

1. `sbt publishLocal` to get the plugin into the local Ivy repository.
2. Create a throwaway Kotlin/Gradle project depending on that artifact plus `gatling-core-java` and
   `gatling-charts-highcharts`.
3. Copy all four files from `src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples/` in.
4. Build it.

Expected: all four compile, `ProducerSimulation.kt` included. Record the result in the PR; check
nothing about the scratch project in.

Then confirm the layout did not move and no toolchain was added:

```bash
ls src/test/kotlin/org/galaxio/gatling/kafka/javaapi/examples/
grep -in "kotlin" build.sbt project/plugins.sbt project/Dependencies.scala
```

Expected: four `.kt` files still in place; no Kotlin reference in any build file.

## US4 — Only tests that can fail

**1. The suite's verdict is unchanged:**

```bash
sbt test
```

Expected: green, with the removed cases gone. Same pass/fail outcome as before the sweep.

**2. Every removal named a survivor** (FR-018, data-model TR-1). Check the commit body: each removed
test must name the retained test that detects the same failure. A removal with no survivor does not
ship.

**3. The strengthened assertion genuinely fails first** (FR-019, TR-2). Before keeping it, revert the
guard it protects and confirm the new assertion goes red; then restore. An assertion that passes both
ways is the vacuous one it replaced.

**4. Race-pinning tests untouched** (FR-022, TR-3):

```bash
sbt "testOnly org.galaxio.gatling.kafka.integration.TrackerLifetimeSpec"
```

Expected: tests (2), (6) and (7) still present and passing. The redesign that would retire them is not
part of this feature.

---

## Before the release tag

Produce and review the break-surface record — see
[contracts/removed-api.md](./contracts/removed-api.md) for the full contract.

1. Fetch the baseline: `org.galaxio:gatling-kafka-plugin_2.13:1.3.0` from Maven Central (verified
   available during research).
2. Extract public signatures from both jars with `javap -public` and diff them. Do not hand-transcribe.
3. Match every entry to a verdict (`A1`–`A12`, `B1`, or a US3 freeze artifact). **An entry with no
   verdict blocks the release** — it is a cascade to record, a mistake to revert, or dead surface that
   needs a verdict added to `spec.md` with evidence first.
4. Confirm every `published` entry also appears in the README migration guide.
5. Check the completed record in, then tag.

The milestone gate is separate and also applies: every issue in `v2.0.0 Cleanup` closed, every PR
merged, before `git tag v2.0.0`.
