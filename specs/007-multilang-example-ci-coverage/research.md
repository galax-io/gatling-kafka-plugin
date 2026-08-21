# Phase 0 Research: Multi-Language Example Coverage in CI

**Feature**: `007-multilang-example-ci-coverage` | **Date**: 2026-08-19 | **Spec**: [spec.md](./spec.md)

Two findings here invalidate the approach the originating issue proposed. Both were established by
inspecting the actual artifacts rather than by reasoning from the issue text, and both are recorded
with the evidence that settles them.

---

## R1. `Gatling / testOnly` cannot run a Java simulation — the issue's cheap path does not exist

**Decision**: Java example simulations are run through `io.gatling.app.Gatling.fromArgs`, driven by a
runner object on the test classpath. They are NOT added to the `Gatling / testOnly` list.

**Evidence**:

`io.gatling.javaapi.core.Simulation` is not a subclass of `io.gatling.core.scenario.Simulation`. It
is an independent abstract class that produces `SimulationParams`:

```text
$ javap -cp . io.gatling.javaapi.core.Simulation          # gatling-core-java 3.13.5
public abstract class io.gatling.javaapi.core.Simulation {
  public io.gatling.javaapi.core.Simulation();
  public io.gatling.core.scenario.SimulationParams params(GatlingConfiguration, String);
  ...
}
```

sbt discovers Gatling simulations through the `gatling-test-framework` fingerprint. That framework
declares exactly one fingerprint, and it matches only the Scala superclass:

```text
$ javap -p -c io/gatling/sbt/GatlingFramework.class       # gatling-test-framework 3.13.5
  11: iconst_1                                   // an array of ONE fingerprint
  17: new    #50   // class io/gatling/sbt/GatlingFingerprint

$ javap -p -c io/gatling/sbt/GatlingFingerprint.class
  10: ldc    #31   // class io/gatling/core/scenario/Simulation
  12: invokevirtual Class.getName
  15: putfield      superclassName
```

A Java simulation therefore matches no fingerprint. `Gatling / testOnly <java FQCN>` selects nothing
and reports no tests — it does not fail, which is the worst possible outcome for a coverage feature:
it would look like it worked.

**What does work**: `gatling-app` models the distinction explicitly and supports both.

```text
$ unzip -l gatling-app-3.13.5.jar | grep SimulationClass
  io/gatling/app/SimulationClass$Java.class
  io/gatling/app/SimulationClass$Scala.class
  io/gatling/app/SimulationClass$JavaScript.class
```

and `Gatling$` exposes a non-exiting entry point returning a status code:

```text
$ javap -p io/gatling/app/Gatling$.class
  public void main(java.lang.String[]);
  public int  fromArgs(java.lang.String[]);      // <- no System.exit, returns status
```

`fromArgs` is the right entry point: `main` terminates the JVM, which would take sbt down with it
when not forked, and would stop a runner from executing more than one simulation per invocation.

**Rationale**: This is the only route that runs the published Java sources as simulations rather than
re-implementing them. `gatling-app` is already on the test classpath — `gatling-test-framework` and
`gatling-charts-highcharts` are `it,test` dependencies, and `GatlingRunner.scala` in the examples
package already imports `io.gatling.app.Gatling`. No new dependency is introduced.

**Alternatives considered**:

- *Add the Java FQCNs to `Gatling / testOnly`* (the issue's proposal). Rejected: silently selects
  nothing, per the fingerprint evidence above.
- *Wrap each Java simulation in a Scala `Simulation` subclass.* Rejected: the two `Simulation` types
  are unrelated, so a wrapper would have to re-declare the scenario and protocol — that tests the
  wrapper, not the example, and reproduces the exact flaw `KafkaJavaapiMethodsGatlingTest` already
  has (Scala code calling the Java facade, standing in for a Java author).
- *Register a second fingerprint via a custom test framework.* Rejected: a whole framework
  implementation to reach an entry point `gatling-app` already offers.

---

**Superseded 2026-08-20 — the conclusion was right, the remedy was wrong.** Everything above about
the fingerprint holds and was re-verified. What did not hold is the step taken from it: a bespoke
`exampleRun` task driving `Gatling.fromArgs`. Every build system already has exactly one command for
running simulations, and inventing a fourth is a mechanism no consumer of this plugin can copy.

The full key inventory of gatling-sbt was enumerated from bytecode — 15 keys, and not one runs a
simulation by class name; execution is delegated wholly to `Defaults.testTasks`. So the sbt answer is
`Gatling / test`, which covers every Scala simulation and nothing else.

For Java and Kotlin the honest conclusion is that **sbt is not where they can run**, and that is a
product boundary rather than a defect here: Gatling's sbt plugin supports Scala only and directs Java
and Kotlin users to Maven or Gradle. They now live in `examples/maven`, a consumer project depending
on the published artifact, run by `mvn verify` through Maven's own Gatling plugin.

`Test/runMain io.gatling.app.Gatling` was measured and rejected: `Gatling$.main` calls
`sys.exit(fromArgs(args))`, so two chained invocations exited 0 having run only the first — the second,
engineered to fail, never executed. That is this feature's own defect reintroduced in a worse form.

Gradle was built as well. `io.gatling.gradle` 3.13.5.4 — the release matching Gatling 3.13.5 — cannot
even configure on Gradle 9.4.1: `Could not get unknown property 'reportsDir'`, the Convention API
removed in Gradle 9.0, plus `Project#javaexec`. Only 3.15.1.2 pinned back via
`gatlingVersion = "3.13.5"` works, and that is a pairing Gatling never tests. The Maven plugin pins no
Gatling version at all — it scans the test classpath and forks — so there is nothing to drift.

---

## R2. No published example is run by CI in any language, Scala included

**Decision**: The runner covers the published examples in **both** JVM languages that are in the
build — Scala and Java — not Java alone.

**Evidence**: The three simulations CI runs are test harnesses that happen to share the `examples`
package. They are not the examples the README links:

| CI runs (`Gatling / testOnly`) | What it is |
|---|---|
| `KafkaGatlingTest` | 24 KB test simulation, this repo's main broker-level test |
| `KafkaJavaapiMethodsGatlingTest` | Scala test exercising the Java facade |
| `KafkaConcurrencyLoadTest` | Sustained-load reply-loss test (#167) |

| Published as documentation (README "Examples") | Run by CI |
|---|---|
| `examples.Avro4sSimulation` (Scala) | no |
| `examples.AvroClassWithRequestReplySimulation` (Scala) | no |
| `examples.BasicSimulation` (Scala) | no |
| `examples.MatchSimulation` (Scala) | no |
| `examples.ProducerSimulation` (Scala) | no |
| all four `javaapi.examples.*` (Java) | no |
| all four `javaapi.examples.*` (Kotlin) | no |

**Rationale**: The spec's Context table credited Scala with runtime coverage of its examples. That
was wrong, and the spec has been corrected. FR-001 — "every example simulation the project publishes
as documentation for a supported language MUST be covered at the strongest level that language's
presence in the build supports" — already requires the Scala examples to run; only the Context table
suggested otherwise. Covering them is also nearly free once R1's runner exists, because the runner is
language-agnostic: `Gatling.fromArgs` resolves `SimulationClass.Scala` and `SimulationClass.Java`
through the same code path.

Leaving Scala out would produce the absurd end state where this feature makes Java examples better
covered than Scala ones.

**Alternatives considered**:

- *Java only, per the issue's literal wording.* Rejected: contradicts FR-001, and the marginal cost
  of the Scala examples is five more entries in a list.

---

## R3. Six of the nine JVM examples do not run as written

**Decision**: Each defect is corrected in the example itself under FR-002a, keeping the DSL calls and
their order intact per FR-002b.

**Evidence and the correction for each**:

| Example | Defect | Correction |
|---|---|---|
| `javaapi.examples.ProducerSimulation` (Java) | No `setUp(...)` at all — declares `scn` and stops. Nothing to execute. | Add `setUp(scn.injectOpen(atOnceUsers(1)))` with the protocol it already declares. |
| `javaapi.examples.AvroClassWithRequestReplySimulation` (Java) | Registry client built from the literal `"schRegUrl"`; payload `MyAvroClass` is an empty `private static class`, not an Avro record. Cannot serialize. | Point the client at `http://localhost:9094`; replace the payload with a real Avro type. |
| `examples.AvroClassWithRequestReplySimulation` (Scala) | Same two defects: `"schRegUrl".split(',')` and `case class MyAvroClass()` with no fields. | Same correction. |
| `examples.BasicSimulation` (Scala) | Second scenario sends to `myTopic2`, replies from `test.t1` — nothing echoes between them, so the reply never arrives — and then checks `jsonPath("$.M").is("DKF")` against a body of `{"m":"dkf"}`. Fails twice over. | Make the second exchange echo like the first, and check the field the payload actually has. |
| `examples.BasicSimulation` (Scala) | `atOnceUsers(50)` on a shared topic. | Reduce to the smallest volume its assertions need (FR-006b). |
| `javaapi.examples.MatchSimulation` + `examples.MatchSimulation` | `matchByMessage` returns the constant `"Custom Message"` for every message, so any reply matches any request. Sound only at one user in flight. | Keep the profile at one user and assert to that bound — do not raise it. |

`javaapi.examples.BasicSimulation`, `examples.MatchSimulation`, `examples.ProducerSimulation` and
`examples.Avro4sSimulation` need no structural correction, only topics and assertions.

**Superseded 2026-08-19 by T014, which ran them.** The table above was derived by reading the
sources. Running them against a real broker found it optimistic: **eight of the nine are broken**, not
six, and one that reading suggested was fine is not.

| Example | requests | OK | KO | Runner verdict without assertions |
|---|---|---|---|---|
| `examples.Avro4sSimulation` (Scala) | 2 | 2 | 0 | pass — correctly |
| `examples.AvroClassWithRequestReplySimulation` (Scala) | 0 | 0 | 0 | **pass** — 1 error, nothing sent |
| `examples.BasicSimulation` (Scala) | 108 | 54 | 54 | **pass** — `ReqRep2` is 0/54 |
| `examples.MatchSimulation` (Scala) | 1 | 0 | 1 | **pass** |
| `examples.ProducerSimulation` (Scala) | 10 | 0 | 10 | **pass** |
| `javaapi.examples.AvroClassWithRequestReplySimulation` (Java) | 0 | 0 | 0 | **pass** — 1 error, nothing sent |
| `javaapi.examples.BasicSimulation` (Java) | 5 | 0 | 5 | **pass** |
| `javaapi.examples.MatchSimulation` (Java) | 1 | 0 | 1 | **pass** |
| `javaapi.examples.ProducerSimulation` (Java) | — | — | — | fail — threw before running |

Only `Avro4sSimulation` works. Every other published example either fails every request, sends
nothing at all, or cannot start.

Two failure modes reading did not predict:

- **Both Avro examples record zero requests.** `Unsupported Avro type 'MyAvroClass'. Supported types
  are null, Boolean, Integer, Long, Float, Double, String, byte[] and IndexedRecord` is raised before
  a request is logged. This is SC-005a's "a run that sends nothing" case occurring for real.
- **`test.t` request-reply times out at 5 seconds**: `Timed out waiting for consumer assignment to
  topic 'test.t' after 5 seconds`. The Java examples and `examples.MatchSimulation` all set a
  5-second timeout, which is too tight for a cold consumer group. An example that times out the first
  time a user runs it is broken documentation, so the timeout is part of the correction.

**The decisive finding is the last column.** Without assertions the runner called 8 of the 9 a pass —
including four that failed 100% of their requests and two that sent nothing at all. Gatling returns
status 0 when a simulation declares no assertions, whatever happened during the run. Clarification
Q3's answer is therefore not a refinement of the coverage; without it there is no coverage. A runner
with no assertions would have shipped as green and proved exactly as much as the `Gatling / testOnly`
line it replaced.

**Rationale**: Every one of these is a defect a user hits by copying the example. Correcting them is
the point, not a side effect — a published example that cannot run is broken documentation whether or
not CI ran it (FR-002a, confirmed in clarification Q1).

**Alternatives considered**:

- *Mark the broken ones compile-only.* Rejected by clarification Q1.
- *Delete the broken examples.* Rejected: they document real capabilities (produce-only, Avro
  request-reply) that would then have no example at all.

---

## R4. Topic isolation between covered examples

**Decision**: Every covered example gets its own topics, named `ex.<lang>.<example>.<role>`. No two
covered examples share a topic.

**Evidence**: As written, `examples.BasicSimulation`, `examples.MatchSimulation`,
`javaapi.examples.BasicSimulation` and `javaapi.examples.MatchSimulation` all use `test.t` for both
request and reply, and both `ProducerSimulation`s use `test.topic`. The runner executes simulations
sequentially in one job against one broker; a reply left on a shared topic by one example is a live
candidate for the next example's consumer, and `MatchSimulation`'s constant matcher accepts anything.
That is a cross-attribution defect manufactured by the test setup, and it would surface as an
intermittent CI failure attributed to the plugin.

`test.t` is also not in CI's `KAFKA_CREATE_TOPICS` at all — it exists only in
`docker-compose.kafka.yml`. Neither is `test.topic`, `request.t`, or `reply.t`. So topics must be
added regardless; naming them per-example costs nothing extra.

**Rationale**: Isolation is what makes a red run attributable to one example. Renaming a topic does
not change what an example teaches (FR-002b) — the topic name is illustrative in every one of them.

**Alternatives considered**:

- *Keep the existing names and rely on distinct `group.id` plus `auto.offset.reset`.* Rejected:
  distinct groups do not stop a stale record being delivered, and `MatchSimulation` would accept it.
- *Delete topics between simulations.* Rejected: adds broker administration to a coverage feature.

---

## R5. Fork and JVM options for the runner

**Decision**: The runner executes in a forked JVM carrying the same `--add-opens` flags the Gatling
configuration already sets, via a dedicated sbt configuration rather than by flipping `Test / fork`.

**Evidence**: `build.sbt` sets

```scala
Gatling / javaOptions := overrideDefaultJavaOptions(
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
)
```

These apply to forked `Gatling` test runs. A runner invoked as `Test / runMain` inherits sbt's own
JVM, which does not carry them, and Gatling's reflective access would fail on Java 17.

`Test / fork := true` would fix the flags but changes execution semantics for the whole test suite —
including the Testcontainers integration specs and the `Global / concurrentRestrictions` limit that
`build.sbt` sets deliberately to keep the broker count survivable. That is a change to something this
feature has no business touching (FR-014's spirit).

**Rationale**: A separate configuration isolates the change to the new runner.

**Alternatives considered**:

- *`Test / fork := true`.* Rejected: blast radius across the existing suite.
- *`Gatling.main` instead of `fromArgs`.* Rejected: `main` exits the JVM, so only one simulation
  could run per invocation and the runner could not aggregate results.

---

## R6. How Kotlin is compiled without entering the build

**Decision**: A self-contained shell script compiles the Kotlin examples with a pinned `kotlinc`
against a classpath sbt exports. Kotlin does not become an sbt-managed source set.

**Evidence**: There is no Kotlin anything in `build.sbt`, `project/plugins.sbt`, or
`project/Dependencies.scala`. `scripts/check-kotlin-examples.sh`, which the issue names, does not
exist on this branch — it belongs to the unmerged `006-v2-cleanup-sweep`. Clarification Q2 settled
that this feature owns the check outright (FR-003a).

Shape:

1. An sbt task writes `Test / fullClasspath` to a file. This satisfies FR-003b — the examples compile
   against the plugin classes this build just produced, not a published release.
2. The script discovers `src/test/kotlin/**/*.kt` by glob, so a new or renamed example is picked up
   without editing a list (US3 acceptance 3).
3. `kotlinc -classpath <exported> -d <temp>` compiles them; a non-zero exit fails CI.

**Rationale**: Keeps the deliberate decision that Kotlin stays out of the build, while making the
examples impossible to break silently.

**Open item for approval**: CI must obtain a Kotlin compiler. The recommendation is to download a
pinned JetBrains release archive verified by checksum, rather than adding a third-party setup action
— an auditable, version-pinned artifact instead of a marketplace dependency. This is a CI toolchain
addition and is flagged in the plan's Constitution Check under Constraints.

**Alternatives considered**:

- *`sbt-kotlin-plugin`.* Rejected: a new build plugin dependency, and it reverses the decision that
  Kotlin stays out of the build.
- *Wait for `006-v2-cleanup-sweep` to land its script.* Rejected by clarification Q2.

---

## R7. Strengthening the compatibility gate to actually construct

**Decision**: `ExampleSmokeValidation` invokes the no-argument constructor of each example instead of
merely looking it up, and derives its list from the example source tree rather than carrying one.

**Evidence**: The gate today does:

```scala
val clazz = Class.forName(className)
require(scalaSimulationClass.isAssignableFrom(clazz) || javaSimulationClass.isAssignableFrom(clazz), …)
clazz.getDeclaredConstructor()          // looked up, never invoked
```

`getDeclaredConstructor()` returns a `Constructor` object. The field initialisers that build the
scenario and protocol never run, so the gate cannot observe a DSL break. Its nine-entry list is also
hand-maintained, with nothing detecting an example missing from it (FR-005).

**Amended 2026-08-19 after T004 measured it.** `newInstance()` alone does *not* work. Gatling
actively forbids direct instantiation:

```text
java.lang.IllegalStateException: Simulations can't be instantiated directly but only by Gatling.
```

Two of the nine examples failed this way — both `BasicSimulation`s — and the discriminator is
`jsonPath(...)`. A check reaches `io.gatling.core.Predef.configuration()`, which throws when the
static `_configuration` has not been installed. Examples with no check happen to construct without
ever touching it, which is why 7 of 9 passed and why the naive approach would have looked like it
worked.

Gatling publishes the intended way out: `GatlingConfiguration.loadForTest()` exists precisely so DSL
objects can be built outside a run. The field it must be installed into, `Predef._configuration`, is
`private[gatling]`, so the gate reaches it through a four-line shim compiled into the `io.gatling.core`
package under `src/test/scala/io/gatling/core/GatlingTestConfiguration.scala`. With that installed,
**all nine construct in under a second**, offline:

```text
constructed 9/9
```

A typed shim is preferred over reflection deliberately: if a Gatling upgrade renames the field, the
build fails at compile time rather than the gate failing at run time — and a gate that breaks quietly
is the defect this feature exists to remove.

Construction itself is offline-safe: building a `KafkaProtocol` stores settings maps and
`CachedSchemaRegistryClient` resolves lazily, so neither opens a connection. Any example that turns
out to need a live service during construction falls to FR-004.

**What this does not prove**: constructing under a test configuration is not the same as Gatling
constructing during a real run. The gate proves the DSL still builds; the runner (R1) proves the run
still works. Neither substitutes for the other.

**Rationale**: This is what Principle I of the constitution already claims the gate does. Making the
claim true is cheaper than weakening the principle.

**Alternatives considered**:

- *Correct the documentation only, leaving the gate as-is.* Rejected in clarification during
  `/speckit-specify`: it leaves the gap open.
- *Have the gate run each simulation.* Rejected: that is the runner's job (R1), and the gate must
  stay usable with no broker (FR-011).

---

## R8. Documentation and constitution corrections

**Decision**: `README.md`, `AGENTS.md`, and `.specify/memory/constitution.md` are all corrected, and
the constitution amendment is versioned **1.0.1 (PATCH)**.

**Evidence of what is untrue today**:

| Location | Statement | Reality |
|---|---|---|
| `AGENTS.md` Test Model | "`ExampleSmokeValidation` checks every README/example simulation still constructs" | It never constructs one (R7). |
| `AGENTS.md` Test Model | "`KafkaGatlingTest` and `KafkaJavaapiMethodsGatlingTest` are the Gatling simulations CI runs" | CI also runs `KafkaConcurrencyLoadTest`. |
| `README.md` Examples | "Validate that all example simulations still construct against the current API" | Same defect as the `AGENTS.md` claim. |
| Constitution, Principle I | "`ExampleSmokeValidation` MUST keep constructing every README and example simulation" | Becomes true once R7 lands; no wording change needed. |
| Constitution, Development Workflow | "Full CI gate … runs `KafkaGatlingTest` and `KafkaJavaapiMethodsGatlingTest`" | Stale; omits `KafkaConcurrencyLoadTest` and, after this feature, the example runs. |

**Rationale**: PATCH is correct because no obligation changes. Principle I's requirement is unchanged
— the implementation is what moves to meet it. The Development Workflow correction is factual. Per
the constitution's own amendment procedure, the PR must also update the Sync Impact Report and every
dependent artifact it touches.

**Alternatives considered**:

- *MINOR (1.1.0), adding a per-language coverage obligation to the constitution.* Rejected: this
  feature's spec already carries that requirement, and writing a coverage policy into the
  constitution is a governance decision that deserves its own proposal rather than riding along.
