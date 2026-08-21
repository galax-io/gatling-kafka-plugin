# Contract: Example Coverage

**Feature**: `007-multilang-example-ci-coverage` | **Date**: 2026-08-19

The interface this feature exposes is not an API — it is a set of guarantees CI makes about the
published examples. Each contract below is written so that it can fail, and names what asserts it.
A contract nothing can break is not a contract; the whole reason this feature exists is that the
project had exactly such a statement in three documents.

---

## C1 — Every published example runs against a real broker

**Guarantee**: On every CI run, all thirteen published example simulations execute against the CI
broker — 5 Scala, 4 Java, 4 Kotlin.

**Asserted by**: each project's own Gatling task, in the project whose build tool its users use:
`sbt "Gatling / test"` in `examples/scala`, `mvn verify` in `examples/java`,
`./gradlew gatlingRun --all` in `examples/kotlin`. Nothing bespoke stands between an example and the
runner a user of this plugin would invoke.

**Fails when**: a simulation throws, or Gatling reports a failed run.

**Note**: sbt cannot express this contract for Java or Kotlin at all. See [R1](../research.md) — the
sbt fingerprint matches only `io.gatling.core.scenario.Simulation`, so a Java FQCN passed to
`testOnly` selects nothing and reports success. Folding those examples back into the plugin's sbt
build silently voids this contract for eight of the thirteen.

---

## C2 — Every published Kotlin example compiles against the published artifact

**Guarantee**: On every CI run, all four Kotlin example simulations compile against the plugin as a
consumer resolves it.

**Asserted by**: `compileGatlingKotlin` in `examples/kotlin`, a prerequisite of `gatlingRun`.
Compiling against the published artifact is a stronger statement than compiling against the plugin's
internal test classpath: it is the contract a consumer actually gets.

**Fails when**: any Kotlin example fails to compile.

**No tool to go missing**: the Kotlin compiler arrives through the Gradle Kotlin plugin and the
committed wrapper, so there is no path where the check silently skips for want of a toolchain.

---

## C3 — No example is silently uncovered

**Guarantee**: Every source file under the three example directories that declares a Gatling
simulation has exactly one coverage level. An example present in the tree but absent from the
coverage inventory fails CI.

**Asserted by**: `ExampleCoverageCheck`, which derives its set from the example source tree per
[DR-1](../data-model.md) and compares it against the covered set. It needs no broker.

**Fails when**: an example is added, renamed, or moved without being covered; or a non-example is
added to an example project without being added to the exclusion list.

**Rationale**: The pre-feature gate carried a hand-written list of nine FQCNs with nothing detecting
an omission. That is how the Kotlin examples came to have no coverage of any kind without anyone
noticing.

---

## C4 — Every example's scenario and protocol are really built

**Guarantee**: The field initialisers that build an example's scenario and protocol execute, so a DSL
break is caught rather than merely a missing class.

**Asserted by**: running the example (C1). Running constructs it — this is subsumed rather than
separately checked.

**Fails when**: an example's scenario or protocol no longer builds.

**History**: this used to be a standalone offline gate, `ExampleSmokeValidation`, which called
`getDeclaredConstructor()` without invoking it and therefore constructed nothing — it printed
"Validated" for an example whose Avro schema literal did not parse. It was corrected to construct,
and then became redundant once the examples moved into consumer projects that run them. The offline
half that survives is C3 and C6, in `ExampleCoverageCheck`.

**Known cost**: construction is no longer checked without a broker. If the broker is unavailable,
C1 and C4 both go unverified in that run.

---

## C5 — A covered example that does nothing fails

**Guarantee**: Each covered example carries assertions over an expected request count and a 100%
success rate. Absence of an error is not a pass.

**Asserted by**: Gatling's own assertion mechanism, which sets a non-zero run status when an
assertion fails; the runner propagates it.

**Fails when**: an example sends fewer requests than it claims, or any request fails or times out
without a matched reply.

**Bounded by**: [DR-3](../data-model.md) — assertions are written to what the injection profile and
matching strategy guarantee, never above. `MatchSimulation`'s constant matcher is sound at one user
in flight, so its profile stays at one user and its assertion says one request.

---

## C6 — Covered examples do not share topics

**Guarantee**: The topic sets of any two covered examples are disjoint. Every topic any covered
example uses exists in both the CI broker definition and the local Compose definition.

**Asserted by**: `ExampleCoverageCheck`, via `ExampleInventory.topicProblems`. It reads the topics out
of the example sources and reports a duplicate as a configuration error rather than letting it
surface as an intermittent reply mismatch later. It runs before any simulation does, and needs no
broker.

**Fails when**: two examples are given the same topic; an example uses a topic that either broker
definition omits; or an example names a topic in a way the reader cannot resolve — a non-literal
argument is reported rather than silently contributing nothing.

**Rationale**: The simulations run sequentially against one broker, and the three projects run one
after another in CI. A record left on a shared topic by one example is a live
candidate for the next example's consumer, and `MatchSimulation` accepts anything. That failure would be attributed to the plugin's correlation
logic, which is precisely the wrong diagnosis.

---

## C7 — No statement about coverage overstates it

**Guarantee**: Every statement in `README.md`, `AGENTS.md`, and `.specify/memory/constitution.md`
describing what the compatibility gate verifies, or which simulations CI runs, is true of what
exists.

**Asserted by**: review, at acceptance. This is the one contract with no automated assertion, which
is itself the reason it was violated in three documents for four releases.

**Known statements corrected by this feature**: see [R8](../research.md) — the `AGENTS.md` Test Model
claim about the gate and its two-simulation CI list, the `README.md` Examples claim, and the
constitution's Development Workflow paragraph. Principle I was amended twice: to say what
"constructing" required, and then to require every example to be compiled and run from a consumer
project once construction became something running does.

**Compensating control**: because C7 cannot fail automatically, the documentation slice (S5) lands
last, after the things it describes exist. Correcting a claim before its subject exists would make
the documentation wrong in the other direction.

---

## What this feature deliberately does not guarantee

- **No wall-clock budget.** Clarification Q5 removed it. Cost is bounded by DR-3 keeping profiles at
  the smallest volume the assertions need.
- **Kotlin examples are not run by sbt** — nothing in sbt can run them. They are compiled and run by
  the Gradle consumer project instead.
- **No standing mutation guard.** The deliberate-break drill (FR-007a) is a one-off acceptance
  artifact, not a check on every run. Coverage can therefore still decay silently between now and the
  next time someone looks — accepted knowingly in clarification Q4.
