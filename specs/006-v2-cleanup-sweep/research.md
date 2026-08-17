# Phase 0 Research: v2.0.0 Cleanup — Validated Removal Sweep

**Feature**: `006-v2-cleanup-sweep` | **Date**: 2026-08-09

The specification's *Audit Verdicts* section already resolved the "is it dead?" questions against the
current sources. This document resolves the remaining **how** questions — the ones that decide whether
the plan is executable, and in what order.

---

## R1. How the break surface is recorded without a binary-compatibility guard

**Context**: Clarification Q1 chose to land independently of the eleven open 1.x milestones, so the
MiMa wiring tracked in #217 is not available. FR-027 requires a hand-authored, checked-in record of
every published symbol removed or changed, reviewed before the release tag.

**Decision**: Produce the record by mechanically diffing the **public** signatures of the previous
release jar against the built jar, then reviewing the diff by hand and checking the reviewed result in
as `specs/006-v2-cleanup-sweep/contracts/removed-api.md`.

- **Baseline**: `org.galaxio:gatling-kafka-plugin_2.13:1.3.0`. Verified during research — the jar is
  present on Maven Central (HTTP 200), so the baseline needs no local build and no credentials.
- **Mechanism**: `javap -public` over every class in each jar, normalised and diffed. No build plugin,
  no new dependency, no change to `project/plugins.sbt`.
- **Review**: the diff is input, not output. Every entry is matched to a verdict (A1–A12, B1) in
  spec.md. An entry with no verdict is an unintended break and blocks the release.

**Rationale**: The verdict tables enumerate the removals we *intend*. What they cannot enumerate are
the **cascades** — folding the `RequestBuilder` trait into its implementation changes the declared
return type of every documented `send(...)`, and removing a `case class` field changes `apply`,
`unapply` and `copy`. A signature diff catches those; a hand-written list does not. Reviewing a
machine-produced diff is the "by hand" verification the clarification asked for, done in the direction
that actually finds surprises.

**Boundary with #217**: This is a one-shot release artifact, not a build gate. It deliberately does
**not** add MiMa to the build, does not run on every compile, and does not preempt the design of the
permanent guard. When #217 lands, the checked-in record becomes its first baseline rather than dead
weight.

**Alternatives considered**:

- *Add MiMa temporarily, then remove it* — would give an authoritative report, but it edits
  `project/plugins.sbt` twice for one release, and an add-then-remove within a change is exactly what
  the project's commit rules forbid.
- *Hand-list the removals from the verdict tables* — cheapest, but blind to cascades, which are the
  only class of break the verdict tables cannot already state.
- *Defer the release until #217 lands* — rejected by clarification Q1.

---

## R2. Which unused-code warnings the guard turns on, and whether they are satisfiable

**Decision**: `-Wunused:imports,privates,locals,patvars`, added to the existing `scalacOptions`
alongside the `-Xfatal-warnings` already in the build. Parameters and implicits are excluded
(clarification Q2, FR-010).

**Rationale — measured, not assumed.** Compiled `compile` and `Test/compile` with these exact options
and `-Xfatal-warnings` lifted:

| Warning class | Findings | Where |
|---|---|---|
| `imports` | 22 | 7 in `src/main`, 15 in `src/test` (9 of those in `examples/KafkaGatlingTest.scala`) |
| `privates` | 1 | the dead type alias in `checks/AvroBodyCheckBuilder.scala` (verdict A8) |
| `locals` | 0 | — |
| `patvars` | 0 | — |

So the guard costs exactly one cleanup commit and then holds, and every finding it produces today is
already an item in verdict A10/A8 — the guard and the sweep agree. `locals` and `patvars` cost nothing
to include and close two more classes of residue, which is why they are in rather than out.

**Why parameters and implicits stay out**: unmeasured, and this codebase is dense with exactly their
false-positive sources — `override` methods implementing Gatling's and Kafka's interfaces cannot drop a
parameter, and `KafkaCheckSupport` is a wall of implicit conversions. Including them would buy coverage
with `@nowarn` annotations, converting a self-enforcing guard into a suppression habit. FR-010 states
this and SC-003 makes it checkable by forbidding suppressions outright.

**Alternatives considered**: `-Wunused:all` / adding `params,implicits` (rejected above);
`imports` alone (rejected — it drops the guard on the one `privates` finding this audit actually made).

---

## R3. Replacing `LazyGenericAvroSerde` without breaking classpath isolation

**Context**: `KafkaSerdesImplicits.avroSerde` is currently `implicit val avroSerde: Serde[GenericRecord]
= new LazyGenericAvroSerde`. The `val` is strict because making it `lazy` would delete the mixin setter
from the compiled trait interface, and `LazyGenericAvroSerde` exists to keep that strict `val` from
constructing a Confluent type at trait-initialisation time (Contract E1 of feature 005).

**Decision**: `implicit def avroSerde: Serde[GenericRecord] = ConfluentSerdes.newAvroSerde()`, and
delete `LazyGenericAvroSerde`.

**Rationale**: A `def` body runs only when something summons a `Serde[GenericRecord]` — i.e. only in an
Avro simulation, which by definition has the artifacts. Initialising `Predef` no longer touches
Confluent, so E1 holds by construction rather than by a deferring wrapper. The pattern is already
established in the same trait: `serdeClass[T]` is an `implicit def` that builds per summon, and
`ConfluentSerdes.newAvroSerde()` already exists as a `def` for the Java facade.

**Risks identified, and how the plan handles them**:

1. *Implicit ambiguity.* The trait also declares `implicit def serdeClass[T](implicit schemaRegUrl:
   String): Serde[T]`, which could in principle also satisfy `Serde[GenericRecord]`. It requires an
   implicit `String` in scope, which ordinary simulations do not have, so it is inapplicable and
   `avroSerde` wins. **Verification**: `ExampleSmokeValidation` plus the Avro example simulations
   compile — an ambiguity would be a compile error, not a runtime surprise.
2. *Per-summon instance instead of one shared.* `GenericAvroSerde` carries mutable `configure`/`close`
   state, so handing each summon its own instance is the safer direction — and it matches what
   `newAvroSerde()`'s own documentation already states about why it is a `def`.
3. *`javaapi.checks.KafkaChecks.avroSerde`* is a `val` holding a `LazyGenericAvroSerde` and must move
   with it. As a `def` in a Scala object it keeps the same `avroSerde()` accessor Java calls, and it
   makes the `KafkaChecks$` arm of the isolation suite strictly stronger: the object initialiser then
   has nothing Avro-related in it at all.

**Test re-pointing (FR-015)**: `PlainClasspathIsolationSpec`'s `LazyGenericAvroSerde` case is rewritten
against the new entry point and must still assert **fails-on-use, not fails-on-summon** — summoning
`avroSerde` under the denying loader must succeed in producing the reference and fail only when
`serializer()`/`deserializer()` is called. The suite's positive control stays: without it the whole
suite can pass for the wrong reason.

**Alternatives considered**: keep `LazyGenericAvroSerde` (rejected — it is the freeze artefact this
release exists to remove); make `avroSerde` a `lazy val` (rejected — the mixin-setter hazard the
original comment documents is real for anyone compiled against ≤1.3.0, and a `def` avoids it entirely).

---

## R4. Collapsing the dependency chain the Kafka Streams surface holds up

**Decision**: remove in this order, in one commit — the two implicits, then `kafka-streams-scala` from
`Dependencies.kafka`, then the two streams entries from `kafkaOverrides`, then the `deprecated:`
justification entry, then rule DR-4 and its check in `checkPublishedPom`.

**Measured starting state** (from a POM generated during research): the build declares exactly four
inherited dependencies today —

```text
compile  org.scala-lang:scala-library:2.13.18
compile  org.apache.kafka:kafka-clients:3.9.2
compile  org.apache.kafka:kafka-streams-scala_2.13:3.9.2   ← removed by this feature
compile  org.apache.avro:avro:1.12.1
```

so the target state is three, all `used-by`.

**What must NOT be removed**: `"org.apache.kafka" % "kafka-clients"` stays in `kafkaOverrides`. It is
there for a different reason — Confluent's Avro artifacts drag in the vendor rebuild of `kafka-clients`,
which outranks the Apache one under highest-version-wins. Removing it would silently restore the defect
feature 005 fixed. Only the two `kafka-streams*` pins go.

**Residual effect to verify, not assume**: `kafka-streams-avro-serde` is `provided` and depends on
`kafka-streams`, so after the pins are dropped Confluent's rebuild of `kafka-streams` may appear on the
*compile and test* classpath. That is harmless — nothing in the plugin references a Kafka Streams class
once the implicits are gone, and `checkPublishedPom`'s transitive check reads the **runtime**
configuration, which excludes `provided`. The plan verifies rather than assumes it: `checkPublishedPom`
must pass, and it prints the transitively-inherited count.

**Ordering constraint**: the implicits must go **first** in the same commit. Their declared types
(`WindowedSerdes.SessionWindowedSerde`, `Consumed`) appear in signatures that implicit search reads for
every simulation, so the artifact cannot be dropped while they exist.

---

## R5. What "test-first" means for a feature that mostly deletes

**Context**: Constitution Principle IV requires a failing-first test for every behaviour change, and
explicitly exempts pure refactors demonstrable by the existing suite passing unchanged.

**Decision**: classify each story and apply the principle per class.

| Class | Stories | How Principle IV is satisfied |
|---|---|---|
| Removal of unreachable surface | US1 (most), US2 | Exempt as a no-observable-behaviour change — and *provably* so: what is removed either cannot execute (A3) or has never carried a value (A4). The existing suite passes unchanged apart from tests of the deleted symbols themselves |
| Guard introduction | US2 (FR-010) | Genuinely red-green: enable the flag, observe the build fail with the 23 findings from R2, remove them, observe green. The red state is the test |
| Behaviour-preserving restructure | US3 | Exempt, but only because `PlainClasspathIsolationSpec` re-pointed at the new construct (FR-015) proves the property is preserved. That re-pointing is written before the deletion, and must fail if pointed at a construct that eagerly builds Confluent |
| Assertion strengthening | US4 (FR-019) | Genuinely red-green, and the only place in this feature where a new test must fail first: the strengthened pending-request assertion is written and demonstrated to fail against a deliberately reverted guard before it is kept |
| Test removal | US4 (rest) | Not a behaviour change. Guarded instead by FR-018 — every removal names the surviving test — which is the mutation argument, recorded rather than assumed |
| Example correction | US5 | Not a behaviour change; verified by compiling the examples in a scratch Kotlin project (R6) |

**Rationale**: writing a "test" that asserts a symbol no longer exists is not expressible in the same
compilation unit, and would be a tautology if it were. The honest equivalent for removals is the
existing suite continuing to pass, plus the break-surface record from R1 proving nothing unintended
went with them.

---

## R6. Verifying the Kotlin examples without adding a Kotlin toolchain

**Context**: Clarification Q3 — the Kotlin examples stay in `src/test/kotlin/`, no compiler is wired
into the build, nothing is relocated. `ProducerSimulation.kt` currently does not parse (unbalanced call
chain; six undeclared types).

**Decision**: verify by compiling the four files **once, outside this repository**, in a throwaway
Kotlin project that depends on the built plugin plus Gatling, and record the result in the PR. Nothing
about that scratch project is checked in.

**Rationale**: the requirement is that a Kotlin user can copy an example and have it compile
(FR-024, SC-007). Compiling it exactly as such a user would is the direct test, and it needs no
permanent build change — which is what Q3 ruled out.

**Checked during research**: all four Kotlin examples use only `topic(...).send(...)` and
`requestReply()...send(...)`. Neither is removed by US1, so no example needs restructuring — only
`ProducerSimulation.kt` needs fixing, and its problems are self-contained (imports, one paren, and
either defining `MyAvroClass` or switching to a type the file already has).

**Accepted tradeoff, recorded in the spec**: nothing automated will catch the next drift. The
compensating measure is that the Kotlin examples are re-read whenever a published entry point changes,
which US1 forces in this release.

**Alternatives considered**: `sbt-kotlin-plugin` (rejected by Q3 — a build dependency for four files);
relocating to `docs/` (rejected by Q3); deleting them (rejected by Q3).

---

## R7. Ordering of the five stories

**Decision**: US1 → US2 → US3 → US5 → US4, one semantic commit each, each green on its own.

**Rationale**:

- **US1 first** because it is the largest deletion and every later story reads the surface it leaves.
  Doing US2's import sweep first would mean sweeping imports out of files US1 then deletes.
- **US2 second** so the `-Wunused` guard is live before US3 and US4 edit anything — from that point the
  build itself prevents either story from leaving residue behind.
- **US3 third**: it is independent of US1 and US2, but it touches `PlainClasspathIsolationSpec`, and
  running it after the guard means the re-pointed test cannot re-introduce an unused import.
- **US5 fourth**, not last, because the Kotlin examples must be re-read against the post-US1 surface and
  that is easier while US1 is fresh — and because it touches nothing US4 touches.
- **US4 last** so the test sweep runs against the final shape of the library. Sweeping first would
  require re-doing the judgement for every test whose subject US1–US3 then changed.

**Break-surface record (R1) is produced after US3**, the last story that changes a published signature,
and reviewed before the release tag.

---

## Open items carried into `tasks.md`

None blocking. Two items are deliberately deferred to execution rather than decided here, because both
are answered by running the build and neither changes the plan's shape:

1. The exact post-removal line counts, against SC-005 (~590 test LOC) and SC-008 (~500 main LOC). These
   are indicative; the binding criterion is the verdict list plus FR-018's named survivors.
2. Whether Confluent's `kafka-streams` rebuild appears on the provided/test classpath after R4's pin
   removal. Harmless either way; `checkPublishedPom` is the gate that decides whether it matters.
