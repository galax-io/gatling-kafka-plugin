# Implementation Plan: v2.0.0 Cleanup — Validated Removal Sweep

**Branch**: `006-v2-cleanup-sweep` | **Date**: 2026-08-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/006-v2-cleanup-sweep/spec.md`

## Summary

Remove the published surface that cannot work, the dependency chain it holds alive, the unreachable
internals behind it, and the two constructs the 1.x binary freeze forced — then wire a compiler guard
so the residue cannot silently return, sweep the tests that cannot fail, and fix the one Kotlin example
that no longer parses.

The approach is decided by two facts established before planning. First, every removal carries a
verdict backed by evidence in the current sources, and three of those verdicts overrule the milestone's
issues (A10 supersedes their estimates with measurements, B1 adds dead surface no issue names, C1
refuses a deletion whose stated reason does not hold). Second, clarification chose to release
independently of the eleven open 1.x milestones, which removes the binary-compatibility guard this kind
of release would normally lean on — so FR-027 substitutes a mechanically-produced, hand-reviewed,
checked-in record of the break surface, diffed against `1.3.0` on Maven Central.

Five stories, one semantic commit each, in the order US1 → US2 → US3 → US5 → US4 (rationale in
[research.md](./research.md) R7).

## Technical Context

**Language/Version**: Scala 2.13.18, Java 17+ (Temurin in CI); Kotlin examples are source-only and stay
uncompiled by decision

**Primary Dependencies**: Gatling 3.13.5 (`provided`), Apache Kafka clients 3.9.2, Avro 1.12.1; Avro4s
4.1.2 and Confluent Schema Registry 7.9.9 stay `provided` and optional. **No dependency is added or
upgraded by this feature**; three are removed (`kafka-streams-scala` from the inherited set, and two
`dependencyOverrides` pins)

**Storage**: N/A

**Testing**: ScalaTest + MUnit unit specs, Testcontainers integration specs, two Gatling simulations in
CI against the Compose stack, `ExampleSmokeValidation` for example construction, `checkPublishedPom` as
a build-time contract gate

**Target Platform**: JVM library published to Maven Central

**Project Type**: Single-module sbt library (Gatling protocol plugin) with a Java/Kotlin-facing facade

**Performance Goals**: Unchanged. This feature adds no runtime work; `avroSerde` moves from one shared
instance to one per summon (R3), which affects Avro simulations only and matches what the Java facade
has always done

**Constraints**: Sonatype releases are permanent — a removal shipped in error cannot be withdrawn.
Every commit must be green on its own under `sbt scalafmtCheckAll scalafmtSbtCheck compile test`. The
`-Wunused` guard must be satisfiable with zero warning suppressions

**Scale/Scope**: ~4,077 LOC `src/main`, ~4,292 LOC `src/test`. Expected removal ≈500 LOC main, ≈590 LOC
test, plus build machinery. 12 files carry the 22 measured unused imports. Published-symbol changes span
the Scala DSL, the `javaapi` facade, and one `case class` field

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

*Source: `.specify/memory/constitution.md` v1.0.0.*

- [x] **I. Published API Compatibility** — **PARTIAL: one deviation, recorded in Complexity Tracking.**
      This feature changes public signatures extensively and by design. Satisfied: approval is the
      milestone plus the clarification session; `!:` markers are planned on every breaking commit
      (FR-025); a README Migration Guide entry ships with the removals (FR-006); and
      `ExampleSmokeValidation` must still construct every README and example simulation (FR-026).
      **Not satisfied**: "deprecate before removing … keep compiling for at least one minor release".
      Only the Kafka Streams helpers (verdict A1) served that notice, in 1.3.0. Everything else is
      removed without one. See Complexity Tracking row 1.
- [x] **II. Real Broker Over Mocks** — Satisfied. No mock is introduced. The Testcontainers integration
      specs and both CI Gatling simulations stay as the verification for every Kafka interaction this
      feature touches, and US4 is explicitly forbidden from replacing a broker-backed test with a stub:
      FR-018 requires a *surviving test* for each removal, and FR-022 protects the race-pinning specs.
- [x] **III. Layer Separation & Single Wire Contract** — Satisfied, and advanced by the feature.
      `KafkaSender` / `KafkaMessageTracker` / `DynamicKafkaConsumer` boundaries are untouched. No
      parallel message or matcher type is introduced. Folding the single-implementation
      `RequestBuilder` trait into `KafkaRequestBuilder` (FR-016) is the principle's own rule applied —
      "abstraction is introduced when a second real caller exists" — run in reverse. Removing the
      topic-less `send(...)` family also deletes an `if (key == null)` branch that expressed control
      flow through a sentinel.
- [x] **IV. Test-First for Behavior Change** — Satisfied per class; see [research.md](./research.md) R5
      for the full classification. Two places are genuinely red-green and must be demonstrated failing
      first: the `-Wunused` guard (enable → 23 findings → fix → green) and the strengthened
      pending-request assertion (FR-019). Removals of unreachable surface take the constitution's
      explicit refactor exemption, and they qualify *provably* — what is removed either cannot execute
      or has never carried a value. Test removals in US4 are not behaviour changes and are governed by
      FR-018 instead.
- [x] **V. One Concern per Change, Always Green** — Satisfied. Spec artifacts commit first and
      separately as `docs(speckit): add 006-v2-cleanup-sweep spec/plan`. Five stories map to five
      semantic commits, each green on its own. Migration-guide prose ships with the story that causes
      the break rather than as a mixed docs commit, because the constitution requires the README entry
      in the same PR as the break (Principle I).
- [x] **Constraints** — Satisfied. No new dependency and no upgrade: the Kotlin toolchain was
      explicitly rejected in clarification Q3, and the `-Wunused` guard is a compiler flag, not an
      artifact. Avro and Schema Registry support stays `provided` and optional — FR-014/FR-015 exist to
      prove it still is. The supported Gatling version does not change, so the README compatibility
      table is untouched.

**Post-Phase-1 re-check**: unchanged. The Phase 1 artifacts introduce no new abstraction, no new
dependency and no new mock; the one deviation below is the same one identified before Phase 0, and
Phase 1 narrowed rather than widened it (the contract in `contracts/removed-api.md` bounds exactly which
symbols may disappear).

## Project Structure

### Documentation (this feature)

```text
specs/006-v2-cleanup-sweep/
├── spec.md                      # Feature specification (with Audit Verdicts + Clarifications)
├── plan.md                      # This file
├── research.md                  # Phase 0 output — R1..R7
├── data-model.md                # Phase 1 output — removal ledger and its rules
├── quickstart.md                # Phase 1 output — how to verify each story
├── contracts/
│   ├── removed-api.md           # The break-surface record contract (FR-027)
│   └── surviving-dsl-surface.md # What must still exist and behave after the sweep
└── checklists/
    └── requirements.md          # Spec quality checklist (complete)
```

### Source Code (repository root)

```text
build.sbt                                  # US1: justification map, DR-4 rule + check; A12 cleanup
                                           # US2: -Wunused guard
project/Dependencies.scala                 # US1: kafka-streams-scala, kafkaOverrides pins

src/main/scala/org/galaxio/gatling/kafka/
├── request/
│   ├── KafkaSerdesImplicits.scala         # US1: A1 implicits  | US3: avroSerde → implicit def
│   ├── ConfluentSerdes.scala              # US3: delete LazyGenericAvroSerde
│   ├── KafkaProtocolMessage.scala         # US1: responseCode  | US2: A11 scaladoc
│   └── builder/
│       ├── KafkaRequestBuilderBase.scala  # US1: topic-less send family
│       ├── KafkaAttributes.scala          # US1: producerTopic Option → plain (verdict B2)
│       ├── RequestBuilder.scala           # US3: delete (fold into KafkaRequestBuilder)
│       └── KafkaRequestBuilder.scala      # US3: absorb the trait
├── protocol/
│   ├── KafkaProtocolBuilder.scala         # US1: KPProducerSettingsStep.timeout/withDefaultTimeout
│   └── KafkaProtocol.scala                # US2: A10 unused import
├── checks/
│   ├── KafkaCheckMaterializer.scala       # US1(B1): avroBody
│   ├── KafkaMessagePreparer.scala         # US1(B1): avroPreparer + AvroErrorMapper
│   ├── AvroBodyCheckBuilder.scala         # US2: A8 private type + A10 import
│   └── KafkaCheckSupport.scala            # US2: A10 import
├── actions/
│   ├── KafkaRequestFailureMessages.scala  # US2: A6 buildFailure
│   ├── KafkaRequestAction.scala           # US2: A10 import
│   └── KafkaAction.scala                  # US1: unreachable missing-topic error path
├── client/
│   ├── KafkaMessageTrackerPool.scala      # US2: A9 completionCause | C1 idleSweep KEPT
│   ├── KafkaMessageTracker.scala          # US1: two responseCode forwards → None
│   └── DynamicKafkaConsumer.scala         # US2: A10 imports ×3 + A11 comment
├── KafkaDsl.scala                         # US3: re-point the ActionBuilder conversion
└── package.scala                          # US1: responseCode out of the trace line

src/main/java/org/galaxio/gatling/kafka/javaapi/
├── request/builder/KafkaRequestBuilderBase.java   # US1: the ~68-overload topic-less matrix
├── request/builder/RequestBuilder.java            # US3: re-point wrapper at KafkaRequestBuilder
├── request/expressions/ExpressionBuilder.java     # US2: A7 bytes(String)
├── checks/KafkaCheckType.java                     # US1: ResponseCode constant
└── checks/KafkaChecks.scala                       # US1: collapse duplicate branch | US3: avroSerde

src/test/
├── scala/.../classpath/PlainClasspathIsolationSpec.scala   # US3: re-point at the new construct
├── scala/.../KafkaLoggingSpec.scala                        # US1: trace-line assertion
├── scala/.../actions/KafkaRequestFailureMessagesSpec.scala # US2: 3 buildFailure cases
├── scala/.../integration/*.scala                           # US4: the sweep + FR-019 strengthening
├── scala/.../client/DynamicKafkaConsumerSpec.scala         # US4 + US2: A10 imports
├── scala/.../examples/*.scala                              # US4: duplicate recipes, dead code
└── kotlin/.../examples/ProducerSimulation.kt               # US5: fix in place

README.md                                  # US1: Migration Guide entry for 2.0.0
```

**Structure Decision**: Existing single-module sbt layout, unchanged. This feature adds no source
directory and no build module. The Kotlin sources stay exactly where they are (clarification Q3), and
the only build-file edits are removals plus one compiler flag.

## Complexity Tracking

> Deviations from the constitution, recorded as its Governance section requires.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| **Principle I — removal without a one-minor-release deprecation cycle.** Applies to the topic-less `send(...)` family (A3), `responseCode` + `KafkaCheckType.ResponseCode` (A4), `KPProducerSettingsStep.timeout`/`withDefaultTimeout` (A5), `LazyGenericAvroSerde` and the `RequestBuilder` trait (US3), and `KafkaCheckMaterializer.avroBody`/`KafkaMessagePreparer.avroPreparer` (B1). Only A1 served notice, in 1.3.0 | Clarification Q1 chose to release independently of the eleven open 1.x milestones, and a deprecation cycle needs one of them. For the largest group (A3, A4, B1) the cycle would also be hollow: a deprecation says "migrate off this by version X", but A3 cannot send at all, A4 has never carried a value, and B1 is unreachable from any entry point — there is nothing to migrate off, so the release would formally bless a broken API for another cycle. The compensating control is FR-027: the break surface is mechanically diffed against 1.3.0, reviewed by hand, and checked in, so no removal ships unreviewed | *Ship a 1.4.0 that deprecates everything first, then 2.0.0* — rejected by clarification Q1, and it spends a full release cycle warning users about entry points that cannot execute. *Keep the two genuinely-working items (A5, and the `RequestBuilder` return type) and remove only the broken ones* — rejected because it leaves the major release carrying a surface the audit proved unused, which is the exact residue this milestone exists to clear; the migration guide entry (FR-006) is what carries the notice instead |
| **Principle IV — no failing-first test for the bulk of the change.** Most of this feature deletes unreachable code | Removals of unreachable surface take the constitution's own refactor exemption, and they qualify provably rather than by assertion: A3 fails before a record is sent, A4 has only ever been `None`, A9's branch cannot be reached, B1 has no caller. The exemption's condition — "demonstrable as such by the existing suite passing unchanged" — is exactly FR-026 | *Write assertions that the removed symbols no longer exist* — not expressible in the same compilation unit, and a tautology if it were. *Defer the whole feature until tests exist for the removed paths* — rejected: writing tests for code that is about to be deleted is the definition of wasted coverage, and the two places that genuinely change behaviour (the guard, FR-019) **are** done red-first |

**Not a deviation, recorded to prevent a later reader assuming otherwise**: the absence of an automated
binary-compatibility guard is not a constitution violation — MiMa is an open issue (#217), not a
ratified requirement. FR-027 exists because the clarification removed the guard this release would
otherwise have benefited from, not because a principle demands one.

**Milestone assignment is not a deviation either.** Constitution V requires every PR to carry the
active milestone, and `AGENTS.md` defines active as "the lowest-numbered open milestone **matching the
current spec/plan**". Milestone #15 `v2.0.0 Cleanup` is the only milestone this spec describes, so it
*is* the active milestone for this work — #4 is merely lower-numbered, not matching. The
`milestone-guard.sh` hook implements the simpler "lowest-numbered open" rule and will therefore need
`MILESTONE_GUARD_OFF=1` for the assignment; that is a limitation of the hook, not an override of the
principle, and T002 records it.
