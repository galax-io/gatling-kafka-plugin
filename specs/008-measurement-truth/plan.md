# Implementation Plan: Measurement Truth in Request-Reply

**Branch**: `008-measurement-truth` | **Date**: 2026-08-24 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/008-measurement-truth/spec.md`

**Milestone**: [v2.1.0 Measurement truth](https://github.com/galax-io/gatling-kafka-plugin/milestone/16) — issues [#227](https://github.com/galax-io/gatling-kafka-plugin/issues/227), [#228](https://github.com/galax-io/gatling-kafka-plugin/issues/228)

## Summary

Two figures a request-reply run reports are not true: requests the plugin rejected before sending
contribute latency samples to the request they were rejected from, and a service that answers with a
tombstone under value correlation is reported as one that never answered.

The approach follows from three properties of Gatling 3.13.5 verified against its bytecode and
recorded in [research.md](./research.md): the request name is the only field a plugin controls that
segregates response-time samples (C2); the response-code slot is discarded before a run's data is
written, so it can carry nothing (C3); and no statistics entry point records a request-counted
failure without a duration (C4). Together they rule out every alternative and leave one lever.

So: refuse a scenario that contains a request-reply whose protocol carries no consumer configuration, before the run starts rather than once per request — scoped to the request-reply builder, because absent consumer configuration is the produce-only shape `KafkaProtocolBuilder.properties(...)` documents and three published examples rely on;
report the two remaining pre-send rejections under a derived request name carrying their real
intervals; count the replies a matcher cannot correlate and name them in the timeout they cause; and
document header correlation as the required shape for tombstone-answering services, proved against a
real broker. The run-wide response-time aggregate stays blended — Gatling feeds it without reference
to request name — and the Migration Guide says so rather than implying otherwise.

## Technical Context

**Language/Version**: Scala 2.13.18 on sbt; Java 17+ (Temurin in CI); the `javaapi` facade is Java, exercised from Kotlin

**Primary Dependencies**: Gatling 3.13.5 (`provided`), Kafka clients 3.9.2 / Confluent 7.9.x, Avro4s 4.1.2 (`provided`, untouched here)

**Storage**: N/A — this feature stores nothing and changes no serialized format

**Testing**: MUnit + ScalaTest unit specs, Testcontainers integration specs, four Gatling harnesses under `Gatling / test` against the `docker-compose.kafka.yml` stack, nine example simulations run from three consumer projects

**Target Platform**: JVM library, published to Sonatype and consumed by downstream Gatling simulations

**Project Type**: Single-project JVM library with a Java facade

**Performance Goals**: No regression on the reply path. The one new per-reply operation is an integer increment on a branch that already exists and already logs; the reply path is the thread that gates reply throughput, so nothing added to it may allocate or format.

**Constraints**: No new dependency. No published Scala or Java signature change. `KafkaMatcher` and `KafkaProtocolMessage` untouched — #218 reopens the matching contract in `v2.11.0`. Target release is a minor, so nothing that forces implementers of a public contract to change.

**Scope boundary**: The build-time refusal lives in the request-reply builder only. Publishing takes a different route end to end — `KafkaRequestActionBuilder` → `KafkaRequestAction`, which uses `components.sender` and never reads the tracker pool — so produce-only simulations are untouched, and contract clause C13 is the guard that keeps it that way.

**Scale/Scope**: Two source files carry the behaviour change ([KafkaRequestReplyAction.scala](src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyAction.scala), [KafkaMessageTracker.scala](src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala)), one carries the build-time gate ([KafkaRequestReplyActionBuilder.scala](src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyActionBuilder.scala)). Two existing assertions change; two new harness scenarios and one responder capability are added. Thirteen contract clauses in [contracts/behavior-contract.md](./contracts/behavior-contract.md).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

*Source: `.specify/memory/constitution.md` v1.1.0.*

- [x] **I. Published API Compatibility**: No public Scala DSL or `javaapi` signature changes, no
      default protocol setting changes, no serialized format changes. **Observable behaviour does
      change** — rejections move to a derived request name, a missing consumer configuration fails
      earlier, and one failure message gains a clause. Approval is recorded in the spec's
      Clarifications; a Migration Guide entry is FR-012 and contract clause C5, and lands in the same
      PR as the change it describes. No entry point is replaced, so no deprecation path is owed. All
      nine published examples compile and run unchanged from their consumer projects (C7) — none
      exercises a rejection and none uses `details(...)`.
      **Minor, not major**: this is a behaviour change without a source or binary break, the same
      shape as v1.2.0's "keyless request-reply is now failed instead of mismatched", which turned
      passing scenarios red and shipped as a minor with a Migration Guide entry. Precedent followed
      deliberately, not assumed — see [research.md D7](./research.md).
- [x] **II. Real Broker Over Mocks**: Reply correlation, timeout handling and the tombstone path are
      exercised end to end in `KafkaFailureModesGatlingTest` against the Compose stack and in
      `KeylessCorrelationSpec` against Testcontainers, with a real responder. `RecordingStatsEngine`
      is used only where there is no Kafka interaction to have — asserting which name and message a
      reporting site emits.
- [x] **III. Layer Separation & Single Wire Contract**: `KafkaSender`, `KafkaMessageTracker` and
      `DynamicKafkaConsumer` keep their responsibilities — the tracker counts what only the tracker
      sees, the action names what only the action reports, the builder validates what only the
      builder can validate before a run. `KafkaProtocolMessage` and `KafkaMatcher` are untouched; no
      parallel type is introduced. The build-time gate removes a per-request branch rather than
      adding one, so no dead code is merged.
- [x] **IV. Test-First for Behavior Change**: Every clause in
      [contracts/behavior-contract.md](./contracts/behavior-contract.md) is a test that fails before
      the change. Two of them cover paths that have **no test today at all** — the missing-consumer
      rejection and how the acquisition failure is reported — so the first commit adds coverage of
      existing behaviour before changing it.
- [x] **V. One Concern per Change, Always Green**: Spec artifacts commit first as
      `docs(speckit): add 008-measurement-truth spec/plan/tasks`. Then one semantic commit per issue:
      `fix(stats): …  (#227)` and `fix(client): … (#228)`, each green under
      `sbt scalafmtCheckAll scalafmtSbtCheck compile test` on its own. Each PR carries milestone
      `v2.1.0 Measurement truth` and `Closes #NNN`. The `README` Migration Guide entry travels with
      the change it documents, because the constitution requires it in the same PR.
- [x] **Constraints**: No new dependency and no upgrade. Avro and Schema Registry stay `provided`
      and are untouched. No supported Gatling version changes, so the README compatibility table
      stands.

**Post-Phase-1 re-check**: passes unchanged. Phase 1 added no type, no dependency and no public
signature; the one requirement it could not fully meet is recorded in Complexity Tracking below.

## Project Structure

### Documentation (this feature)

```text
specs/008-measurement-truth/
├── plan.md                              # This file
├── spec.md                              # Feature specification
├── research.md                          # Phase 0 — decisions D1..D7, verified constraints C1..C6
├── data-model.md                        # Phase 1 — reported outcome shapes, tracker state
├── quickstart.md                        # Phase 1 — how to prove it works
├── contracts/
│   └── behavior-contract.md             # Phase 1 — clauses C1..C13
├── checklists/
│   └── requirements.md                  # Spec quality checklist (16/16)
└── tasks.md                             # Phase 2 — /speckit-tasks, not created here
```

### Source Code (repository root)

```text
src/main/scala/org/galaxio/gatling/kafka/
├── actions/
│   ├── KafkaRequestReplyActionBuilder.scala   # + build-time refusal of a missing consumer config (C6);
│   │                                          #   scoped here so produce-only stays untouched (C13)
│   ├── KafkaRequestReplyAction.scala          # + derived names (C1, C2); − the per-request no-pool branch
│   └── KafkaRequestFailureMessages.scala      # + the derived-name construction and its kinds
└── client/
    └── KafkaMessageTracker.scala              # + uncorrelatable-reply count (C8, C11); timeout clause (C9, C10)

src/test/scala/org/galaxio/gatling/kafka/
├── actions/
│   ├── KafkaRequestReplyActionSpec.scala      # ~ rejection now asserts the derived name (C1, C2)
│   └── (new) consumer-settings build-time spec # C6, across the shared builder
├── client/
│   └── KafkaMessageTrackerSpec.scala          # + counting and message clauses (C8..C11)
├── integration/
│   └── KeylessCorrelationSpec.scala           # unchanged — name-agnostic, still proves nothing is sent
└── examples/
    ├── KafkaFailureModesGatlingTest.scala     # ~ derived-name assertion; + value- and header-correlated
    │                                          #   tombstone scenarios (C3, C4, C9, C12)
    └── EchoResponder.scala                    # + echo a named correlation header onto the reply

README.md                                      # + Migration Guide entries for #227 and #228 (C5, C12)
```

**Structure Decision**: The existing single-project layout is kept exactly. This feature adds no
package, no module and no type — it changes what three existing collaborators report and adds one
integer of actor state. Test coverage lands in the specs and the harness that already own these
paths; no new harness is introduced, because `KafkaFailureModesGatlingTest` exists precisely to hold
by-design failures and already runs a tombstone responder against a real broker.

## Phase Outputs

| Phase | Artifact | Status |
|---|---|---|
| 0 | [research.md](./research.md) — 7 decisions, 6 verified Gatling constraints, full blast radius | Complete |
| 1 | [data-model.md](./data-model.md) — reported outcome, request name, rejection, uncorrelatable reply, config validity | Complete |
| 1 | [contracts/behavior-contract.md](./contracts/behavior-contract.md) — C1..C13 plus a compatibility ledger | Complete |
| 1 | [quickstart.md](./quickstart.md) — commands, clause-to-test map, hand-verification steps | Complete |
| 2 | [tasks.md](./tasks.md) — 31 tasks across 6 phases | Complete |

## Complexity Tracking

> One requirement cannot be met as literally written. Recorded here rather than quietly satisfied,
> per the constitution's compliance-review rule.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| FR-006 requires a derived name "constructed so that it cannot collide with a request name a simulation could itself declare". Delivered instead: a bracketed suffix that is improbable by construction and documented as reserved. | Request names are free-form strings chosen by the simulation author, so no suffix can be proven collision-free. Gatling's own `redirectNamingStrategy` carries the identical exposure and is a shipped, user-overridable default. | *Reserving a character no name may contain* would require validating every declared request name — a new failure mode imposed on every user to protect against a collision nobody has hit. *Making the suffix configurable* buys a permanent public API surface (Constitution I) to solve a hypothetical. *Abandoning the derived name* leaves the per-request figure wrong, which is the defect the milestone exists to fix. |
