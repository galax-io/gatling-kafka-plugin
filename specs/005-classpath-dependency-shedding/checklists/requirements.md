# Specification Quality Checklist: Classpath and Dependency Shedding

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-08-09
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs)
- [x] Focused on user value and business needs
- [x] Written for non-technical stakeholders
- [x] All mandatory sections completed

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Success criteria are technology-agnostic (no implementation details)
- [x] All acceptance scenarios are defined
- [x] Edge cases are identified
- [x] Scope is clearly bounded
- [x] Dependencies and assumptions identified

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria
- [x] User scenarios cover primary flows
- [x] Feature meets measurable outcomes defined in Success Criteria
- [x] No implementation details leak into specification

## Notes

**Status: all items pass.** Validated over two iterations.

### Iteration 1

Three `[NEEDS CLARIFICATION]` markers were raised — at, not over, the allowed budget of three — each
one a scope decision with no defensible default:

- **FR-018** — Apache coordinates vs. non-inherited scope for the mandatory Kafka client dependency.
- **FR-019** — the milestone calls for shedding the Kafka Streams artifact in this release, while
  Constitution Principle I requires the two published implicits holding it to keep compiling for at
  least one more minor, and they cannot compile without it. The milestone description and the ratified
  constitution pointed in different directions.
- **FR-020** — whether Avro consumers may be asked for a one-line additional import.

All three were put to the maintainer at specification time rather than guessed.

### Iteration 2

All three resolved and recorded in the spec's Clarifications section (session 2026-08-09), with the
rejected alternatives and the reason each was rejected. Downstream sections were reconciled with the
answers rather than left contradicting them:

- Story 4's title, independent test, and scenarios 1 and 5 now accommodate the one dependency
  deliberately retained past its usefulness (Kafka Streams, held only by the deprecation window).
- SC-007 states that exception explicitly instead of asserting an absolute zero it would fail.
- Out of Scope now names artifact removal and entry-point removal as deferred, so the boundary between
  this release and the next major is unambiguous.
- Assumptions record the equivalence claim FR-018 rests on, flagged as a finding to raise if planning
  disproves it rather than something to work around silently.

### Interpretation notes on items marked complete

- **Implementation details.** This feature's subject matter *is* packaging: artifact coordinates,
  repositories, and what a consumer's build inherits are the user-observable facts, not implementation
  choices. Coordinates and source paths appear only in the Problem section as verified evidence and in
  Assumptions as verification records. The Functional Requirements state outcomes ("resolvable from the
  default public repository", "a scope consumers do not inherit") and leave mechanism to planning.

- **Non-technical stakeholders.** The stakeholder for this feature is the plugin's consumer — a
  performance engineer running a build. Stories are written from that consumer's position ("can I
  install it and get a first send working?"), which is the appropriate altitude here.

- **Technology-agnostic success criteria.** SC-003 names sbt, Gradle, and Maven. These are the three
  installation paths the project already documents and supports, so they are scope boundaries rather
  than implementation choices.

### Verification performed while writing the spec

Claims in the Problem section were checked against live artifact repositories on 2026-08-09 rather than
inferred from the build definition. The published `1.2.0` POM was fetched and read; each declared
coordinate was probed on Maven Central and on the Confluent repository. This extended issue #185's
finding: the two `org.apache.kafka` `-ce` coordinates are affected identically to the two `io.confluent`
ones, so four inherited dependencies are unresolvable, not two. Issue #185 should be updated, or a
companion issue opened, so the milestone's tracked scope matches what the fix must cover.

**Correction applied after the first draft.** The maintainer pointed out that the declared Kafka version
had moved on. The spec had been written against the branch point (`7.9.5-ce` / `7.9.8`); `main` was
already at `7.9.9-ce` / `7.9.9`. The branch was fast-forwarded to `main` and every artifact updated to
distinguish two version sets that had been conflated: what published `1.2.0` declares, and what the
next release would declare. All seven vendor versions checked — `7.9.2-ccs`, `7.9.5-ce`, `7.9.5-ccs`,
`7.9.8-ce`, `7.9.8-ccs`, `7.9.9-ce`, `7.9.9-ccs` — are absent from Maven Central, so no requirement or
decision changed.

The correction did surface a finding worth keeping: dependency automation had advanced all four broken
coordinates during specification, and they stayed equally unresolvable. Nothing asserts the property,
so each bump lands green. The same automation maintains a dependency declaration the build never
applies. Both now appear in the spec as the argument for attaching a standing assertion to FR-001
rather than only correcting the version strings, and every rule in the spec, data model, and contracts
is stated over dependency *properties* so that the next bump cannot invalidate them.
