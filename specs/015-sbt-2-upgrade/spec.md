# Feature Specification: Build the galax-io sbt projects on sbt 2.0.9

**Created**: 2026-09-24

**Status**: Approved (Decision DEC-9104, option `candidate-bestofbreed`)

**Scope**: gatling-picatinny, sbt-schema-registry-plugin, gatling-amqp-plugin,
gatling-jdbc-plugin and gatling-kafka-plugin. The same specification is
committed to each of them. demo-repository is left out of this delivery by the
maintainer's decision; moving it follows separately.

## Context

Every sbt build in the organization can move to sbt 2: the only plugin
obstacles are an unused sbt-avro in gatling-kafka-plugin and demo-repository's
gatling-sbt 4.18.1, and demo-repository is out of scope here. The latest
stable sbt 2 is **2.0.9** (2026-09-14). Maven Central metadata marks the
2.1.0-M2 milestone as latest and release, so the version is pinned exactly.
sbt 2 moves the build definition to Scala 3 and needs JDK 17 or newer; it does
not change the Scala version a project publishes for.

sbt-mima-plugin 1.2.x relies on sbt 2.0.7 or newer to pick its previous
artifact, which rules out the sbt 2.0.0 and 2.0.6 targets the organization
uses today.

## User Scenarios & Testing

### User Story 1 - Library users see no change (Priority: P1)

As a user of gatling-picatinny, gatling-amqp-plugin, gatling-jdbc-plugin or
gatling-kafka-plugin, I get the same Scala 2.13 artifacts, with the same
dependencies and binary interface, whichever sbt version built them.

**Acceptance Scenarios**:

1. **Given** a library built on sbt 2.0.9, **When** its artifacts are compared
   with the previous commit built on its sbt 1 launcher, **Then** the published
   artifact set, the normalized POMs, the jar entry lists and the class-file
   major versions are the same, and MiMa reports no issue between them.
2. **Given** a library with a blocking MiMa check (amqp against 1.3.0, jdbc
   against 1.5.0, kafka against 2.1.0), **When** it builds on sbt 2.0.9,
   **Then** that check still blocks, now run by the sbt 2 MiMa plugin.

### User Story 2 - sbt plugin users on sbt 1 and sbt 2 keep their plugin (Priority: P1)

As a user of sbt-schema-registry-plugin on sbt 1 or on sbt 2, I keep getting
a release for my sbt, with the same minimum sbt version as before.

**Acceptance Scenarios**:

1. **Given** the plugin built from an sbt 2.0.9 launcher with projectMatrix,
   **When** it is released, **Then** it publishes `_2.12_1.0` (Scala 2.12.21,
   the same sbt 1 API as before) and `_sbt2_3` (Scala 3.8.4), and `_2.12_1.0`
   matches what the previous sbt 1 launcher produced.
2. **Given** that release, **When** gatling-kafka-plugin moves to it on sbt
   2.0.9, **Then** its build loads the `_sbt2_3` artifact and its own artifacts
   do not change.

### User Story 3 - One gate proves every upgrade (Priority: P1)

As the maintainer, every upgrade pull request runs the same parity check,
hosted in gatling-picatinny, and cannot merge when an artifact changes.

**Acceptance Scenarios**:

1. **Given** a pull request whose base builds on sbt 1 and whose head builds
   on sbt 2.0.9, **When** the gate runs, **Then** it fails on any difference in
   the published artifacts and on any MiMa issue of the head against the base.
2. **Given** a head whose `project/build.properties` names a pre-release or an
   sbt 2 version below 2.0.7, **When** the gate runs, **Then** it fails.

### Edge Cases

- A build that cannot pass its gate stays on sbt 1.13.0, with the reason
  written into `project/build.properties`.
- sbt-schema-registry-plugin cannot keep `_2.12_1.0` unchanged under
  projectMatrix: it keeps pluginCrossBuild on a 1.13.0 launcher, retargets its
  Scala 3 axis to sbt 2.0.9, and records why.
- sbt 2 caches tasks and runs tests incrementally: CI restores no sbt 2 task
  cache or `target/` directory, so no test is skipped.

## Requirements

### Functional Requirements

- **FR-001**: Each in-scope build MUST pin exactly `sbt.version=2.0.9`, unless
  its gate fails, in which case it pins 1.13.0 with the reason next to the pin.
- **FR-002**: No upgrade MAY change a `scalaVersion`, a published library
  dependency or a MiMa baseline, except sbt-schema-registry-plugin's move from
  1.8.0 to 1.9.0.
- **FR-003**: gatling-picatinny MUST host a reusable parity gate
  (`.github/workflows/sbt-upgrade-parity.yml`) that builds a pull request's
  base with its own launcher and its head with the pinned one, compares the
  published artifact set, normalized POMs (dependencies with scopes and
  versions, the Scala library, licenses, scm, developers), jar entry lists and
  class-file major versions, runs MiMa of head against base, and fails on any
  difference; it MUST reject pre-release sbt versions and sbt 2 below 2.0.7.
- **FR-004**: sbt-schema-registry-plugin, gatling-amqp-plugin,
  gatling-jdbc-plugin and gatling-kafka-plugin MUST call that gate pinned by
  full commit SHA.
- **FR-005**: sbt-schema-registry-plugin MUST cross-build `_2.12_1.0` and
  `_sbt2_3` with projectMatrix from an sbt 2.0.9 launcher, keep blocking MiMa
  on both axes, and run scripted tests on the sbt 1 and sbt 2 minimums and on
  sbt 1.13.0 and 2.0.9.
- **FR-006**: gatling-kafka-plugin MUST drop the unused sbt-avro plugin, move
  its Scala example to sbt 2.0.9 while still checking it on sbt 1.13.0, and
  move to the first sbt-schema-registry-plugin release built by its sbt 2
  launcher.
- **FR-007**: Every CI job that runs sbt 2 MUST use JDK 17 or newer and MUST
  NOT restore an sbt 2 task cache or `target/` directory.

### Success Criteria

- **SC-001**: All five builds run on sbt 2.0.9 (or, for a build that failed
  its gate, on 1.13.0 with the reason recorded).
- **SC-002**: The first release of each publishing repository after its
  upgrade shows on Maven Central the same artifact IDs and POM dependency set
  as the release before it, with only the version changed.
- **SC-003**: sbt-schema-registry-plugin users on sbt 1 and on sbt 2 can
  upgrade to its next release without raising their sbt version.

## Assumptions

- The Gatling libraries publish Scala 2.13 artifacts built with
  `scalaVersion` 2.13.18.
- sbt-schema-registry-plugin 1.9.0 is published on both axes; where an axis
  lacks it, that axis keeps its newest existing baseline.
- All in-scope repositories are public, so they can call a reusable workflow
  in gatling-picatinny.

## Out of Scope

- demo-repository (maintainer's decision for this delivery).
- templates-gatling, which may still generate sbt 1 builds.
