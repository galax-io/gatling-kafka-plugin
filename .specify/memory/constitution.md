<!--
Sync Impact Report
==================
Version change: (unversioned template) → 1.0.0
Bump rationale: Initial ratification. Every placeholder replaced with concrete,
enforceable rules derived from the repository's existing process (AGENTS.md,
.github/workflows/, scripts/, README.md). No prior version existed, so this is
1.0.0 rather than a MAJOR bump of something.

Modified principles (template placeholder → ratified name):
  [PRINCIPLE_1_NAME] → I. Published API Compatibility (NON-NEGOTIABLE)
  [PRINCIPLE_2_NAME] → II. Real Broker Over Mocks
  [PRINCIPLE_3_NAME] → III. Layer Separation & Single Wire Contract
  [PRINCIPLE_4_NAME] → IV. Test-First for Behavior Change
  [PRINCIPLE_5_NAME] → V. One Concern per Change, Always Green

Added sections:
  [SECTION_2_NAME] → Technology & Compatibility Constraints
  [SECTION_3_NAME] → Development Workflow & Quality Gates
  Governance → concrete amendment procedure, versioning policy, compliance review

Removed sections: none

Templates requiring updates:
  ✅ .specify/templates/plan-template.md — Constitution Check filled with concrete gates
  ✅ .specify/templates/tasks-template.md — test-optionality note reconciled with
     Principle IV; path conventions corrected to this repo's Scala/sbt layout
  ✅ .specify/templates/spec-template.md — reviewed, already aligned (no mandatory
     section added or removed by this constitution)
  ✅ AGENTS.md — reviewed, consistent; constitution governs where they diverge
  ✅ README.md — reviewed, consistent (Compatibility, Migration Guide, Releasing)
  ⚠ .specify/templates/commands/*.md — directory does not exist in this install; N/A

Deferred TODOs: none

Amendment 1.0.1 (2026-08-19)
============================
Version change: 1.0.0 → 1.0.1 (PATCH)
Bump rationale: no obligation changes. Two factual corrections and one clarification of what an
existing obligation already meant.

Modified sections:
  Development Workflow & Quality Gates → "Full CI gate" named two simulations; CI runs three, and
    now also runs the published Scala examples under the same `Gatling / test`, plus the Java and
    Kotlin examples from the `examples/maven` consumer project.
  Principle I → the `ExampleSmokeValidation` clause now says what "constructing" requires. The
    obligation is unchanged; the implementation was not meeting it. `ExampleSmokeValidation` looked
    up a constructor without invoking it, so no example was ever constructed and the gate reported
    success for an example that could not build. Corrected under specs/007-multilang-example-ci-coverage.

Templates requiring updates:
  ✅ AGENTS.md — Test Model corrected in the same change
  ✅ README.md — Examples section corrected in the same change
  ✅ .specify/templates/plan-template.md — reviewed; its Constitution Check wording still holds
  ✅ .specify/templates/spec-template.md, tasks-template.md — reviewed, unaffected

Deferred TODOs: none

Amendment 1.1.0 (2026-08-21)
============================
Version change: 1.0.1 → 1.1.0 (MINOR)
Bump rationale: Principle I's example clause is restated in terms of what now exists, and it demands
more than before — every example compiled AND run from a consumer project, where the old clause asked
only that a gate construct them. Materially expanded guidance, so MINOR rather than PATCH.

Modified sections:
  Principle I → the `ExampleSmokeValidation` clause named a gate that could only construct examples
    while they shared this build's classpath. They no longer do: they live in one consumer project
    per language, each on that language's own build tool, each depending on the published artifact.
    Running them constructs them, so the requirement is now to run them. `ExampleCoverageCheck`
    (renamed from `ExampleSmokeValidation`) keeps the separate obligation that no example goes
    uncovered.
  Development Workflow & Quality Gates → "Full CI gate" updated to the three example projects.

Templates requiring updates:
  ✅ AGENTS.md — Commands and Test Model corrected in the same change
  ✅ README.md — Examples section corrected in the same change
  ✅ .specify/templates/* — reviewed, unaffected

Deferred TODOs: none
-->

# Gatling Kafka Plugin Constitution

## Core Principles

### I. Published API Compatibility (NON-NEGOTIABLE)

The Scala DSL under `org.galaxio.gatling.kafka`, the Java facade under
`org.galaxio.gatling.kafka.javaapi`, default protocol settings, and serialized wire formats form a
published contract consumed by downstream Gatling simulations. Sonatype releases are permanent and
cannot be withdrawn.

- Changes to public signatures, observable behavior, or serialized formats MUST be proposed and
  approved before implementation. They are never a side effect of another change.
- A breaking change MUST carry a `!:` or `BREAKING CHANGE` Conventional Commit marker so it drives
  a major version, and MUST ship with a Migration Guide entry in `README.md` in the same PR.
- Deprecate before removing: a replaced entry point MUST keep compiling for at least one minor
  release, annotated as deprecated with its replacement named.
- Every published example simulation MUST be compiled and run against the current API, from a
  consumer project that depends on the published artifact — one per language, on that language's own
  build tool. Running constructs the scenario and the protocol, so it subsumes the construction check
  a gate inside this build used to attempt. A failure there is an API break to reconsider, not a
  check to relax.
- `ExampleCoverageCheck` MUST keep failing when an example has no recorded coverage, so a new example
  cannot be added without being run somewhere.

**Rationale**: Users pin one plugin version against one Gatling version. Silent signature or
default drift breaks load-test suites at runtime, in the environment where they are hardest to
diagnose, and the broken artifact can never be unpublished.

### II. Real Broker Over Mocks

Kafka behavior MUST be validated against a real broker — Testcontainers under `sbt test`, or the
`docker-compose.kafka.yml` stack for the Gatling simulations CI runs.

- Mocks and stubs are permitted only for units with no Kafka interaction: matchers, serializers,
  check materialization, and builder wiring.
- Consumer lifecycle, tracker-pool concurrency, reply correlation, timeout handling, and error
  propagation MUST be exercised end-to-end against a broker, never asserted against a stub.
- Where a real integration path exists, substituting a mock for it MUST be treated as a gap in
  coverage rather than as coverage.

**Rationale**: Rebalancing, offset-commit timing, and correlation races are precisely the behaviors
a mock reproduces incorrectly, and precisely the behaviors this plugin exists to get right.

### III. Layer Separation & Single Wire Contract

`KafkaSender` sends, `KafkaMessageTracker` and `KafkaMessageTrackerPool` track, and
`DynamicKafkaConsumer` consumes. These responsibilities MUST NOT be merged into one another.

- `KafkaProtocolMessage` is the single wire representation and `KafkaMatcher` the single matching
  contract. Extend them. Parallel message or matcher types MUST NOT be introduced.
- Actions MUST receive their collaborators by injection and MUST NOT construct them internally, so
  each layer stays independently testable.
- Control flow MUST NOT be expressed through exceptions, and dead or duplicated code MUST NOT be
  merged.
- Abstraction is introduced when a second real caller exists, not in anticipation of one. In public
  API surface a speculative abstraction immediately becomes a compatibility obligation under
  Principle I.

**Rationale**: Send, track, and consume fail independently and under different conditions. Keeping
them separate is what makes a failure attributable to one of them; one wire type and one matching
contract are what keep that attribution meaningful across the Scala and Java surfaces.

### IV. Test-First for Behavior Change

Every behavior change MUST land with a test that fails before the change and passes after it.

- Follow red-green-refactor where practical. At minimum, add a focused regression test that names
  the behavior being introduced or fixed.
- A bug fix MUST include a test that reproduces the bug against the pre-fix code.
- Tests are NOT optional for behavior work, in specs, plans, or task lists. A feature specification
  cannot waive this principle.
- Pure refactors that change no observable behavior are exempt from new tests and MUST be
  demonstrable as such by the existing suite passing unchanged.

**Rationale**: This plugin's failures are asynchronous and intermittent — a missed reply, a
mistimed commit, a leaked consumer. Only a test written to fail first proves the fix addresses the
reported behavior rather than coincidentally hiding it.

### V. One Concern per Change, Always Green

- Spec-first: `specs/NNN-*/` artifacts land as their own `docs(speckit): …` commit BEFORE any
  `feat` or `fix` commit. Spec artifacts MUST NOT be folded into implementation commits.
- One tracked issue maps to one semantic commit (`feat(scope): … (#NNN)`), and each such commit
  MUST be green on its own under `sbt scalafmtCheckAll scalafmtSbtCheck compile test`.
- One concern per PR. Documentation, refactors, and opportunistic improvements go in separate PRs
  and MUST NOT be mixed into an issue commit.
- Commits express intent, not path: no add-then-remove churn within a PR. Squash before review.
- Every PR MUST be assigned to the active milestone and MUST close its issue via `Closes #NNN`. A
  PR without a milestone MUST NOT merge.
- Conventional Commit subjects MUST be accurate. git-cliff derives the release notes from them and
  the release version is chosen from them.

**Rationale**: The changelog, the released version number, and the milestone release gate are all
generated from commit and PR metadata. Inaccurate metadata produces a wrong release, not merely an
untidy history.

## Technology & Compatibility Constraints

- Scala 2.13 on sbt, Java 17+. `build.sbt`, `project/Dependencies.scala`, and
  `project/plugins.sbt` are the single source of truth for language, Gatling, Kafka, and Avro
  versions. This constitution MUST NOT restate those version numbers.
- Avro4s and Confluent Schema Registry support is `provided` scope and MUST remain optional: the
  plugin MUST stay usable with plain serialization and no Schema Registry on the classpath.
- New dependencies and non-Scala-Steward upgrades require approval before merge. Adding a
  dependency to the published artifact's `compile` scope is an API-surface decision governed by
  Principle I.
- Gatling compatibility is per-release-line and published in the README compatibility table. That
  table MUST be updated in the same PR that changes a supported Gatling version.
- `.github/workflows/` is the source of truth for formatting, compile, test, coverage, and release
  behavior. Local commands mirror CI; where the two disagree, CI is correct and the local command
  is fixed.

## Development Workflow & Quality Gates

**Branching**: Trunk-based. Branch from `main`; `release/*` branches are cut from `main` for
stabilization. Rebase only — merge commits MUST NOT appear in PR branches. Never force-push to
`main` and never commit directly to `main`.

**Before every push**: format with `sbt scalafmtAll scalafmtSbt`, then verify with
`sbt scalafmtCheckAll scalafmtSbtCheck compile test`. The shared `.githooks/pre-commit` hook,
enabled once per clone via `scripts/install-hooks.sh`, enforces formatting on every commit;
compile and tests are enforced by CI.

**Full CI gate** requires the Compose stack (Kafka, Zookeeper, Schema Registry) and runs
`KafkaGatlingTest`, `KafkaJavaapiMethodsGatlingTest`, and `KafkaConcurrencyLoadTest` under coverage
alongside `sbt test`. The published examples are not in this build: they run from the three consumer
projects under `examples/`, one per language, against the published artifact. Java and Kotlin cannot
run in sbt at all — Gatling's sbt plugin supports Scala only. The exact invocations live in
`AGENTS.md` and `.github/workflows/ci.yml`.

**Milestone linkage** is enforced by `scripts/check-linkage.sh` with the `linkage-guard` and
`milestone-guard` PreToolUse hooks. A release milestone is tag-ready only when every issue in it is
closed and every PR merged. Release milestones MUST be named `vX.Y.0 <description>`, or
`vX.Y.Z <description>` for a dedicated patch milestone, so that `--for-tag` resolves.

**Release** is manual and tag-driven. The version comes from the tag via dynver and is chosen from
the Conventional Commits since the last tag: `feat` → minor, `!:` or `BREAKING CHANGE` → major,
otherwise patch. Tags are valid only on `main` or `release/*`. A published tag MUST NEVER be
deleted and a version number MUST NEVER be reused.

## Governance

This constitution supersedes ad-hoc practice. Where it and `AGENTS.md` disagree, this document
governs and `AGENTS.md` MUST be corrected in the same PR that surfaces the conflict. `AGENTS.md`
remains the runtime development guide and carries the operational detail; this document carries the
non-negotiables.

**Amendment procedure**: an amendment is a PR that (a) edits this file, (b) updates the Sync Impact
Report at the top of it, and (c) updates every dependent artifact the change touches —
`.specify/templates/plan-template.md`, `.specify/templates/spec-template.md`,
`.specify/templates/tasks-template.md`, `AGENTS.md`, and `README.md`. An amendment that leaves a
dependent artifact contradicting a principle is incomplete and MUST NOT merge.

**Versioning policy** for this document, semantic and independent of the plugin's release version:

- **MAJOR**: a principle is removed, or redefined in a way that invalidates previously compliant
  work.
- **MINOR**: a principle or section is added, or existing guidance is materially expanded.
- **PATCH**: clarification, wording, or typo fix that changes no obligation.

**Compliance review**: every PR review MUST verify the change against these principles. A deviation
is permitted only when it is recorded in the plan's Complexity Tracking table with the rejected
simpler alternative named. An undocumented deviation blocks merge.

**Version**: 1.1.0 | **Ratified**: 2026-07-31 | **Last Amended**: 2026-08-21
