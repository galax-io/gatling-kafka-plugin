# gatling-kafka-plugin — Agent Guide

Kafka protocol plugin for Gatling — produce-only and request-reply load testing, with plain
serialization and Avro helpers.

> Sections above the `---` are **project-specific** — the concrete facts of this repo.
> Everything below the `---` is the reusable development process (galax-io convention),
> adapted to this repo's actual CI/release tooling.

## Role

- Act as a Principal Engineer in software development and performance testing.
- Bring strong Scala, Java, Kotlin, Gatling plugin, Kafka, and Avro expertise.
- Prefer small, clear, backward-compatible changes unless the task explicitly requires otherwise.

## Stack

- Scala 2.13 core on sbt; Gatling 3.13.5; Java 17+ (Temurin in CI).
- Kafka plugin for produce-only and request-reply flows.
- Confluent Kafka clients 7.9.2-ccs, kafka-streams-scala.
- Avro4s 4.1.2 + Confluent Schema Registry 7.9.2 (optional, `provided` scope).
- Java API facade (`javaapi`) with Kotlin-compatible usage and tests.
- Testcontainers + ScalaTest/MUnit; Docker Compose (`docker-compose.kafka.yml`) for a local broker.
- GitHub Actions CI, Scala Steward, Codecov, Sonatype publish via sbt-ci-release.

## Commands

```bash
sbt scalafmtAll scalafmtSbt                       # format — run before every push
sbt scalafmtCheckAll scalafmtSbtCheck             # format gate — must pass before push
sbt clean compile                                 # compile
sbt test                                          # unit specs + Testcontainers integration spec
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleCoverageCheck"      # example coverage + topic contract (no broker)
sbt "Gatling / test"                              # the 3 test harnesses
sbt 'set ThisBuild / version := "0.0.0-EXAMPLES-SNAPSHOT"' publishM2             # then run any example project below
bash scripts/install-hooks.sh                     # enable the pre-commit git hook — once per clone
```

Default verification: `sbt scalafmtCheckAll scalafmtSbtCheck compile test`.

The full Gatling line above is what CI runs; it needs Kafka + Zookeeper + Schema Registry
(`docker-compose.kafka.yml`). A pre-commit git hook (see Tooling) formats staged sources on every
commit; compile + tests stay in CI.

## Installed Skills

- Use the installed Scala, Java, Kotlin, TDD, and unit-test skills when they apply.
- Default skill set: `scala-pro`, `java-best-practices`, `kotlin-patterns`, `kotlin-testing`, `tdd-workflow`, `unit-test-utility-methods`.
- Prefer Scala guidance for core plugin/runtime code, Java guidance for `src/main/java/.../javaapi`, Kotlin guidance for Kotlin tests/examples, and TDD plus focused regression coverage for behavior changes.

## Structure

<!-- A light index, not a full tree. Base Scala package: org.galaxio.gatling.kafka -->
- `protocol/` -> Kafka protocol model, builders, Gatling wiring, shared producer/consumer settings.
- `actions/` -> publish-only and request-reply action/builder pairs.
- `client/` -> Kafka sender, dynamic consumer, message tracker, and tracker pool.
- `checks/` -> check materialization, Avro body checks, message preparer, and DSL helpers.
- `request/` -> message model, serialization implicits, and request builder DSL.
- `src/main/java/org/galaxio/gatling/kafka/javaapi/` -> Java/Kotlin-facing facade.
- `src/test/scala/` -> Scala examples, integration, and unit coverage.
- `examples/{scala,java,kotlin}/` -> one consumer project per language, each running its examples against the published artifact.

## Architecture

- Keep layers separate: `KafkaSender` sends, `KafkaMessageTracker` tracks, `DynamicKafkaConsumer` consumes. Don't merge concerns between them.
- `KafkaProtocolMessage` is the single wire representation. `KafkaMatcher` is the single matching contract. Extend these — don't invent parallel types.
- Treat Scala DSL, Java builders, defaults, and plugin semantics as compatibility-sensitive.
- Kafka interactions are async: review consumer lifecycle, tracker pool concurrency, reply correlation, timeout handling, and error propagation carefully.
- Apply SOLID when it improves clarity and testability. Inject dependencies; don't construct them inside action classes. Prefer KISS and DRY, but avoid premature abstraction in public APIs.

## Test Model

- Follow TDD where practical; add focused regression tests for behavior changes.
- Prefer a real broker (Testcontainers, or the Compose stack) over mocks when validating Kafka behavior.
- `KafkaGatlingTest`, `KafkaJavaapiMethodsGatlingTest`, and `KafkaConcurrencyLoadTest` are test simulations, not examples, and they are all `sbt "Gatling / test"` runs.
- The published examples are **not** in this build. They live in three consumer projects, one per language, each on the build tool its users use and each depending on the published artifact: `examples/scala` (sbt), `examples/java` (Maven), `examples/kotlin` (Gradle). Publish with `publishM2` under the sentinel version first, then run each with its own native task.
- **Do not try to run Java or Kotlin simulations from sbt.** `io.gatling.javaapi.core.Simulation` does not extend `io.gatling.core.scenario.Simulation`, and `gatling-test-framework` declares exactly one sbt fingerprint, matching only the Scala superclass — so naming one selects nothing and exits 0. Gatling's sbt plugin supports Scala only.
- `examples/kotlin` pins Gradle 8.12 in its committed wrapper because `io.gatling.gradle` 3.13.5.4 — the release matching Gatling 3.13.5 — cannot configure on Gradle 9. Bump the wrapper and the plugin together or not at all.
- `ExampleCoverageCheck` needs no broker. It fails if an example in any of the three projects has no recorded coverage, if two examples share a topic, or if a topic they use is missing from either broker definition.
- Preserve backward compatibility for published Scala and Java APIs.

---

<!-- ===================================================================== -->
<!-- DEVELOPMENT PROCESS — galax-io convention, adapted to this repo.       -->
<!-- ===================================================================== -->

## Boundaries

**Always:** format before commit (`sbt scalafmtAll scalafmtSbt`), branch from `main`, keep commits semantic and green, preserve backward compat for published Scala/Java APIs and downstream consumers. `build.sbt` + `project/Dependencies.scala` + `project/plugins.sbt` = dependency truth; `.github/workflows/` = CI/release truth.

**Ask first:** new deps or upgrades, changing public API signatures / observable behavior / serialized formats, editing another repo, release/publish workflow changes.

**Never:** force-push or commit to `main`, merge commits in PR branches (rebase only), commit broken code, opportunistic refactors outside scope, mock external systems where a real integration path exists, commit or publish `AGENTS.md`/`CLAUDE.md` unless the user explicitly asks.

Keep changes scoped to this repo and preserve existing user changes. Prefer `rg` for search.

## Milestones (ALWAYS)

Every piece of work is tied to a milestone. No exceptions unless explicitly told otherwise.

- **Every PR** is assigned to the active milestone before merging. No milestone = do not merge.
- **Every issue** fixed by a PR is closed when that PR lands on `main`. Don't leave completed issues open.
- **Spec work** (`specs/NNN-*/`) belongs to the milestone that owns the spec; link the spec PR to that milestone when you open it.
- **Active milestone** = the lowest-numbered open milestone matching the current spec/plan. Check `gh api repos/galax-io/gatling-kafka-plugin/milestones` if unsure.

Enforced by [`scripts/check-linkage.sh`](scripts/check-linkage.sh) plus two PreToolUse(Bash) hooks wired in [`.claude/settings.json`](.claude/settings.json) — [`linkage-guard.sh`](.claude/hooks/linkage-guard.sh) (gates release tagging only; normal push/PR/merge untouched) and [`milestone-guard.sh`](.claude/hooks/milestone-guard.sh) (blocks assigning an issue/PR to anything but the current release milestone):

- `scripts/check-linkage.sh --pr <N>` — gate one PR: milestone + `Closes #<issue>` + issue in the same milestone.
- `scripts/check-linkage.sh --for-tag vX.Y.Z` — gate a release: every milestone issue closed, every PR merged.
- `scripts/check-linkage.sh` — audit the active milestone (lowest-numbered open).
- `scripts/current-milestone.sh` — print the current release milestone as `<number>\t<title>`; single source of truth shared by the checker and the milestone guard.

> The `--for-tag` gate resolves `vX.Y.Z` to a milestone whose title starts with that **exact** version first (a dedicated patch milestone, e.g. `v1.1.1 <description>`); if none exists it falls back to the milestone whose title starts with `vX.Y.0`. Name release milestones `vX.Y.0 <description>` (or `vX.Y.Z <description>` for a dedicated patch milestone) for the gate to resolve; audit and `--pr` modes work with any milestone name.
>
> **This repo has no version-named milestones yet.** Until one exists, `current-milestone.sh` prints nothing and `milestone-guard.sh` stays out of the way, but `linkage-guard.sh` will block a `git tag vX.Y.Z` / tag push because `--for-tag` cannot resolve a milestone. Create the `vX.Y.0 …` milestone before the next release, or bypass deliberately (see Tooling).

## Commits & PRs

- **Spec-first.** `specs/NNN-*/` artifacts land as a `docs(speckit): add NNN-<feature> spec/plan/tasks` commit BEFORE any `feat`/`fix`. Never fold spec artifacts into implementation commits.
- **1 issue = 1 commit.** Each tracked GitHub issue maps to one semantic commit (`feat(scope): … (#NNN)`), green on its own (`sbt scalafmtCheckAll scalafmtSbtCheck compile test`). Docs and out-of-scope improvements go in separate PRs — never mixed with issue commits.
- **Conventional Commits drive the changelog & version.** git-cliff groups release notes by type (`feat`/`fix`/`perf`/`docs`/deps/…); the subjects since the last tag also guide the version you pick (`feat` → minor, `!:`/`BREAKING CHANGE` → major, else patch). Write accurate subjects.
- **Intent, not path.** No add-then-remove within a PR. Squash churn before review.
- **1 concern per PR.** Feature ≠ docs/README. Stack dependent PRs; update with `--force-with-lease`.
- **Idiomatic code.** Follow Scala/Java/Kotlin idioms and the conventions already in the codebase; no control-flow-by-exception, no dead or duplicated code.

## Release Process

Trunk-based with release branches. Trunk is `main`; `release/*` branches are cut from `main` for stabilization. Releases are **manual and tag-driven** — [`.github/workflows/ci.yml`](.github/workflows/ci.yml) only tests; [`.github/workflows/release.yml`](.github/workflows/release.yml) runs only on `v*` tags.

Pushing a `vX.Y.Z` tag on `main` or a `release/*` branch:

1. `release.yml` validates the tag sits on `main` or `release/*` (rejects stray tags).
2. `sbt clean compile test` runs as a release sanity gate.
3. `sbt ci-release` publishes to Sonatype (sbt-ci-release / dynver, PGP-signed).
4. Release notes are generated from Conventional Commits by git-cliff ([`cliff.toml`](cliff.toml), `orhun/git-cliff-action`), and a GitHub Release is created (`softprops/action-gh-release`).

### Minor/major release (e.g. 1.2.0, 2.0.0)

1. `git checkout -b release/X.Y.0 main`
2. `git push -u origin release/X.Y.0`
3. `git tag vX.Y.0` on the release branch
4. `git push origin vX.Y.0` — triggers `release.yml`

### Patch release (e.g. 1.2.1)

1. Fix lands on `main` first (via PR)
2. `git cherry-pick <fix-sha>` onto `release/X.Y.0`
3. `git tag vX.Y.1` on the release branch
4. `git push origin vX.Y.1`

### Milestone gate

A release milestone is **tag-ready** only when every issue in it is closed and every PR merged. Because a release is a deliberate local `git tag vX.Y.Z` / tag push, the [`linkage-guard`](.claude/hooks/linkage-guard.sh) hook actually gates it: it runs `check-linkage.sh --for-tag vX.Y.Z` and blocks the tag until the milestone passes. Name release milestones `vX.Y.0 …`, or `vX.Y.Z …` for a dedicated patch milestone, so the gate resolves (see Milestones). To enforce the same gate server-side, add a `scripts/check-linkage.sh --for-tag "$GITHUB_REF_NAME"` step to `release.yml` once milestones are version-named.

### Rules

- **Version comes from the tag** (dynver) — pick it from the Conventional Commits since the last tag: `feat` → minor, `!:`/`BREAKING CHANGE` → major, otherwise patch.
- **Tags only on `main` or `release/*`** — `release.yml` rejects tags anywhere else.
- **Never delete a release tag** after publish starts — creates stuck registry deployments.
- **Never reuse a version number** — Sonatype rejects duplicates permanently.
- **CI in `.github/workflows/` is the source of truth** for formatting, compile, tests, coverage, and release behavior.

## Tooling

- [`scripts/check-linkage.sh`](scripts/check-linkage.sh) — issue↔PR↔milestone contract checker (see Milestones). Needs `gh` + `jq`.
- [`scripts/current-milestone.sh`](scripts/current-milestone.sh) — resolves the current `vX.Y.Z` milestone; shared by the checker and the milestone guard.
- [`.claude/hooks/linkage-guard.sh`](.claude/hooks/linkage-guard.sh) — PreToolUse(Bash) hook; gates release-tag pushes only, ~0 tokens otherwise. Bypass: `LINKAGE_OFF=1 <cmd>`.
- [`.claude/hooks/milestone-guard.sh`](.claude/hooks/milestone-guard.sh) — PreToolUse(Bash) hook; blocks a `gh issue/pr … --milestone` assignment that isn't the current release milestone. Bypass a deliberate backlog move: `MILESTONE_GUARD_OFF=1 <cmd>`.
- [`setup-speckit.sh`](setup-speckit.sh) — installs spec-kit extensions/presets. Run deliberately: needs the `specify` CLI + network and installs from third-party GitHub archives, **auto-accepting spec-kit's untrusted-source prompt** — review the pinned sources in the script before running.
- [`cliff.toml`](cliff.toml) — git-cliff config: Conventional Commits → grouped GitHub Release notes (used by `release.yml`).
- [`.githooks/pre-commit`](.githooks/pre-commit) — shared git hook (wired via `core.hooksPath`): runs `sbt scalafmtAll scalafmtSbt` and re-stages the commit's files, so every commit is scalafmt-clean (compile + tests stay in CI). `-batch` + closed stdin so a failure aborts instead of hanging. Bypass with `SKIP_SCALAFMT=1` or `git commit --no-verify`.
- [`scripts/install-hooks.sh`](scripts/install-hooks.sh) — enable it (`git config core.hooksPath .githooks`); run once per clone. In a linked worktree with `extensions.worktreeConfig`, a per-worktree `core.hooksPath` can shadow it — the script reports that instead of silently no-op'ing.
- `specs/` — spec-kit working dir; features live in `specs/NNN-<feature>/`.

## Repo Notes

- `build.sbt`, `project/Dependencies.scala`, and `project/plugins.sbt` are the source of truth for build and dependency behavior.
- Changes in `client/`, message tracking, protocol wiring, or action execution can affect both correctness and observability under load.
- Real broker behavior is usually more valuable than mocks here.
