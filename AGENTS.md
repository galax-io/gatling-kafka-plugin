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
# install   sbt update Test/compile scalafmtCheckAll scalafmtSbtCheck mimaReportBinaryIssues coverage update coverageOff
# test      sbt test
sbt scalafmtAll scalafmtSbt                       # format — run before every push
sbt scalafmtCheckAll scalafmtSbtCheck             # format gate — must pass before push
sbt clean compile                                 # compile
sbt test                                          # unit specs + Testcontainers integration spec
sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleCoverageCheck"      # example coverage + topic contract (no broker)
sbt "Gatling / test"                              # every Gatling simulation on the test classpath
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
- `KafkaGatlingTest`, `KafkaFailureModesGatlingTest`, `KafkaJavaapiMethodsGatlingTest`, and `KafkaConcurrencyLoadTest` are test simulations, not examples, and they are all `sbt "Gatling / test"` runs.
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

A product repository has no milestones: a release is a business milestone of `galax-io/galaxio-sdlc-backlog` (see "Feature milestones"), and a PR here carries none.

- **Issues are for important tasks**, not technical ones. A PR that completes one writes `Closes owner/repo#N` (the issue closes when the PR lands on `main`); one PR of several writes `Part of owner/repo#N`. A technical PR (sync, pins, docs, CI) needs no issue: its commit already records it.
- **Spec work** (`specs/BCKLG-<N>-*/`) lives in the backlog and travels in the feature's backlog PR together with its work, never in a PR of its own.

## Feature milestones (ALWAYS)

A feature is an issue in `galax-io/galaxio-sdlc-backlog`. It belongs to exactly one backlog milestone whose description has the line `Release: business`: that milestone is its release.

- **Trunk-based.** `main` is the trunk: every PR, a feature's, a fix, a bot's or CI, is opened into `main` from a short-lived branch. Unfinished behaviour is not merged or stays behind a flag. The PR of a feature writes `Part of galax-io/galaxio-sdlc-backlog#<feature>`, which records it in the feature.
- **Work enters through the backlog.** A bug, an audit finding or a request is not worked on until it belongs to a feature: grouped into one or given one. An issue from outside stays in its repository and carries the label `status:needs-feature` until it is linked to a feature in the backlog.
- **Release.** The backlog's `scripts/release-build.sh <release>` gives this repository the next minor, `vX.Y.0`, when `main` has commits after its last tag, and runs the phase `prepare` in every such repository: `release/X.Y.0` is cut from `main` and this repository's checks run on it. Only when every repository is green does it run `publish` everywhere: the tag is set on the head `prepare` checked and the GitHub Release is made; `main` is not touched and the branch stays. A red check in one repository publishes nothing anywhere. The backlog's release of the milestone lists each repository's version and links its GitHub Release.
- **Patch.** A fix of a released version is a PR into `main` first, then `git cherry-pick -x` onto `release/X.Y.0` by a PR into that branch; `prepare` and `publish` with `vX.Y.Z` tag it there. A release branch is never merged into `main`.
- **Feature done.** The feature's documents PR in the backlog writes `Closes galax-io/galaxio-sdlc-backlog#<feature>` and is merged last; the work's PRs here write `Part of`.
- **Published releases are not edited.** A fix after the release is a patch on the release branch or the next release; a tag is never moved.

## Labels (ALWAYS)

One label scheme for every repository of the organization: `scripts/labels.json` in `galax-io/galaxio-sdlc-backlog`. A label says what the milestone and the feature link do not: the kind of work, its topic, how bad it is, where it stands.

- **Names.** Every label is `group:value`. Labels a tool owns (GitHub's `good first issue` and `help wanted`, Dependabot's, and those code reads, listed as `reserved`) are left alone.
- **Exactly one `type:*` on every open issue:** `bug`, `enhancement`, `docs`, `maintenance`, `question`; in the backlog `type:feature` (an issue of a business milestone) or `type:standing` (the standing feature of an older milestone). An issue without a type is not triaged.
- **At most one each:** `severity:*` (`critical`, `high`, `medium`, `low`: the impact, not the order of work), `status:*` (`needs-triage`, `needs-info`, `needs-feature`, `ready-for-agent`, `ready-for-human`, `blocked`), `tier:*`.
- **Any number:** `area:*` (topics shared by every repository), `module:*` (parts of one repository, declared in the scheme), `source:external`, `source:audit`.
- **Not labels.** The order of work is the milestone: no `priority:*`. The feature is the `Part of` link: no `feature:*`. A rejected or duplicate issue is closed with that reason and a comment: no `wontfix`, `invalid`, `duplicate`.
- **The engine's labels** `sdlc:*` live in the backlog: only the engine sets `sdlc:gate`, `sdlc:done` and `sdlc:failed`. The label of a run's workflow, `workflow:<pipeline name>`, is the maintainer's to set, at most one on an intent; the engine never sets or removes it.
- **A new label is a change to the scheme**, a PR to `scripts/labels.json`, not a label made in one repository. `scripts/sync-labels.sh` applies the scheme; `--check` lists what breaks it.

## Task number (ALWAYS)

A task of the backlog has one number across the organization: `BCKLG-<N>`, where `<N>` is its issue number in `galax-io/galaxio-sdlc-backlog`. It is the only number a spec gets, and it names the task wherever `#N` alone would be read as an issue or PR of another repository.

- **Spec.** Its directory is `specs/BCKLG-<N>-<name>/`, `<N>` its main task. Its `spec.md`, `plan.md` and `tasks.md` carry the line `**Задачи**: BCKLG-<N>[, BCKLG-<M>…]` under the title, or `**Tasks**: …` in a repository whose files are English only, read alike: every task it carries, the main one first. A task has one spec: before writing one, find the spec whose tasks line names the task, check it against the task as its issue says now, and update it rather than write another. A spec directory from before the number keeps its name.
- **Branch.** A task's documents branch in the backlog is `spec/BCKLG-<N>-<name>`, whether made by hand or by the engine, which works on it and opens no other branch in the backlog; the engine's run is `BCKLG-<N>-<time>`, and its branches in the code repositories are `sdlc/BCKLG-<N>-<time>`.
- **Commit.** A commit that does a task's work carries the trailer `Refs: BCKLG-<N>`; its subject stays a Conventional Commit.
- **PR.** Its body carries the line `Refs: BCKLG-<N>` beside `Part of galax-io/galaxio-sdlc-backlog#<N>`, or beside `Closes galax-io/galaxio-sdlc-backlog#<N>` in the task's documents PR.
- **Checked.** The backlog's `scripts/check-linkage.sh --specs` checks every spec there.

## Agents & Models (ALWAYS)

Route work by the reasoning it needs, not by habit; parallelize only where agents would not rebuild the same context. Every model is the newest of its class that the accounts can run; an older one is never used.

| Tier | Claude (effort) | Codex (effort) | Google (effort) | Work |
|---|---|---|---|---|
| deep | `claude-opus-5-5` (xhigh) | `gpt-6.2-sol` (xhigh) | `gemini-4-neon` (xhigh) | complex code, review and verification (every critic), research, root-cause analysis, specs/plans, architecture |
| light | `claude-sonnet-5-5` (high) | `gpt-6.1-terra` (high) | `gemini-4-hydrogen` (high) | simpler work a plan already spells out: task/issue generation, mechanical infra (CI config, version pins, renames, doc fixes) |
| trivial | `claude-haiku-5-5` (high) | `gpt-6-luna` (high) | `gemini-4-hydrogen` (low) | very simple edits, gathering context, finding files, bookkeeping |
| exceptional | `claude-fable-5-2` (medium) | `gpt-6.2-astra` (xhigh) | `gemini-4-carbon` (xhigh) | only when the maintainer names the chosen model for a task; no automatic selection or vendor fallback |

- **Newest only.**
  - When a newer model of a class appears, this table and every pin of it move to it in the same change: `.claude/settings.json`, `.claude/agents/`, `setup-speckit.sh` (in spec-kit-galaxio-bootstrap), and any code that routes models.
  - Terra stays at 6.1 and Luna at 6 because Codex refuses `gpt-6.2-terra` and `gpt-6.2-luna` to a ChatGPT account.
  - `gpt-6.2-sol` and `gpt-6.2-astra` need Codex CLI 0.167.2 or later (0.165 refuses them); `gpt-6.1-terra` needs 0.159 or later.
  - Claude Code's own aliases and its background model are pinned to the table in `.claude/settings.json` (`env`: `ANTHROPIC_DEFAULT_FABLE_MODEL`, `ANTHROPIC_DEFAULT_OPUS_MODEL`, `…_SONNET_MODEL`, `…_HAIKU_MODEL`, `ANTHROPIC_SMALL_FAST_MODEL`). Without that pin its `haiku` alias means an older Haiku.
- **Codex models by tier.** Deep work runs Sol on xhigh effort, light work runs Terra on high effort, and trivial work runs Luna on high effort. A stage keeps its tier through a vendor fallback; task labels do not switch the model inside its executor session.
- **Google models by tier.** Gemini CLI (`gemini`) and Antigravity (`agy`) follow the same tiers: deep runs `gemini-4-neon` at xhigh, light runs `gemini-4-hydrogen` at high, trivial runs `gemini-4-hydrogen` at low, and `gemini-4-carbon` at xhigh is exceptional (explicit maintainer choice only). Select the model explicitly with `--model <id>` (e.g. `agy --model gemini-4-neon` / `gemini --model gemini-4-neon`) and matching thinking depth.
- **Exceptional is explicit.** Fable, Astra and Carbon run only on their own vendor's harness, with no cross-vendor fallback. Token cost keeps them opt-in: the maintainer names the model on the chosen stage, never as defaultModel. The engine runs Fable at medium; interactive Claude Code uses `claude --model claude-fable-5-2 --effort medium`. Raising effort needs an explicit instruction.
- **Reviews are deep.** Every critic and reviewer runs at the deep tier, on the other vendor than the work it judges.
- **Research is adversarial.** Deep research gets a second opinion from the other vendor (`deep-researcher` does this via `codex exec --sandbox read-only`). A claim only one model supports is unverified, not a fact.
- **Every task carries its tier.** `tasks.md` lines add `[tier:deep]`, `[tier:light]` or `[tier:trivial]` next to `[P]`, and issues carry the matching `tier:*` label where the project labels them. When unsure, it is deep.
- **Parallelize by context, not by file.**
  - Every sub-agent builds its own context from scratch. Agents that must read the same code, spec, or repository repeat that cost and leave integration work behind.
  - `[tier:deep]` work that shares a context runs in one agent, task after task: the main session or a single deep sub-agent. This holds even when `[P]` marks the tasks as parallelizable.
  - Fan deep work out only where each agent's context differs: separate repositories, or subsystems an agent can learn without the others.
  - `[tier:light]` mechanical work (`infra-worker`) and `[tier:trivial]` work (`trivial-worker`) may fan out freely.
  - Dependent tasks stay sequential.
- **Pinned, not remembered.**
  - The Spec Kit kit (`.claude/skills/speckit-*`, `.agents/skills/speckit-*`, `.specify/` but `.specify/memory/`) is the same in every repository: `scripts/build-speckit-kit.sh` of spec-kit-galaxio-bootstrap generates it, pins the Claude Code skills to their tier (override with `SPECKIT_DEEP_MODEL` / `SPECKIT_LIGHT_MODEL`), and it arrives with `copier update` (or `scripts/adopt.sh --speckit`). `scripts/check-speckit.sh` fails when this repository's kit differs; do not edit it here and do not run `specify extension`, `preset` or `integration` commands here. The constitution (`.specify/memory/constitution.md`) is this repository's own.
  - `.claude/agents/` defines the tiered sub-agents, each with its tier's model and `effort`.
  - Where a project routes models in code, it routes by this table: a task or stage names its tier, and each harness runs its own vendor's model of that tier. A model outside the table is an error.

## Commits & PRs

- **Spec-first.** `specs/BCKLG-<N>-<feature>/` artifacts → `docs(speckit): add BCKLG-<N>-<feature> spec/plan/tasks` as the first commit of the backlog PR that carries the feature's work, BEFORE any `feat`/`fix`. Never folded into an implementation commit, never a spec-only PR; that PR merges together with the feature's PRs in the other repositories (backlog constitution, Principle II).
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

### Rules

- **Version comes from the tag** (dynver) — pick it from the Conventional Commits since the last tag: `feat` → minor, `!:`/`BREAKING CHANGE` → major, otherwise patch.
- **Tags only on `main` or `release/*`** — `release.yml` rejects tags anywhere else.
- **Never delete a release tag** after publish starts — creates stuck registry deployments.
- **Never reuse a version number** — Sonatype rejects duplicates permanently.
- **CI in `.github/workflows/` is the source of truth** for formatting, compile, tests, coverage, and release behavior.

## Tooling

- [`setup-speckit.sh`](setup-speckit.sh) — installs spec-kit extensions/presets. Run deliberately: needs the `specify` CLI + network and installs from third-party GitHub archives, **auto-accepting spec-kit's untrusted-source prompt** — review the pinned sources in the script before running.
- [`cliff.toml`](cliff.toml) — git-cliff config: Conventional Commits → grouped GitHub Release notes (used by `release.yml`).
- [`.githooks/pre-commit`](.githooks/pre-commit) — shared git hook (wired via `core.hooksPath`): runs `sbt scalafmtAll scalafmtSbt` and re-stages the commit's files, so every commit is scalafmt-clean (compile + tests stay in CI). `-batch` + closed stdin so a failure aborts instead of hanging. Bypass with `SKIP_SCALAFMT=1` or `git commit --no-verify`.
- [`scripts/install-hooks.sh`](scripts/install-hooks.sh) — enable it (`git config core.hooksPath .githooks`); run once per clone. In a linked worktree with `extensions.worktreeConfig`, a per-worktree `core.hooksPath` can shadow it — the script reports that instead of silently no-op'ing.
- `specs/` — spec-kit working dir; features live in `specs/NNN-<feature>/`.

## Repo Notes

- `build.sbt`, `project/Dependencies.scala`, and `project/plugins.sbt` are the source of truth for build and dependency behavior.
- Changes in `client/`, message tracking, protocol wiring, or action execution can affect both correctness and observability under load.
- Real broker behavior is usually more valuable than mocks here.
