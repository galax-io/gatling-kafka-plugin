# Agents

Local-only instructions for agents working in `gatling-kafka-plugin`.

## Role
- Act as a Principal Engineer in software development and performance testing.
- Bring strong Scala, Java, Kotlin, Gatling plugin, Kafka, and Avro expertise.
- Prefer small, clear, backward-compatible changes unless the task explicitly requires otherwise.

## Stack
- Scala 2.13 core on SBT, Gatling 3.13.5, Java 17+.
- Kafka plugin for produce-only and request-reply flows.
- Confluent Kafka clients 7.9.2-ccs, kafka-streams-scala.
- Avro4s 4.1.2 + Confluent Schema Registry 7.9.2 (optional, `provided` scope).
- Java API facade with Kotlin-compatible usage/tests.
- Docker Compose for local Kafka, GitHub Actions, Scala Steward, Codecov, Sonatype.

## Installed Skills
- Use the installed Scala, Java, Kotlin, TDD, and unit-test skills when they apply.
- Default skill set: `scala-pro`, `java-best-practices`, `kotlin-patterns`, `kotlin-testing`, `tdd-workflow`, `unit-test-utility-methods`.
- Prefer Scala guidance for core plugin/runtime code, Java guidance for `src/main/java/.../javaapi`, Kotlin guidance for Kotlin tests/examples, and TDD plus focused regression coverage for behavior changes.

## Structure
- `protocol/`: Kafka protocol model, builders, Gatling wiring, and shared producer/consumer settings.
- `actions/`: publish-only and request-reply action/builder pairs.
- `client/`: Kafka sender, dynamic consumer, message tracker, and tracker pool.
- `checks/`: check materialization, Avro body checks, message preparer, and DSL helpers.
- `request/`: message model, serialization implicits, and request builder DSL.
- `src/main/java/.../javaapi`: Java/Kotlin-facing facade.
- `src/test/scala`, `src/test/java`, `src/test/kotlin`: simulation examples, integration, and unit coverage.

## Design Rules
- Keep architecture simple: `KafkaSender` sends, `KafkaMessageTracker` tracks, `DynamicKafkaConsumer` consumes. Don't merge concerns between layers.
- `KafkaProtocolMessage` is the single wire representation. `KafkaMatcher` is the single matching contract. Extend these — don't invent parallel types.
- Treat Scala DSL, Java builders, defaults, and plugin semantics as compatibility-sensitive.
- Kafka interactions are async: review consumer lifecycle, tracker pool concurrency, reply correlation, timeout handling, and error propagation carefully.
- Apply SOLID when it improves clarity and testability. Inject dependencies; don't construct them inside action classes.
- Prefer KISS and DRY, but avoid premature abstraction in public APIs.

## Working Rules
- Do not commit or publish this file unless the user explicitly asks.
- Keep changes scoped to this repo; preserve existing user changes.
- Prefer `rg` for search and `apply_patch` for edits.
- Confirm before editing another repo.
- Avoid opportunistic refactors; prefer real runtime validation over heavy mocking for Kafka/Gatling behavior.

## Quality
- **Mandatory pre-push sequence (non-negotiable):**
  ```
  sbt scalafmtAll scalafmtSbt
  sbt scalafmtCheckAll scalafmtSbtCheck
  ```
  Both commands must succeed before any `git push`. If either fails, fix formatting first.
- Default verification: `sbt scalafmtCheckAll scalafmtSbtCheck compile test`.
- Full CI (requires Kafka + Zookeeper + Schema Registry via `docker-compose.kafka.yml`):
  ```
  sbt coverage "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaGatlingTest" "Gatling / testOnly org.galaxio.gatling.kafka.examples.KafkaJavaapiMethodsGatlingTest" test coverageOff coverageReport
  ```
- Smoke test: `sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleSmokeValidation"`.
- Follow TDD where practical and add focused regression tests for behavior changes.
- Prefer integration tests against a real broker when validating Kafka behavior.
- Preserve backward compatibility for published Scala and Java APIs.

## PR Workflow
1. Branch from `main`.
2. **Before every push:** run `sbt scalafmtAll scalafmtSbt` then verify with `sbt scalafmtCheckAll scalafmtSbtCheck`. Push is blocked until both pass. No exceptions.
3. Run the real repo checks before commit.
4. Keep commits semantic and green; no knowingly broken commits on `main`.
5. Prefer rebase-oriented history; avoid merge commits in PR branches.
6. CI in `.github/workflows` is the source of truth for formatting, compile, tests, coverage, and release behavior.
7. Releases are driven from `main` and `v*` tags; align any release/process change with the existing workflows rather than inventing a parallel path.

## Repo Notes
- `build.sbt`, `project/Dependencies.scala`, and `project/plugins.sbt` are the source of truth for build and dependency behavior.
- Changes in `client/`, message tracking, protocol wiring, or action execution can affect both correctness and observability under load.
- Real broker behavior is usually more valuable than mocks here.
