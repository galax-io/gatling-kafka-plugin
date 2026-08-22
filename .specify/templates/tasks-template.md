---

description: "Task list template for feature implementation"
---

# Tasks: [FEATURE NAME]

**Input**: Design documents from `/specs/[###-feature-name]/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Per Constitution Principle IV (Test-First for Behavior Change), test tasks are MANDATORY for any task that changes observable behavior, and each must be written to fail before its implementation task. A feature specification cannot waive this. Only pure refactors that change no observable behavior may omit new tests, and those must be identifiable as such by the existing suite passing unchanged. Per Principle II, Kafka interactions are tested against Testcontainers or the `docker-compose.kafka.yml` stack — not mocks.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

This is a single-module Scala/sbt project (`gatling-kafka-plugin`):

- **Scala plugin sources**: `src/main/scala/org/galaxio/gatling/kafka/{protocol,actions,client,checks,request}/`
- **Java facade**: `src/main/java/org/galaxio/gatling/kafka/javaapi/`
- **Tests**: `src/test/scala/`, `src/test/java/`, `src/test/kotlin/`
- **Build/dependency truth**: `build.sbt`, `project/Dependencies.scala`, `project/plugins.sbt`

The sample tasks below use generic placeholder paths — replace them with real paths from the tree
above per plan.md.

<!--
  ============================================================================
  IMPORTANT: The tasks below are SAMPLE TASKS for illustration purposes only.

  The /speckit-tasks command MUST replace these with actual tasks based on:
  - User stories from spec.md (with their priorities P1, P2, P3...)
  - Feature requirements from plan.md
  - Entities from data-model.md
  - Endpoints from contracts/

  Tasks MUST be organized by user story so each story can be:
  - Implemented independently
  - Tested independently
  - Delivered as an MVP increment

  DO NOT keep these sample tasks in the generated tasks.md file.
  ============================================================================
-->

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create project structure per implementation plan
- [ ] T002 Initialize [language] project with [framework] dependencies
- [ ] T003 [P] Configure linting and formatting tools

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

Examples of foundational tasks (adjust based on your project):

- [ ] T004 Setup database schema and migrations framework
- [ ] T005 [P] Implement authentication/authorization framework
- [ ] T006 [P] Setup API routing and middleware structure
- [ ] T007 Create base models/entities that all stories depend on
- [ ] T008 Configure error handling and logging infrastructure
- [ ] T009 Setup environment configuration management

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - [Title] (Priority: P1) 🎯 MVP

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 1 (MANDATORY for behavior change — Principle IV) ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T010 [P] [US1] Unit test for [component] in src/test/scala/[path]/[Name]Spec.scala
- [ ] T011 [P] [US1] Testcontainers integration test for [Kafka journey] in src/test/scala/[path]/[Name]IntegrationSpec.scala

### Implementation for User Story 1

- [ ] T012 [P] [US1] Create [type] in src/main/scala/org/galaxio/gatling/kafka/[layer]/[Name].scala
- [ ] T013 [P] [US1] Extend KafkaProtocolMessage/KafkaMatcher if needed (Principle III: extend, don't duplicate)
- [ ] T014 [US1] Implement [component] in src/main/scala/org/galaxio/gatling/kafka/[layer]/[Name].scala (depends on T012, T013)
- [ ] T015 [US1] Wire into action/builder in src/main/scala/org/galaxio/gatling/kafka/actions/[Name].scala (inject collaborators — Principle III)
- [ ] T016 [US1] Add validation and error handling
- [ ] T017 [US1] Add logging for user story 1 operations

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - [Title] (Priority: P2)

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 2 (MANDATORY for behavior change — Principle IV) ⚠️

- [ ] T018 [P] [US2] Unit test for [component] in src/test/scala/[path]/[Name]Spec.scala
- [ ] T019 [P] [US2] Testcontainers integration test for [Kafka journey] in src/test/scala/[path]/[Name]IntegrationSpec.scala

### Implementation for User Story 2

- [ ] T020 [P] [US2] Create [type] in src/main/scala/org/galaxio/gatling/kafka/[layer]/[Name].scala
- [ ] T021 [US2] Implement [component] in src/main/scala/org/galaxio/gatling/kafka/[layer]/[Name].scala
- [ ] T022 [US2] Expose via DSL in src/main/scala/org/galaxio/gatling/kafka/KafkaDsl.scala (Principle I: API-surface change)
- [ ] T023 [US2] Integrate with User Story 1 components (if needed)

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - [Title] (Priority: P3)

**Goal**: [Brief description of what this story delivers]

**Independent Test**: [How to verify this story works on its own]

### Tests for User Story 3 (MANDATORY for behavior change — Principle IV) ⚠️

- [ ] T024 [P] [US3] Unit test for [component] in src/test/scala/[path]/[Name]Spec.scala
- [ ] T025 [P] [US3] Testcontainers integration test for [Kafka journey] in src/test/scala/[path]/[Name]IntegrationSpec.scala

### Implementation for User Story 3

- [ ] T026 [P] [US3] Create [type] in src/main/scala/org/galaxio/gatling/kafka/[layer]/[Name].scala
- [ ] T027 [US3] Implement [component] in src/main/scala/org/galaxio/gatling/kafka/[layer]/[Name].scala
- [ ] T028 [US3] Mirror on the Java facade in src/main/java/org/galaxio/gatling/kafka/javaapi/[Name].java

**Checkpoint**: All user stories should now be independently functional

---

[Add more user story phases as needed, following the same pattern]

---

## Phase N: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] TXXX [P] Documentation updates in README.md (separate PR — Principle V, one concern per PR)
- [ ] TXXX Code cleanup and refactoring (separate PR)
- [ ] TXXX Performance optimization across all stories
- [ ] TXXX [P] Additional unit tests in src/test/scala/
- [ ] TXXX Run `sbt scalafmtAll scalafmtSbt` then `sbt scalafmtCheckAll scalafmtSbtCheck compile test`
- [ ] TXXX Run `sbt "Test / runMain org.galaxio.gatling.kafka.examples.ExampleCoverageCheck"` (API-compat gate, Principle I)
- [ ] TXXX Run quickstart.md validation

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Final Phase)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - May integrate with US1 but should be independently testable
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) - May integrate with US1/US2 but should be independently testable

### Within Each User Story

- Tests MUST be written and MUST FAIL before implementation (Principle IV; exempt only for pure
  refactors with no observable behavior change)
- Models before services
- Services before endpoints
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes, all user stories can start in parallel (if team capacity allows)
- All tests for a user story marked [P] can run in parallel
- Models within a story marked [P] can run in parallel
- Different user stories can be worked on in parallel by different team members

---

## Parallel Example: User Story 1

```bash
# Launch all tests for User Story 1 together:
Task: "Unit test for [component] in src/test/scala/[path]/[Name]Spec.scala"
Task: "Testcontainers integration test for [Kafka journey] in src/test/scala/[path]/[Name]IntegrationSpec.scala"

# Launch all independent types for User Story 1 together:
Task: "Create [type] in src/main/scala/org/galaxio/gatling/kafka/[layer]/[Name].scala"
Task: "Extend KafkaProtocolMessage/KafkaMatcher as needed"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Test User Story 1 independently
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 → Test independently → Deploy/Demo
4. Add User Story 3 → Test independently → Deploy/Demo
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1
   - Developer B: User Story 2
   - Developer C: User Story 3
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before implementing
- Commit per tracked issue, not per task: one issue = one semantic commit, green on its own under
  `sbt scalafmtCheckAll scalafmtSbtCheck compile test` (Principle V)
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
