# Feature Specification: v2.0.0 Cleanup — Validated Removal Sweep

**Feature Branch**: `006-v2-cleanup-sweep`

**Created**: 2026-08-09

**Status**: Draft

**Input**: User description: "https://github.com/galax-io/gatling-kafka-plugin/milestone/15 рационально реши что действительно можно удалить, пробегись по всему репозиторию чтобы понять рациональность удаления, если видешь костыли какие можно упростить тоже надо упростить"

**Milestone**: [#15 — v2.0.0 Cleanup](https://github.com/galax-io/gatling-kafka-plugin/milestone/15) (5 open issues: [#231](https://github.com/galax-io/gatling-kafka-plugin/issues/231), [#232](https://github.com/galax-io/gatling-kafka-plugin/issues/232), [#216](https://github.com/galax-io/gatling-kafka-plugin/issues/216), [#215](https://github.com/galax-io/gatling-kafka-plugin/issues/215), [#181](https://github.com/galax-io/gatling-kafka-plugin/issues/181))

---

## Why this feature exists

The milestone proposes deleting roughly 1,100 lines across the library, its tests, its examples and
its build. Deleting published symbols is irreversible on Sonatype, so each removal has to be
justified before it is made, not after. This specification is the record of that justification: every
item below carries a verdict backed by evidence gathered from this repository, and the verdicts —
not the issue text — are what the implementation is held to.

Three verdicts differ from what the milestone's issues assert. They are called out explicitly in
*Audit Verdicts* below, because acting on the issue text alone would delete something that is not
dead, and would leave dead surface behind that no issue names.

## Audit Verdicts

Read this section before planning. It is the deliverable the user asked for ("rationally decide what
can really be deleted"), and it overrides the issue bodies wherever the two disagree.

### A. Confirmed dead — remove

| # | Item | Evidence gathered in this audit |
|---|---|---|
| A1 | `sessionWindowedSerde`, `consumedFromSerde` (`request/KafkaSerdesImplicits.scala`) | Repository-wide search finds no caller in `src/`, `README.md` or the examples — only their own declarations, their deprecation notes, and the README paragraph announcing their removal |
| A2 | `kafka-streams-scala` dependency, the `kafka-streams` + `kafka-streams-scala` `kafkaOverrides` pins, the `deprecated:` justification entry, and contract rule DR-4 with its check | Each exists only to support A1. `kafka-streams-scala` is inherited by every consumer; the overrides pin it against Confluent's rebuild; the justification map records the debt; DR-4 is a budget of exactly one, written for this single case |
| A3 | The topic-less `send(...)` family — `request/builder/KafkaRequestBuilderBase.scala` top-level `send` overloads, and the whole `javaapi/request/builder/KafkaRequestBuilderBase.java` overload matrix that routes through them | Every builder they produce carries `producerTopic = None`; `KafkaAction.resolveProducerTopic` turns that into the failure `"Kafka producer topic is not defined."` at send time, so no scenario built through them can ever send. `sendWithClass(payload, vClass, headers)` additionally calls `Serdes.serdeFrom(Object.class)`, which throws `IllegalArgumentException` while the scenario is still being constructed |
| A4 | `KafkaProtocolMessage.responseCode`, and the `KafkaCheckType.ResponseCode` enum constant | Nothing anywhere assigns `responseCode` on a message: neither `KafkaProtocolMessage.from` nor the construction in `KafkaAction.resolveToProtocolMessage` sets it, so the two reply-received paths in `KafkaMessageTracker` that forward `message.responseCode` have only ever forwarded `None`. `KafkaDsl.simpleCheck` is the only producer of a Kafka `CheckBuilder` and it returns `Simple`, so the `ResponseCode` branch in `KafkaChecks.toScalaCheck` is unreachable — and it is identical to the `Simple` branch beside it, both building through `kafkaStatusCheck`. **Scope limit:** the reporting slot itself stays. `KafkaRequestReplyAction` and `KafkaMessageTracker.failPending` populate it with the real failure type on KO paths; only the always-empty field on the message goes |
| A5 | `KPProducerSettingsStep.timeout` / `.withDefaultTimeout` (Scala `protocol/KafkaProtocolBuilder.scala` only) | No example, test or README recipe reaches them. The one `.withDefaultTimeout` in `examples/KafkaGatlingTest.scala` sits after `.consumeSettings(...)`, so it is `KPConsumeSettingsStep`'s — which stays. The Java facade's `KPProducerSettingsStep` never had timeout methods, so only the Scala step is affected |
| A6 | `KafkaRequestFailureMessages.buildFailure`, plus the three spec cases that exercise only it | Search finds callers only in `actions/KafkaRequestFailureMessagesSpec.scala`; the production call site went with the change tracked as #136 |
| A7 | `ExpressionBuilder.bytes(String)` (`javaapi/request/expressions/`) | Package-private with zero callers; not published API |
| A8 | `AvroBodyCheckBuilder`'s `private type KafkaCheckMaterializer` | Reported by the compiler under `-Wunused:privates` (see A10) |
| A9 | `KafkaMessageTrackerPool.completionCause`'s `CompletionException` unwrap branch | The readiness value is a plain `CompletableFuture` created in `DynamicKafkaConsumer.requestTopicSubscription` and returned unwrapped; every completion path calls `completeExceptionally` with a raw exception, and `whenCompleteAsync` is registered on that same future rather than a derived stage — so the callback can never observe a `CompletionException` |
| A10 | 22 unused imports across 12 files, plus the unused private type in A8 | **Measured, not estimated**: compiled with `-Wunused:imports,privates,locals,patvars` and `-Xfatal-warnings` lifted. 7 unused imports in `src/main` (`actions/KafkaRequestAction.scala`, `checks/AvroBodyCheckBuilder.scala`, `checks/KafkaCheckSupport.scala`, `client/DynamicKafkaConsumer.scala` ×3, `protocol/KafkaProtocol.scala`) and 15 in `src/test` (`examples/KafkaGatlingTest.scala` ×9, `client/DynamicKafkaConsumerSpec.scala` ×2, and one each in `examples/BasicSimulation.scala`, `examples/KafkaJavaapiMethodsGatlingTest.scala`, `examples/ReadmeExamplesCompileOnly.scala`, `integration/TrackerAcquisitionIsolationSpec.scala`). `locals` and `patvars` produced no findings at all |
| A11 | Stale narration: the comment in `client/DynamicKafkaConsumer.scala` describing rethrow behaviour that no longer exists, and the `KafkaProtocolMessage` scaladoc pointing at `KafkaProtocolBuilderNew` | Both name code deleted by earlier changes; `KafkaProtocolBuilderNew` does not exist in the repository |
| A12 | `build.sbt`'s empty `avroSchemas` setting, its commented-out `RegistrySubject` import, and the commented-out `schemaRegistryUrl` line | `avroSchemas` is `Seq()`, so `schemaRegistrySubjects ++= avroSchemas` adds nothing |

### B. Dead surface no issue names — found by this audit, remove with the rest

| # | Item | Evidence |
|---|---|---|
| B1 | `checks/KafkaCheckMaterializer.avroBody[T]`, `checks/KafkaMessagePreparer.avroPreparer[T]`, and the `AvroErrorMapper` string only `avroPreparer` uses | Both public entry points named `avroBody` — `KafkaCheckSupport.avroBody` for Scala and `KafkaDsl.avroBody()` for Java — route to `AvroBodyCheckBuilder._avroBody`, which materializes through `kafkaStatusCheck`. `KafkaCheckMaterializer.avroBody` has no caller anywhere in `src/`. `AvroBodyCheckBuilder` even carries a comment explaining why the guard lives in the extractor "not on `KafkaMessagePreparer.avroPreparer`" — the preparer it defers to is itself unreachable |
| B2 | `request/builder/KafkaAttributes.producerTopic` is `Option[Expression[String]]` but can only ever be `Some` once A3 lands | Six construction sites exist, all in `KafkaRequestBuilderBase.scala`. Three set `producerTopic = None` (`:62`, `:76`, `:92`) and all three belong to the topic-less family A3 removes; the surviving three (`:18`, `:37` in `OnlyPublishStep`, `:116` in `RROutTopicStep`) always pass a real topic. The `Option` therefore encodes a state that cannot occur, and `KafkaAction.missingProducerTopicError` is a diagnostic for that impossible state. **This is a cascade of A3, not an independent finding** — it is recorded as its own verdict because it changes a published signature and RE-5 admits no unrecorded removal |

Removing B1 and B2 changes published signatures, which is exactly why they belong in this release
rather than a later one. B2 in particular is the kind of change Constitution I forbids as a silent
side effect: it is recorded here so that it is a decision, not a consequence.

### C. Contested — the issue's stated reason does not hold

| # | Item | What the issue says | What the code shows | Verdict |
|---|---|---|---|---|
| C1 | `KafkaMessageTrackerPool`'s `idleSweep.cancel(false)` in the termination hook | "belt-and-braces: the executor is already shut down", and therefore the `idleSweep` val "has no readers" | `idleSweep.cancel(false)` runs *before* `setupExecutor.shutdown()`, not after. Between them the hook waits on the consumer future and drains the continuation executor — a window that can last seconds, during which an uncancelled periodic sweep would keep firing. The `idleSweep` val has no readers only *after* the cancel is removed, so the second claim is a consequence of the first, not independent evidence | **Keep.** This is not dead code. If a later change still wants it gone, it needs a different justification and a stated argument about that teardown window — not this one |

### D. Explicitly out of scope

| Item | Why |
|---|---|
| `Global / concurrentRestrictions += Tags.limit(Tags.Test, 2)` in `build.sbt` | A genuine workaround — its own comment names the real fix (one shared broker across the integration specs instead of one per suite). That is a refactor of seven test files with its own risk profile, and it belongs with the test-suite milestone, not with a removal sweep |
| Adding Kotlin compilation to the build, or relocating the Kotlin examples | Decided against during clarification. The examples stay where they are and the build gains no Kotlin toolchain; only the broken example is fixed (US5) |
| Adding test coverage of any kind | Coverage gaps are tracked separately (tracker-pool acquire/release units; the untested `checks/` package). This feature only removes what cannot fail and only adds the guard in US2 |
| `TrackerLifetimeSpec` tests (2), (6) and (7) | These pin real races. They are owned by the reply-channel lifecycle work, which this feature does not wait for and does not perform — so the races are still live and the tests still have something to detect. They die with that design change or not at all |
| The `javaapi` request-reply overload matrix (`RROutTopicStep`, `OnlyPublishStep`) | Tracked separately as a Java-API-surface concern in an earlier milestone. Only the part of the matrix that routes through the topic-less `send(...)` — item A3 — is removed here, because that part cannot work at all |

---

## Clarifications

### Session 2026-08-09

- Q: Milestone 15 is the highest-numbered milestone, with eleven 1.x milestones still open ahead of it. What does this feature assume about them? → A: Land independently — implement and release without waiting; the removed signatures are verified by hand against the previous release rather than by an automated binary-compatibility guard.
- Q: FR-010 makes the build fail on unused code. How wide should that guard be? → A: Unused imports, private members, locals and pattern variables. Unused parameters and unused implicits are deliberately excluded.
- Q: Nothing compiles `src/test/kotlin/` and one example no longer parses. How should this be resolved? → A: The Kotlin examples stay exactly where they are. Do not wire a Kotlin compiler into the build and do not relocate them — fix the broken example against the current API and leave the layout alone.

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Every DSL entry point a simulation can reach actually works (Priority: P1)

A performance engineer writing a Kafka load test reads the DSL, picks a `send(...)` that matches the
shape of their data, and builds a scenario. Today several of those choices compile, look correct,
and then fail at run time or while the scenario is being constructed — because they were orphaned by
an earlier refactor and kept alive only by the compatibility freeze. Alongside them, the plugin makes
every consumer inherit a Kafka Streams library that nothing in the plugin uses.

After this story, every entry point the engineer can reach either works or does not exist, and the
dependency the removed surface was holding up disappears from their build.

**Why this priority**: This is the reason the release carries a major number. It is the only item in
the milestone whose absence would make the major version unjustified, and it is the change consumers
will actually notice — in their dependency tree and in their editor's completion list.

**Independent Test**: Build a scenario using each surviving `send(...)` shape against a real broker
and observe a sent record; then inspect the published POM and confirm `kafka-streams-scala` is no
longer among the dependencies a consumer inherits.

**Acceptance Scenarios**:

1. **Given** a simulation author browsing the Scala DSL, **When** they look for a way to send without
   naming a topic, **Then** no such method exists, and the compiler directs them to the topic-first
   form that works.
2. **Given** a simulation author browsing the Java facade, **When** they look for a `send(...)` that
   does not follow `.topic(...)` or `.requestReply()...`, **Then** no such method exists — including
   the headers-taking variant that used to throw while the scenario was being built.
3. **Given** a consumer resolving the plugin from Maven Central with no extra repository configured,
   **When** they inspect what the plugin brings in, **Then** the inherited set is the Kafka client,
   the Scala library and Avro core — and every one of them is justified by a component that uses it.
4. **Given** a failed request-reply, **When** the reader looks at its reported failure type, **Then**
   it still names the real cause — while the message-level response-code field that was always empty
   is gone, and no check type promises to read one.
5. **Given** the published-POM contract check, **When** it runs, **Then** it passes with a
   justification map containing only entries backed by real use, and with no special allowance for
   deprecated dependencies.

---

### User Story 2 - Dead code in the library is gone, and cannot silently come back (Priority: P2)

A contributor opening the plugin's sources should be able to trust that what is there is reachable.
Today the library carries orphaned helpers, an unreachable Avro check path, unused imports, a private
type nothing uses, a dead exception-unwrapping branch, and comments describing code that no longer
exists. Every one of them costs a future reader the work of proving it dead again.

After this story, the residue is gone, and the compiler enforces that the import- and privates-level
part of it stays gone.

**Why this priority**: The removals are individually small; the guard is what makes the story worth
doing. Without it the next audit re-derives the same findings. It ranks below US1 because none of it
changes what a consumer can do.

**Independent Test**: Compile the project and the test sources with the unused-code warnings enabled
and the build's existing fatal-warning setting in force; the build succeeds, and re-introducing any
unused import fails it.

**Acceptance Scenarios**:

1. **Given** the library sources, **When** a reader looks for the Avro check preparer path, **Then**
   there is exactly one Avro body check path and it is the one both the Scala and Java entry points
   use.
2. **Given** the build with unused-code warnings enabled, **When** it compiles the main and test
   sources, **Then** it reports nothing and succeeds.
3. **Given** a contributor who adds an import and does not use it, **When** they build, **Then** the
   build fails and names the import.
4. **Given** a reader following a code comment, **When** the comment describes behaviour or names a
   type, **Then** that behaviour and that type exist in the current sources.
5. **Given** the tracker pool's teardown path, **When** a run terminates, **Then** the periodic idle
   sweep is still stopped before the pool waits on its other executors — this story does not change
   that behaviour (verdict C1).

---

### User Story 3 - The two constructs the binary freeze forced are simplified away (Priority: P3)

Two pieces of the library exist purely because a 1.x binary-compatibility promise forbade the simpler
form. One is a whole serde implementation whose only job is to defer constructing a Confluent type so
that a published trait member could stay strict. The other is a single-method interface that exists
only because it is the declared return type of the documented `send(...)` methods, and which has
exactly one implementation.

After this story, both are expressed directly, and the classpath-isolation guarantee the first one
protected is still enforced — just against the simpler construct.

**Why this priority**: Pure simplification. It has no consumer-visible benefit beyond a smaller API,
and neither change would justify a major release on its own — but both are only possible in one, so
they ride along here.

**Independent Test**: Load the DSL entry points through a classloader that refuses the optional Avro
artifacts and confirm they still initialise, and that the Avro serde fails only when something
actually uses it.

**Acceptance Scenarios**:

1. **Given** a plain simulation with no optional Avro artifacts on the classpath, **When** it starts,
   **Then** it runs to completion — nothing on its path constructs an optional type.
2. **Given** the same classpath, **When** a scenario actually performs an Avro operation, **Then** it
   fails at that point with an error naming the missing artifact, exactly as it does today.
3. **Given** a simulation author calling `send(...)`, **When** they inspect the returned value,
   **Then** it is the concrete request builder, with no intermediate abstraction to navigate.
4. **Given** the classpath-isolation guard suite, **When** it runs, **Then** it still proves the
   isolation contract, re-pointed at the simplified construct rather than deleted along with it.

---

### User Story 4 - The test suite contains only tests that can fail (Priority: P4)

The test sources now outweigh the library. Some of that growth is tests that assert nothing a
sibling test does not already assert, tests whose guard condition cannot be reached, permutation
runs where three cases prove what fifty do, and example simulations that declare the same recipe
twice or inject the same scenario five times.

After this story, every remaining test earns its place, and one weak test is strengthened rather than
deleted.

**Why this priority**: It speeds up and clarifies the suite but changes nothing a consumer sees. It
must come after US1–US3 so the sweep runs against the final shape of the library rather than being
redone.

**Independent Test**: Run the full suite before and after; the pass/fail outcome is identical, the
removed cases are gone, and the deliberately weak assertion now fails against a deliberately
introduced regression.

**Acceptance Scenarios**:

1. **Given** a test that cannot fail against the very defect it cites as motivation, **When** the
   sweep runs, **Then** it is removed together with the helpers only it used.
2. **Given** a vacuous assertion that a pending request stays pending, **When** the sweep runs,
   **Then** it is strengthened to wait several poll cycles on a topic that cannot be assigned —
   not deleted.
3. **Given** two tests where one's assertions are a strict subset of the other's, **When** the sweep
   runs, **Then** only the stronger survives, and the audit record names it.
4. **Given** an example simulation, **When** a reader uses it as documentation, **Then** each recipe
   appears once, no scenario is injected in identical copies, and no code is reachable only from a
   comment.
5. **Given** a test that pins a real race, **When** the sweep runs, **Then** it is untouched, or at
   most merged with a near-duplicate whose every failure mode the survivor still detects.

---

### User Story 5 - The Kotlin examples are correct (Priority: P5)

The repository ships Kotlin example simulations for Kotlin users, and they stay where they are. What
is wrong with them is not their location: one of them has drifted into a state where it no longer
parses — it has an unbalanced call chain and names six types it never imports or defines — because
nothing in the build compiles Kotlin and so nothing signalled the breakage.

After this story a Kotlin user can copy any of these examples into their own project and have it
compile against the current API. The layout does not change and no Kotlin toolchain is added to the
build.

**Why this priority**: It affects what a Kotlin user gets from the repository, not what the library
does. It is last because it is fully independent of every removal above.

**Independent Test**: Compile each Kotlin example in a scratch Kotlin project that depends on this
plugin and on Gatling; all of them compile, including the one that currently does not parse.

**Acceptance Scenarios**:

1. **Given** a Kotlin user following any of the project's Kotlin examples, **When** they copy it into
   their own project with the documented dependencies, **Then** it compiles against the current API.
2. **Given** the example that currently does not parse, **When** this story completes, **Then** its
   call chain is balanced and every type it names is imported or defined.
3. **Given** the Kotlin examples after the removals in US1, **When** a reader checks them, **Then**
   none of them uses an entry point this release removed.
4. **Given** the repository layout, **When** this story completes, **Then** the Kotlin sources are
   still in the same place and the build has gained no Kotlin toolchain.

---

### Edge Cases

- **A consumer's simulation calls a removed method.** It stops compiling, with the compiler naming
  the missing method. That is the intended outcome of a major release; the migration guide must name
  the replacement for each removal, and for the topic-less `send(...)` family the "replacement" is
  the topic-first form that has always been the only working one.
- **A consumer relied on inheriting `kafka-streams-scala`.** Their build stops resolving that
  coordinate. The migration guide must tell them to declare it directly.
- **A consumer's simulation reads the response code off a message.** It stops compiling. Since that
  field was always empty, no data is lost — but the migration guide has to say so, because silence
  would read as "this used to carry data". Reported failure types on KO paths are unaffected, and the
  guide must say that too, so readers do not expect their reports to change.
- **A consumer built a check with the removed check type constant.** It stops compiling; the simple
  check type is the surviving equivalent, and it already materialized identically.
- **The trace log line describing a message.** It currently ends with an always-empty response-code
  field, and a logging test asserts that exact text. Removing the field changes both. The log line
  must keep describing everything it still has — topics, key, value, header count — and the test must
  be updated to the new text rather than relaxed to a substring match.
- **The optional Avro artifacts are absent at run time.** Unchanged: the failure still happens on
  first Avro use, naming the missing class — the simplification in US3 must not turn this into a
  failure at simulation start.
- **A future dependency bump re-introduces a vendor-only coordinate.** The published-POM contract
  still catches it; only the deprecation allowance is removed, not the check.
- **The sweep in US4 removes a test that turns out to be the only one covering a behaviour.** Each
  removal must name the surviving test that covers the same failure; a removal with no named survivor
  is not permitted.

## Requirements *(mandatory)*

### Functional Requirements

**Removal of unusable and unused published surface (US1)**

- **FR-001**: The plugin MUST NOT expose any way to build a send action without a producer topic.
  Every `send(...)` reachable from the Scala DSL and the Java facade MUST produce an action capable
  of sending. Once that holds, the request attributes MUST NOT model an absent producer topic as a
  representable state, and the runtime diagnostic for that state MUST be removed with it (verdict
  B2) — a type that can express what cannot happen is the residue this feature exists to clear.
- **FR-002**: The plugin MUST NOT expose the Kafka Streams windowing and topology helpers, and MUST
  NOT cause consumers to inherit a Kafka Streams library.
- **FR-003**: The plugin MUST NOT expose a message field or a check type for a response code, since
  no code path has ever populated either. Reported failure types on failing requests MUST be
  unaffected — they come from a different source and carry real values.
- **FR-004**: The plugin MUST NOT expose a producer-scoped reply timeout. The consume-scoped timeout
  controls, which examples and documentation do use, MUST remain.
- **FR-005**: Every dependency a consumer inherits MUST be justified by a component that uses it. The
  published-POM contract MUST NOT retain any allowance for a dependency justified only by a
  deprecation.
- **FR-006**: Each removal in FR-001 through FR-004 MUST appear in the project's migration guide,
  naming what to use instead, or stating plainly that nothing is lost where nothing is.

**Removal of unreachable internals, and a guard against their return (US2)**

- **FR-007**: The plugin MUST contain exactly one Avro body check path, reached identically from the
  Scala DSL and the Java facade. The unreachable preparer-based path MUST be removed.
- **FR-008**: The plugin MUST NOT retain the orphaned request-build failure message helper, the
  package-private byte-expression helper, the unused private type alias, or the unreachable
  exception-unwrapping branch identified in verdicts A6–A9.
- **FR-009**: The project MUST NOT retain unused imports in its own sources. All 22 identified in
  verdict A10 MUST be removed.
- **FR-010**: The build MUST fail on an unused import, an unused private member, an unused local, or
  an unused pattern variable, in both main and test sources. Unused method parameters and unused
  implicit parameters are deliberately **outside** the guard: this project's sources are full of
  interface-mandated overrides and implicit conversions that cannot drop a parameter, so including
  them would trade a self-enforcing guard for a suppression-annotation habit. The guard MUST be
  satisfiable with no warning suppressions anywhere in the sources.
- **FR-011**: Comments and documentation strings MUST NOT describe behaviour or name types that no
  longer exist. The instances identified in verdict A11 MUST be corrected or removed.
- **FR-012**: The build MUST NOT retain the empty schema-subject setting and its commented-out
  scaffolding identified in verdict A12.
- **FR-013**: The tracker pool's periodic idle sweep MUST continue to be stopped before the pool
  waits on its remaining executors during termination. It MUST NOT be removed as part of this
  feature (verdict C1).

**Simplification of freeze-forced constructs (US3)**

- **FR-014**: The DSL MUST continue to supply a generic Avro serde, and initialising any DSL entry
  point MUST NOT construct an optional Schema-Registry type. The dedicated deferring implementation
  that reconciled these two requirements under the binary freeze MUST be replaced by the direct form
  the freeze forbade.
- **FR-015**: The classpath-isolation guarantee MUST remain enforced by an automated check, re-aimed
  at the replacement construct. The check MUST still distinguish "fails on use" from "fails on
  first reference", and MUST retain the positive control that proves the check itself works.
- **FR-016**: The single-implementation request-builder abstraction MUST be folded into its
  implementation, so `send(...)` returns the concrete builder.

**Test-suite sweep (US4)**

- **FR-017**: Tests that cannot fail — whether because their guard is unreachable, or because their
  assertions are a strict subset of another test's — MUST be removed, along with helpers left with no
  reader.
- **FR-018**: Each removal under FR-017 MUST name the surviving test that detects the same failure.
  A removal with no named survivor MUST NOT be made.
- **FR-019**: The assertion that a reply request stays pending until assignment MUST be strengthened
  to observe several poll cycles against a topic that cannot be assigned, rather than removed.
- **FR-020**: Permutation-heavy runs MUST be reduced to the smallest set that still detects every
  failure the larger set detected.
- **FR-021**: Example simulations MUST NOT declare byte-identical duplicate configurations, MUST NOT
  inject identical copies of the same scenario, and MUST NOT carry code reachable only from a
  comment.
- **FR-022**: Tests pinning known concurrency races MUST NOT be removed by this feature. The design
  change that would make those races impossible by construction has not happened and is not part of
  this feature, so every test that pins one still has a defect to detect.

**Kotlin examples (US5)**

- **FR-023**: The Kotlin example sources MUST stay in their current location. No Kotlin compiler,
  plugin or alternative build MUST be added, and the files MUST NOT be relocated or deleted.
- **FR-024**: Every Kotlin example MUST parse, MUST reference only types it imports or defines, and
  MUST use only entry points that exist after this release's removals.

**Release integrity (cross-cutting)**

- **FR-025**: Every removal of a published symbol MUST be reflected in a commit message that marks it
  as breaking, so the release notes and the version number derived from the history are correct.
- **FR-026**: The full verification set — formatting gates, compilation, the unit and integration
  suites, the example smoke validation, the published-POM contract, and both continuous-integration
  Gatling simulations — MUST pass after each story, not only at the end.
- **FR-027**: Because this release does not wait for an automated binary-compatibility guard, the
  feature MUST produce a hand-authored record of the break surface: every published symbol removed or
  changed, compared against the previous release, reviewed before the release tag is pushed. The
  record MUST be checked in, not left in a pull-request comment, so the next release can diff against
  it. A removal absent from the record MUST NOT ship.

### Key Entities

- **Inherited dependency set**: what a consumer's build pulls in transitively from this plugin.
  Governed by a justification record that pairs every entry with the component that uses it. This
  feature shrinks the set and removes the record's deprecation allowance.
- **Published surface**: the Scala DSL, the Java facade, protocol settings and serialized formats
  that downstream simulations compile against. Removals here are irreversible once released.
- **DSL entry point**: any type whose initialisation happens on the path of an ordinary simulation.
  Initialising one must never require an optional artifact.
- **Test that earns its place**: a test that detects at least one realistic defect no other retained
  test detects. The unit of judgement for US4.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A simulation author can no longer reach any send entry point that fails for lack of a
  topic — the count of such reachable entry points goes from 68+ to zero.
- **SC-002**: A consumer resolving the plugin from a plain public-repository setup inherits four
  coordinates or fewer, every one of them justified by a component that uses it, and none of them a
  stream-processing library.
- **SC-003**: The unused-code guard reports zero findings across main and test sources with no
  suppressions present, and a deliberately re-introduced unused import fails the build.
- **SC-004**: A plain simulation with no optional Avro artifacts available runs to completion, and an
  Avro-using one fails only at the point of Avro use — both unchanged from today.
- **SC-005**: Test sources shrink by roughly 590 lines with no reduction in detected defects: the
  suite's pass/fail outcome is unchanged, and every removal names a surviving test.
- **SC-006**: The previously vacuous pending-request assertion fails when a deliberate regression is
  introduced, where before it passed regardless.
- **SC-007**: Every Kotlin example compiles when copied into a Kotlin project depending on this
  plugin — including the one that does not parse today — and they are still in the same directory,
  with no Kotlin toolchain added to the build.
- **SC-008**: Library sources shrink by roughly 500 lines while every documented capability — plain
  produce, request-reply, checks, Avro — still works end to end against a real broker.
- **SC-009**: The migration guide lets a reader upgrading from the previous major line find, for each
  compilation error the upgrade causes, the replacement or the statement that nothing is lost.
- **SC-010**: Each story's commit is independently green under the project's standard verification
  command.
- **SC-011**: A reviewer can list every published symbol this release removes or changes from a single
  checked-in record, and every entry in it is traceable to a verdict in this specification. Sampling
  any removal from the release and looking it up in the record succeeds.

## Assumptions

- **Release positioning — independent of the 1.x line.** This feature does **not** wait for the
  eleven open 1.x milestones. It is implemented and released on its own, which means no automated
  binary-compatibility guard is available to report the break surface; FR-027 replaces it with a
  hand-authored record. The consequences are carried through the requirements rather than assumed
  away: the reply-channel lifecycle redesign has not happened, so the tests it owns stay (FR-022),
  and the deprecation notice already served for the Kafka Streams helpers is the only notice any
  removal here has had.
- **Cross-milestone issue closure.** Two issues this work closes are recorded against earlier
  milestones (the topic-less `send(...)` deprecation and the response-code finding). They are closed
  by this work regardless, and the milestone linkage is handled deliberately rather than by the
  automatic guard.
- **Deprecation notice already served.** The Kafka Streams helpers were deprecated in an earlier
  minor release with the removal version named, so removing them here needs no further notice period.
- **Verdict C1 stands until argued otherwise.** The idle-sweep cancellation is treated as live code.
  A later change may still remove it, but not on the reasoning recorded in the issue.
- **Story independence.** US1 through US5 are separable and can ship as separate commits in the
  stated order. US4 is assumed to run last among the code stories so it sweeps the final shape of the
  library rather than an intermediate one.
- **Measured over estimated.** Where this specification states a count — 22 unused imports, zero
  unused locals — it is from a compiler run against the current sources, not from the issue text. The
  issue's own estimates ("8 files", "~20 in the examples") are superseded.
- **Example simulations are documentation.** Only vacuous validation, dead code and duplicated
  recipes are in scope for them; their value as illustration is preserved.
- **Kotlin examples stay uncompiled, deliberately.** They keep their current location and the build
  gains no Kotlin toolchain, so nothing automated will catch the next drift — that is an accepted
  tradeoff, not an oversight. Two consequences follow and are carried by requirements rather than by
  hope: the broken example is fixed by hand under FR-024, and because US1 removes published entry
  points, the Kotlin examples must be re-read against the surviving API in the same change. Checked
  during specification: all four currently use only `topic(...).send(...)` and
  `requestReply()...send(...)`, neither of which this release removes, so no example needs
  restructuring.
- **No new coverage.** This feature removes and simplifies. Every gap it exposes is recorded for the
  test-coverage milestone rather than filled here.
