# Feature Specification: Multi-Language Example Coverage in CI

**Feature Branch**: `007-multilang-example-ci-coverage`

**Created**: 2026-08-19

**Status**: Draft

**Input**: User description: "https://github.com/galax-io/gatling-kafka-plugin/issues/240 — CI runs Gatling simulations in one language of three, and ExampleSmokeValidation asserts less than it claims"

## Context

The project publishes example simulations in three languages and tells users all three are supported.
Continuous integration exercises one of them.

| Language | Example simulations published | Compiled by CI | Run against a broker by CI |
|---|---|---|---|
| Scala | 5 (`src/test/scala/…/examples`) | yes | **no** |
| Java | 4 (`src/test/java/…/javaapi/examples`) | yes | **no** |
| Kotlin | 4 (`src/test/kotlin/…/javaapi/examples`) | **no** | **no** |

CI does run three Gatling simulations, and all three are Scala — but none of them is a published
example. `KafkaGatlingTest`, `KafkaJavaapiMethodsGatlingTest`, and `KafkaConcurrencyLoadTest` are
test harnesses that happen to share the `examples` package with the documentation examples. So the
gap is wider than one language: **no example the project publishes is run by CI in any language.**
Scala at least has runtime coverage of the plugin through those harnesses; Java and Kotlin have none
at all.

One of the three is also *named* for the Java API (`KafkaJavaapiMethodsGatlingTest`) but is Scala
code calling the Java facade — it exercises the facade's runtime behaviour, not what a Java author
actually writes.

The gate that is supposed to cover this — `ExampleSmokeValidation` — is described in `README.md`,
`AGENTS.md`, and the project constitution (Principle I) as *constructing* every README and example
simulation against the current API. It does not construct anything. It loads each class and looks up
a no-argument constructor without invoking it, so the field initialisers that build the scenario and
the protocol never execute.

The cost of this gap is already on record: a Kotlin example built its protocol with a call removed in
1.0.0 and sat broken across four releases, because nothing compiled it. A Java example can go the
same way one step later — compiling cleanly and failing on every request, which is exactly the class
of defect the topic-less `send(...)` family turned out to be.

**Every Gatling run in this repository has reported green while two of the three documented languages
had no runtime coverage at all.** This feature closes that gap and makes the project's own statements
about its gate true.

## Clarifications

### Session 2026-08-19

- Q: Two of the four published Java examples cannot run as written (`ProducerSimulation` declares no injection profile; `AvroClassWithRequestReplySimulation` uses a placeholder registry URL and a non-Avro payload). How should FR-002 treat them? → A: Correct both examples in place so all four run.
- Q: `scripts/check-kotlin-examples.sh` (named by the issue) does not exist on this branch and Kotlin has no build wiring. How is Kotlin compile coverage delivered? → A: This feature owns a self-contained Kotlin compile check; no dependency on `006-v2-cleanup-sweep`.
- Q: What makes a covered example's run pass, given FR-006's "a run that sends nothing is a failure"? → A: Gatling assertions carried by each covered example — an expected request count and a 100% success rate.
- Q: Is the deliberate-break demonstration a one-off acceptance step or a permanent automated guard? → A: A one-off acceptance step per language, run by hand and recorded as evidence in the PR.
- Q: What CI wall-clock budget should this feature commit to? → A: None — SC-008 is removed. Coverage outranks speed here, and an invented ceiling would only obstruct.
- Q (2026-08-20, raised during implementation): a bespoke `exampleRun` sbt task was built to run the Java examples. Should it stay? → A: No. Use the most native mechanism each build system already provides, and introduce no new entity. Java and Kotlin are built and run from a neighbouring Maven or Gradle project — the one Gatling prescribes for them.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A Java author's example is proven to work, not just to compile (Priority: P1)

A load engineer writing simulations in Java copies a published example from this project into their
own suite and runs it against their broker. Today the only assurance behind that example is that it
compiled — nobody, human or machine, has ever run it. If the example sends to a topic the DSL no
longer routes correctly, or builds a protocol whose defaults changed, the engineer discovers it in
their own environment, mid-run, against their own system under test.

After this feature, every published Java example has been run against a real broker on the commit
that published it, and has produced the records it claims to produce. Where an example could not be
run as written, it was corrected until it could — a published example a user cannot run is broken
documentation regardless of CI.

**Why this priority**: This is the language gap with the highest user population and the lowest cost
to close — the broker is already running in CI and the sources already compile. It delivers value on
its own, with nothing else in this feature done.

**Independent Test**: Introduce a deliberate runtime-only defect into one Java example — one that
compiles cleanly, such as sending to a topic that does not exist — and confirm CI fails. Revert and
confirm CI passes.

**Acceptance Scenarios**:

1. **Given** the published Java example simulations, **When** CI runs on any commit, **Then** all
   four execute against the CI broker and their requests succeed.
2. **Given** a published Java example, **When** it executes in CI, **Then** the
   records it claims to produce are observably present on the broker — a run that sends nothing is a
   failure, not a pass.
3. **Given** a change to the plugin that breaks a Java example at run time but not at compile time,
   **When** CI runs, **Then** CI fails and names the example that broke.
4. **Given** a Java example that could not be executed as written, **When** it is corrected so that
   it can be, **Then** the DSL calls it demonstrates and their order are unchanged — the correction
   makes the example runnable without changing what it teaches.
5. **Given** an example that genuinely cannot be corrected within this feature, **When** CI runs,
   **Then** its exclusion is explicit and carries a recorded reason — it is never silently skipped.

---

### User Story 2 - The compatibility gate asserts what the project says it asserts (Priority: P1)

A maintainer reviewing a pull request reads in `AGENTS.md`, `README.md`, and the constitution that
`ExampleSmokeValidation` checks every example still *constructs* against the current API, and treats
a green run as evidence that no example broke. That evidence does not exist. The gate proves only
that each class loads and declares a no-argument constructor.

After this feature, either the gate really does construct every example — so the scenario and the
protocol are genuinely built and a DSL break is caught — or every statement describing it says
precisely what it checks. Both, ideally: strengthen the gate and then describe it accurately.

**Why this priority**: This is a correctness defect in the project's own documentation and governing
constitution. A maintainer relying on a false statement is worse than one who knows there is no gate,
because the false statement stops them looking for real coverage. It is also independent of User
Story 1 and cheap.

**Independent Test**: Break the DSL used in one example's protocol or scenario construction — a
change that still compiles and still leaves a no-argument constructor in place — and confirm the gate
fails. Then read every document that mentions the gate and confirm each statement is verifiable
against what the gate does.

**Acceptance Scenarios**:

1. **Given** an example whose scenario or protocol construction throws, **When** the compatibility
   gate runs, **Then** the gate fails and names that example.
2. **Given** the compatibility gate as it will exist after this feature, **When** a reader compares
   `README.md`, `AGENTS.md`, and the constitution against it, **Then** no statement about the gate
   overstates what it verifies.
3. **Given** a newly added example simulation, **When** the compatibility gate runs, **Then** the new
   example is covered by it — an example the gate does not know about is a failure, not a silent
   omission.

---

### User Story 3 - Kotlin examples cannot rot unnoticed (Priority: P2)

A Kotlin user follows the README's Kotlin section and the linked Kotlin examples. Those examples are
not compiled by anything in this repository's build, and one of them was broken across four releases
before anyone noticed. The README links them as if they were maintained.

After this feature, a Kotlin example that stops compiling against the current API fails a CI job. Not
a run against a broker — a compile check is the honest, affordable level of coverage for a language
that is deliberately not part of the build.

**Why this priority**: The defect that motivated this issue was a Kotlin one, so the gap is proven
real. It is P2 rather than P1 because Kotlin is deliberately outside the build toolchain, so closing
it costs more per unit of coverage than Java does — not because it is optional. This feature owns the
check outright rather than waiting on another branch, so US3 ships with the rest.

**Independent Test**: Introduce a syntax or API error into one Kotlin example and confirm the job
covering Kotlin fails; revert and confirm it passes.

**Acceptance Scenarios**:

1. **Given** the published Kotlin example simulations, **When** CI runs, **Then** every one of them
   is compiled against the current plugin API.
2. **Given** a Kotlin example that no longer compiles against the current API, **When** CI runs,
   **Then** CI fails and names the example.
3. **Given** a Kotlin example added or renamed, **When** CI runs, **Then** it is picked up by the
   compile check without anyone editing a hard-coded list.

---

### Edge Cases

- **An example is documentation, not a runnable simulation.** At least one published Java example
  declares scenarios but never sets up an injection profile, so a load-test runner has nothing to
  execute. Coverage must classify it deliberately rather than fail confusingly.
- **An example needs infrastructure CI does not provide.** The Avro request-reply example depends on
  a schema registry and on a payload type that must be a real Avro record. Running it requires more
  than adding a name to a list.
- **An example needs a topic the CI broker never creates.** CI builds its broker from its own service
  definition, not from the local Compose file, so topics present locally are absent in CI. Every
  topic a newly covered example uses must be added to the CI broker's topic list.
- **Two covered examples share a topic.** Examples that use the same request/reply topic must not
  cross-contaminate each other's replies when run in the same CI job.
- **An example matches replies in a way that only works for one user at a time.** A covered example
  must inject a load profile its matching strategy can actually satisfy, or its profile must be
  bounded to what it supports — and its assertions must be written to that bound, not above it.
- **A new example file is added and nobody wires it up.** The mechanism that decides what is covered
  must notice an unlisted example rather than quietly ignore it.
- **CI wall-clock grows.** A covered example that waits on replies contributes its full timeout to
  the worst case. There is no committed budget (see Success Criteria), so the control is the
  injection profiles: each covered example runs at the smallest volume its assertions require, and
  a covered example whose profile is larger than its assertions need is over-specified.
- **Instantiating an example has side effects.** Strengthening the compatibility gate to construct
  each example executes its field initialisers, which may build clients pointed at hosts that do not
  exist in the gate's environment. Construction must not require a live broker or registry.

## Requirements *(mandatory)*

### Functional Requirements

#### Coverage

- **FR-001**: Every example simulation the project publishes as documentation for a supported
  language MUST be covered by CI at the strongest level that language's presence in the build
  supports: executed against a broker where the language is part of the build, compiled where it is
  not.
- **FR-002**: All thirteen published example simulations MUST be executed against a real broker on
  every CI run. Each language uses the Gatling task its own build system provides — no bespoke
  mechanism may be introduced for this, and none may stand between an example and the runner its
  users would invoke.
- **FR-002a**: An example that cannot be executed as written MUST be corrected so that it can be, and
  the correction MUST be treated as a documentation defect fix in its own right — a published example
  a user cannot run is broken documentation, whether or not CI ran it. Specifically: an example that
  declares a scenario but no injection profile MUST gain one, and an example whose payload type or
  service endpoint is a placeholder MUST be given a real one that works against the CI stack.
- **FR-002b**: A correction made under FR-002a MUST NOT change what the example teaches. The DSL calls
  the example demonstrates, and the order in which it demonstrates them, MUST survive the correction.
- **FR-003**: Kotlin example simulations MUST be compiled against the current plugin API on every CI
  run, and — superseding the original ceiling of compile-only — MUST also be executed against a real
  broker. The Maven consumer project compiles and runs them in one step, so compile-only would now be
  a deliberate reduction rather than a saving.
- **FR-003a**: The Kotlin compile check MUST be self-contained within this feature. It MUST NOT
  depend on any unmerged branch, and CI MUST NOT be left in a state where the Kotlin job passes
  because the thing that performs the check is absent.
- **FR-003b**: The Kotlin compile check MUST compile the examples against the same plugin build the
  rest of CI tests, not against a published release, so an API change in the same commit is caught.
- **FR-004**: If any example still cannot be executed after the corrections required by FR-002a, it
  MUST be recorded as compile-only together with the reason it cannot run. Silent omission MUST NOT
  be possible.
- **FR-005**: The set of covered examples MUST be derived from the example sources themselves, so
  that adding, renaming, or moving an example without covering it fails CI rather than passing
  unnoticed.

#### Evidence

- **FR-006**: A covered example MUST carry assertions that fail its run when it does not do what it
  claims: an expected request count, so a run that sends nothing fails, and a 100% success rate, so a
  run whose requests are rejected or whose replies never arrive fails. Absence of an error MUST NOT
  by itself constitute a pass.
- **FR-006a**: Adding these assertions to a published example is permitted under FR-002b and does not
  count as changing what the example teaches — the DSL calls demonstrated and their order are
  unaffected, and a simulation that states its own success criteria is the better example.
- **FR-006b**: An assertion MUST be written over what the example itself guarantees, not over a
  volume it happens to produce today. A covered example's injection profile and its assertions MUST
  stay consistent with each other, so that changing one without the other fails rather than passes.
- **FR-007**: A deliberate defect in any covered example MUST fail CI, for each of the three
  languages, at the coverage level that language has.
- **FR-007a**: FR-007 MUST be demonstrated by hand once per language as an acceptance step, not
  automated into a standing CI check. The demonstration MUST be recorded as evidence naming the
  example that was broken, what was broken in it, and which CI job went red. Evidence that does not
  name all three is not acceptance.
- **FR-007b**: The defect chosen for each language MUST be one that its coverage level is actually
  claimed to catch — a run-time-only defect where the language is executed, a compile-time defect
  where it is only compiled. A demonstration that would have failed anyway proves nothing.
- **FR-008**: CI MUST provision every topic used by every covered example, in the CI broker
  definition, kept consistent with the local development broker definition.

#### The compatibility gate

- **FR-009**: The example compatibility gate MUST actually construct each example simulation, so that
  the scenario and protocol built in its field initialisers are executed.
- **FR-010**: The compatibility gate MUST fail, naming the offending example, when an example's
  scenario or protocol construction fails.
- **FR-011**: The compatibility gate MUST NOT require a running broker, schema registry, or any other
  external service. It runs before infrastructure-dependent steps and must stay usable on a laptop
  with nothing running.
- **FR-012**: Every statement in `README.md`, `AGENTS.md`, and the project constitution describing
  what the compatibility gate verifies MUST be true of the gate as it exists after this feature. No
  statement may claim coverage the gate does not provide.
- **FR-013**: Where compilation — not the compatibility gate — is what actually protects an example
  from an API break, the documentation MUST say so.

#### Non-regression

- **FR-014**: Existing Scala simulation coverage MUST NOT be reduced, reordered into a weaker form,
  or made conditional by this feature.
- **FR-015**: This feature MUST NOT change any published Scala or Java API signature, protocol
  default, or serialized format. Changes to example sources are permitted only under FR-002a, are
  bounded by FR-002b, and MUST be identified as documentation fixes.

### Key Entities

- **Example Simulation**: A simulation published as user-facing documentation, in Scala, Java, or
  Kotlin. Attributes: language, source location, whether it is runnable, the topics it uses, the
  external services it needs.
- **Coverage Level**: What CI does with an example — *executed against a broker*, *compiled*, or
  *compile-only with a recorded reason*. Every Example Simulation has exactly one.
- **Compatibility Gate**: The check that every example still builds its scenario and protocol against
  the current API, without external infrastructure.
- **CI Broker Topic Inventory**: The set of topics the CI broker creates. Must be a superset of the
  topics every covered example uses, and must stay in step with the local development broker.
- **Deliberate-Break Drill**: The one-off manual verification, per language, that a defect in an
  example actually fails CI. Attributes: the language, the example broken, the defect introduced, the
  CI job that went red. Its recorded result is the evidence that the coverage claimed here is real;
  it is an acceptance artifact, not a standing CI check.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All three documented languages have runtime CI coverage, up from none. Java and Kotlin
  move from zero coverage to running against a real broker; the Scala examples, which were also never
  run, move with them.
- **SC-002**: 9 of 9 published JVM example simulations (5 Scala, 4 Java) are executed in CI, up from
  0. Any example that could not be corrected is listed as compile-only with a recorded reason; zero
  are unaccounted for.
- **SC-003**: 4 of 4 published Kotlin example simulations are compiled **and run** by CI, up from 0
  of either. Zero are unaccounted for, and the check depends on nothing outside this feature's own
  branch.
- **SC-004**: The deliberate-break drill fails CI in 3 of 3 languages. Today it would fail in 1 of 3.
  Recorded once at acceptance, with the broken example, the defect, and the red job named for each.
- **SC-005**: A break in an example's scenario or protocol construction is detected by the
  compatibility gate in 100% of cases. Today it is detected in 0%.
- **SC-005a**: A covered example that sends nothing, or whose requests all fail, fails CI. Verified
  by suppressing the sends in one covered example and confirming the run goes red.
- **SC-006**: Zero statements about the compatibility gate in `README.md`, `AGENTS.md`, or the
  constitution overstate what it verifies, confirmed by reading each against the gate.
- **SC-007**: Adding a new example simulation without covering it fails CI, demonstrated once.
- **SC-008**: The existing Scala simulation coverage runs unchanged — same simulations, same
  assertions, same pass criteria.

No wall-clock budget is set for this feature. Coverage where there is none outranks job duration
here, and a ceiling invented without a measurement to justify it would obstruct the deliverable
rather than protect anything. The cost is bounded instead by the injection profiles themselves,
which stay at the smallest volume the assertions in FR-006 need.

## Assumptions

Facts established by inspecting the repository at `007-multilang-example-ci-coverage`, and the
defaults chosen where the issue did not specify:

### About the Java examples

- **Two of the four Java examples cannot be run as written, and are corrected by this feature.**
  `ProducerSimulation` declares a scenario but never sets up an injection profile, so there is nothing
  for a runner to execute. `AvroClassWithRequestReplySimulation` constructs its registry client
  against a placeholder URL and carries a payload type that is an empty class rather than an Avro
  record, so it cannot serialize. The issue's estimate that the Java examples "already compile and the
  broker is already up, add them to the list" holds for two of the four; the other two are fixed under
  FR-002a. Both are genuine documentation defects — a user copying either one today gets something
  that does not run.
- **Two of the four look runnable but need care.** `BasicSimulation` uses the same topic for request
  and reply, so it reads back its own message; `MatchSimulation` matches every reply to every request
  by a constant, which is only sound at one user in flight. Both are acceptable as covered examples
  provided their injection profiles stay within what they support.
- **The CI broker creates none of the topics the Java examples use.** CI's topic list carries
  `myTopic1..6`, `test.t1/2/3/5/6`, `load.request`, and `load.reply`; the Java examples use `test.t`,
  `test.topic`, `request.t`, and `reply.t`. The local Compose stack creates `test.t` but not the
  others, and the two broker definitions are maintained separately by design.

### About the compatibility gate

- **The gate covers nine example classes today** — five Scala and four Java — by fully-qualified name
  in a hard-coded list. No Kotlin example appears, and nothing detects an example missing from the
  list.
- **Strengthening the gate to construct each example is feasible without infrastructure.** Building a
  protocol and a scenario configures clients; it does not connect to a broker or a registry. The
  assumption is that construction of all nine stays offline; any example that turns out to require a
  live service during construction is handled under FR-004 as compile-only with a recorded reason.
- **Both remedies the issue offers are adopted, not one.** The gate is strengthened to construct, and
  the documentation is corrected to describe what it verifies. Strengthening alone would leave the
  constitution's Principle I true by luck rather than by statement; correcting alone would leave the
  gap open.
- **The constitution is in scope for the documentation correction.** Principle I states the gate
  "MUST keep constructing every README and example simulation". Correcting `README.md` and
  `AGENTS.md` while leaving the constitution asserting something untrue would not satisfy FR-012.

### About Kotlin

- **Kotlin is not part of the build.** There is no Kotlin plugin, source configuration, or dependency
  in `build.sbt`, `project/plugins.sbt`, or `project/Dependencies.scala`. The four Kotlin examples are
  compiled by nothing.
- **The helper script the issue names does not exist on this branch.** `scripts/check-kotlin-examples.sh`
  is referenced by the issue as belonging to the `006-v2-cleanup-sweep` work, which is on a separate
  branch and not merged. This feature therefore provides its own compile check (FR-003a) rather than
  waiting on it. If `006` lands an equivalent first, the two are reconciled at merge — a cheap
  merge-time concern next to a feature blocked on another branch.
- **Compile coverage was the original target for Kotlin; it was exceeded.** The premise was that
  running Kotlin simulations needs a Kotlin toolchain inside the sbt build. It does not: they run
  from a Maven consumer project, where `kotlin-maven-plugin` compiles them and Maven's Gatling plugin
  runs them. There is no toolchain to install and nothing was traded away for it.

### About scope and process

- The three Scala simulations CI runs today stay exactly as they are; this feature adds coverage and
  does not rebalance existing coverage.
- No plugin source under `src/main` changes. Changes land in CI configuration, the broker topic
  inventory, the compatibility gate, the documentation, and the example sources where an example is
  itself defective (FR-002a).
- This feature belongs to milestone **v1.13.0 Test suite integrity**, which owns issue #240.

## Out of Scope

- Introducing a Kotlin toolchain into the *sbt* build. Kotlin does now run against a broker, but from
  the Maven consumer project, which needs nothing added to sbt.
- Adding new example simulations for their own sake. Coverage is the deliverable; the examples that
  exist are the population. Correcting an existing example so it can run is in scope (FR-002a);
  rewriting one to teach something different is not (FR-002b).
- Rewriting the Scala simulations CI already runs, or changing their assertions.
- Any change to the plugin's published Scala or Java API.
- The wider `006-v2-cleanup-sweep` work. This feature neither depends on it nor pre-empts it.

## Dependencies

- The CI broker and schema registry services already defined in the CI workflow. No new
  infrastructure is introduced.
- No dependency on `006-v2-cleanup-sweep`. The Kotlin compile check is owned by this feature
  (FR-003a); reconciliation with any equivalent `006` lands is handled at merge.
