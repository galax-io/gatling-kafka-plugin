# Feature Specification: Classpath and Dependency Shedding

**Feature Branch**: `005-classpath-dependency-shedding`

**Created**: 2026-08-09

**Status**: Draft

**Input**: User description: "https://github.com/galax-io/gatling-kafka-plugin/milestone/10 — v1.3.0 Classpath and dependency shedding. S1 — a consumer following the README cannot resolve the plugin at all: compile-scope Confluent artifacts are not on Maven Central and the published POM strips the resolver. Also sheds the Kafka Streams dependency held by two unused implicits."

**Milestone**: [v1.3.0 Classpath and dependency shedding](https://github.com/galax-io/gatling-kafka-plugin/milestone/10)

**Tracked issues**: [#185](https://github.com/galax-io/gatling-kafka-plugin/issues/185) (bug, priority p1), [#214](https://github.com/galax-io/gatling-kafka-plugin/issues/214)

---

## Problem

The published plugin artifact declares dependencies that a consumer cannot obtain. The result is that
the installation path documented in the README does not work for a consumer whose build resolves from
the default public artifact repository only — which is the default configuration of every supported
build tool.

### Verified evidence (2026-08-09)

**What is published today.** The POM for `org.galaxio:gatling-kafka-plugin_2.13:1.2.0` declares **four**
dependencies in a scope consumers inherit, none of which exist on Maven Central, and declares **no**
repository from which to fetch them:

| Declared in published `1.2.0` | Maven Central | Confluent repository |
| --- | --- | --- |
| `org.apache.kafka:kafka-clients:7.9.5-ce` | absent (404) | present (200) |
| `org.apache.kafka:kafka-streams-scala_2.13:7.9.5-ce` | absent (404) | present (200) |
| `io.confluent:kafka-streams-avro-serde:7.9.8` | absent (404) | present (200) |
| `io.confluent:kafka-avro-serializer:7.9.8` | absent (404) | present (200) |

**What the next release would publish.** `main` has since moved to newer versions of the same four
coordinates, and every one of them is equally absent from Maven Central:

| Declared on `main` | Maven Central | Confluent repository |
| --- | --- | --- |
| `org.apache.kafka:kafka-clients:7.9.9-ce` | absent (404) | present (200) |
| `org.apache.kafka:kafka-streams-scala_2.13:7.9.9-ce` | absent (404) | present (200) |
| `io.confluent:kafka-streams-avro-serde:7.9.9` | absent (404) | present (200) |
| `io.confluent:kafka-avro-serializer:7.9.9` | absent (404) | present (200) |

The repository list is stripped from the published POM by design (correct Sonatype hygiene for the
publisher, but it removes the only signal a consumer could have followed). Issue #185 identified the two
`io.confluent` artifacts; verification for this specification found that the two `org.apache.kafka`
artifacts are affected identically, because the build pins Confluent's `-ce` rebuild rather than the
Apache release. The problem is a property of the vendor coordinate, not of any particular version: the
`-ccs` variants are absent from Maven Central too, at every version checked. Equivalent Apache-released
coordinates (`org.apache.kafka:kafka-clients`, `org.apache.kafka:kafka-streams-scala_2.13`) are present.

**Version automation cannot fix this and has been masking it.** Routine dependency updates keep moving
these four coordinates forward inside the vendor line — `7.9.5-ce` → `7.9.9-ce`, `7.9.8` → `7.9.9` —
which keeps them current and keeps them unresolvable. Nothing in the build asserts that an inherited
dependency is fetchable from the default public repository, so each bump lands green. The same
automation also maintains a dependency declaration the build never applies (`avro-compiler`, most
recently bumped to 1.12.1), which is the FR-015 cleanup. This is why FR-001 needs an assertion attached
to it rather than a one-time correction.

The optional-dependency precedent already exists and is inconsistently applied: `avro4s-core` is
correctly declared as a non-inherited dependency and documented as something consumers add themselves,
while the Confluent artifacts serving the same optional Avro capability are inherited.

### Why the Avro artifacts cannot simply be made optional

Every simulation reaches the plugin through a single DSL entry point that transitively mixes in the
serde surface. That surface eagerly constructs an Avro serde at initialisation and exposes
Schema-Registry types in published Java signatures. Four sites in `src/main` carry the coupling:

- `request/KafkaSerdesImplicits.scala` — an eagerly initialised Avro serde value, a Schema Registry
  backed serde factory, and the two unused Kafka Streams implicits, all on the trait mixed into the
  default Scala DSL import.
- `javaapi/checks/KafkaChecks.scala` — a second eagerly initialised Avro serde value.
- `javaapi/KafkaDsl.java` — public entry points whose parameter and return types are Schema Registry
  and Avro types.
- `javaapi/expressions/Builders.java` — a public constructor taking a Schema Registry client.

Consequently a consumer without the Avro artifacts today fails when the DSL is loaded, not only when an
Avro feature is used. Making the artifacts optional without addressing this would convert a
resolution-time failure into a load-time failure — no better for the consumer.

### Unused Kafka Streams surface

Two implicits (`sessionWindowedSerde`, `consumedFromSerde`) have had no caller in sources, tests, or
documentation since the initial commit. They belong to Kafka Streams — a different product with no role
in a load test — and they are the only reason the Kafka Streams artifact is an inherited dependency.
They also leak Streams types into the implicit scope of every simulation that imports the DSL. A
hand-maintained exclusion on the Avro serde artifact exists solely to manage the resulting collision.
Separately, one build dependency declaration (`avro-compiler`) is defined but never added to the build,
and two Kafka Streams imports in an example simulation are unused.

---

## Clarifications

### Session 2026-08-09

- **Q: The mandatory Kafka client is also Confluent-only — issue #185 did not cover it. Relocate it, or
  push it onto consumers to declare?**
  → **A: Relocate.** Move to the Apache-released coordinates present on Maven Central and keep the
  client inherited, so a consumer still gets a working plugin by declaring the plugin alone. Accepts a
  visible change to the client version line, which the documented compatibility statement must track.
  Rejected: making it non-inherited like the Gatling core artifacts, which would force every consumer —
  including plain-serialization users — to edit their build in order to upgrade. Recorded as FR-018.

- **Q: Milestone 10 calls for shedding the Kafka Streams artifact in v1.3.0, but Constitution Principle
  I requires the two published implicits holding it to keep compiling for at least one more minor, and
  they cannot compile without it. Which wins?**
  → **A: The constitution wins.** Deprecate both implicits in this release naming their removal
  release; keep the artifact inherited and make it Central-resolvable under FR-018's relocation; shed
  the artifact when the implicits are removed. Rejected: shedding now, which risks breaking compilation
  for *every* consumer — the implicits sit on the trait mixed into the default DSL entry point, so
  implicit search reads their signatures whether or not a consumer uses them. Also rejected: removing
  them immediately as a major release, which promotes a packaging fix into a major version bump.
  Recorded as FR-019. The milestone's "shedding" in this release is therefore the deprecation and the
  dead-declaration cleanup, not the artifact removal.

- **Q: Making the Confluent Avro artifacts optional converts a resolution failure into a load failure
  unless the Avro surface is separated from the default DSL entry point. Is a source change acceptable
  for Avro consumers?**
  → **A: Yes, one import.** Avro and Schema Registry entry points move behind a dedicated opt-in entry
  point, with the current locations deprecated in place for at least this release and the one-line
  change stated in the migration guide. Rejected: keeping everything reachable from the current import
  by making the Confluent references inert, which is hard to prove and fails as a runtime
  `NoClassDefFoundError` mid-load-test if one eager reference is missed — and may not be achievable at
  all for the Java entry points that name Schema Registry types in their signatures. Recorded as
  FR-020.

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A new consumer can install and run the plugin (Priority: P1)

A performance engineer adds the plugin to a project that resolves artifacts from the default public
repository only, copies the minimal produce example from the README, and runs it against a broker. They
do not use Avro, do not run a Schema Registry, and have not been told about any additional repository.

**Why this priority**: This is the S1 defect. Until it is fixed, the documented installation path
produces a hard build failure for every new consumer, and no other capability of the plugin is
reachable. Fixing this alone restores the product.

**Independent Test**: From a scratch project on each supported build tool, configured with the default
public repository and nothing else, declare only the plugin, then compile and execute a plain
produce simulation and a plain request-reply simulation against a broker. No manual repository
configuration and no additional dependency declarations are permitted for this scenario to pass.

**Acceptance Scenarios**:

1. **Given** a scratch sbt project whose only declared dependency is the plugin and whose only
   configured repository is the default public one, **When** the build resolves dependencies,
   **Then** resolution succeeds with no unresolved-dependency error.
2. **Given** the same project, **When** a plain-serialization produce simulation is compiled,
   **Then** compilation succeeds with no missing-class or missing-dependency error arising from the
   plugin's own classpath.
3. **Given** the same project and a running broker, **When** the produce simulation is executed,
   **Then** it completes and reports successful requests, with no class-loading failure attributable
   to an absent Avro or Schema Registry artifact.
4. **Given** the same project, **When** a plain-serialization request-reply simulation is compiled and
   executed against a broker, **Then** replies are correlated and checks report successfully.
5. **Given** equivalent scratch projects for the Gradle and Maven installation instructions, **When**
   each resolves and compiles the equivalent minimal simulation, **Then** both succeed under the same
   constraints.

---

### User Story 2 - An Avro / Schema Registry consumer has a documented, working path (Priority: P2)

A performance engineer needs Avro payloads validated against a Schema Registry. They read the
installation documentation, add the artifacts and the repository it names, and run their existing Avro
simulation.

**Why this priority**: Avro support is a headline capability of the plugin and the reason the Confluent
artifacts are present at all. Making them optional is only acceptable if the opt-in path is documented
precisely enough to follow once, without trial and error. This story is what keeps the P1 fix from
being a capability regression.

**Independent Test**: From a scratch project configured with the default public repository plus exactly
the repository named in the documentation, declare the plugin plus exactly the optional artifacts named
in the documentation, then compile and execute an Avro produce simulation and an Avro-body check
against a broker and Schema Registry. Passing requires that no coordinate or repository outside the
documentation was needed.

**Acceptance Scenarios**:

1. **Given** the installation documentation, **When** a consumer looks for Avro setup instructions,
   **Then** they find the exact coordinates and the exact repository URL required, expressed for each
   of the three documented build tools.
2. **Given** a project following those instructions, **When** an Avro produce simulation using
   automatic schema derivation is compiled and executed, **Then** it succeeds.
3. **Given** a project following those instructions, **When** an Avro body check against a
   Schema-Registry-backed record is compiled and executed, **Then** the check materialises and
   evaluates successfully.
4. **Given** a project following those instructions on the Java facade, **When** the Avro entry points
   are used from Java or Kotlin, **Then** they compile and execute successfully.
5. **Given** a consumer who does **not** follow the Avro instructions, **When** they use only plain
   serialization, **Then** nothing in their build or run requires an Avro artifact.

---

### User Story 3 - An upgrading consumer is told exactly what changed (Priority: P2)

A team on a 1.2.x release upgrades to this release. Their build previously inherited artifacts it will
now have to declare, and their code may touch entry points that are being retired.

**Why this priority**: The change deliberately removes artifacts from what consumers inherit. Without a
migration entry, an upgrade turns a working suite into an unresolved-dependency or missing-class
failure, and the fix is not discoverable from the error. This is equal in priority to Story 2 because a
silent break for existing users is as damaging as an undocumented path for new ones.

**Independent Test**: Take an existing simulation suite written against the previous release, upgrade
only the plugin version, and apply exactly the steps listed in the migration guide. The suite must
compile and run without any further change.

**Acceptance Scenarios**:

1. **Given** a suite using only plain serialization on the previous release, **When** only the plugin
   version is upgraded, **Then** it compiles and runs with no change to build files or sources.
2. **Given** a suite using Avro or Schema Registry on the previous release, **When** the plugin version
   is upgraded and only the migration guide's listed steps are applied, **Then** it compiles and runs.
3. **Given** the migration guide, **When** an upgrading consumer reads it, **Then** it states which
   artifacts are no longer inherited, what to add, and which repository to configure.
4. **Given** a suite that references a retired entry point, **When** it is compiled against this
   release, **Then** it still compiles and the build reports a deprecation naming both the replacement
   or rationale and the release in which removal will occur.

---

### User Story 4 - Every inherited dependency is justified, and dead weight is marked for removal (Priority: P3)

A consumer inspects the plugin's dependency tree, or an auditor reviews what a load test drags onto its
classpath. Everything inherited is either something the plugin executes, or something explicitly
recorded as retained only until a named release.

**Why this priority**: This is hygiene rather than a defect — nothing is broken for a consumer today by
the unused surface alone. It is bundled here because the same artifacts are implicated in the P1 fix,
and resolving both at once avoids touching the dependency set twice in two releases.

**Independent Test**: Enumerate the plugin's inherited dependencies and, for each, name either a code
path in the plugin that requires it or the deprecation it is retained for. Any dependency with neither
fails this story. Independently, confirm no source or build file references a declaration that the
build never applies.

**Acceptance Scenarios**:

1. **Given** the plugin's published dependency set, **When** each inherited artifact is traced,
   **Then** every artifact resolves either to a plugin code path that uses it or to a recorded
   deprecation that names the release in which the artifact goes away.
2. **Given** the two unused Kafka Streams implicits, **When** the sources are inspected, **Then** each
   is marked deprecated with a stated removal release, states that the plugin itself does not use it,
   and no plugin code path calls either.
3. **Given** the build definition, **When** it is inspected for dependency declarations, **Then** no
   declaration exists that the build never applies.
4. **Given** the example simulations, **When** they are compiled under the project's warnings-as-errors
   settings, **Then** no unused import of a Kafka Streams type remains.
5. **Given** any exclusion rule that exists only to resolve a collision between artifacts this release
   relocates or stops inheriting, **When** the relocation is complete, **Then** either the exclusion is
   removed or the collision it still prevents is stated in the build definition.

---

### Edge Cases

- **A consumer who already configured the Confluent repository and relied on inherited Avro artifacts.**
  Their build resolves today; after this change the artifacts are no longer inherited and their build
  breaks at resolution or compilation. This case must be covered by the migration guide, not discovered
  at runtime.
- **A consumer using the default Scala DSL import but no Avro.** The DSL entry point currently
  initialises an Avro serde eagerly, so absence of the Avro artifact is a load-time failure rather than
  a use-time one. Plain usage must not require the Avro artifact to be present at compile time or at
  run time.
- **A consumer using the Java or Kotlin facade.** Published Java entry points expose Schema Registry
  types in their signatures. Whatever separation is chosen must keep the Java facade usable for plain
  serialization without Schema Registry artifacts present, and must not silently drop published Java
  entry points.
- **Build tools that do not propagate non-inherited scopes into the configuration the tests run in.**
  The Gradle Gatling configuration and Maven test scope treat optional scopes differently from sbt.
  Instructions must be verified per build tool rather than translated by analogy.
- **A consumer compiling with warnings treated as errors.** A newly added deprecation must not turn an
  otherwise-clean consumer build red without warning; the migration guide must say the deprecation is
  coming and name its removal release.
- **A change to the Kafka client artifact's version line.** If the mandatory Kafka client dependency
  moves from the Confluent rebuild to the Apache release, the version string consumers see changes even
  though the code is equivalent. Any broker-compatibility expectation stated in documentation must
  match what is actually shipped.
- **Transitive arrival of a shed artifact.** A consumer may still receive an artifact transitively via
  another dependency. Verification must assert on what the plugin *declares*, not merely on whether a
  particular test project happens to resolve.
- **A consumer of the retired Kafka Streams implicits.** They have no in-plugin replacement, because
  the capability belongs to a different product. The deprecation must say so rather than name a
  replacement that does not exist.

---

## Requirements *(mandatory)*

### Functional Requirements

#### Resolvability

- **FR-001**: Every dependency the published artifact declares in a scope that consumers inherit MUST be
  resolvable from the default public artifact repository alone, with no additional repository
  configured by the consumer.
- **FR-002**: The published artifact MUST NOT depend on a consumer configuring a repository that the
  published metadata does not, and cannot, carry.
- **FR-003**: Resolvability MUST be verified from a scratch consumer project for each documented
  installation path — sbt, Gradle, and Maven — rather than inferred from the build definition.

#### Plain-serialization usability

- **FR-004**: A consumer using plain serialization MUST be able to compile and execute produce and
  request-reply simulations with no Avro artifact, no Schema Registry artifact, and no Schema Registry
  service present.
- **FR-005**: Loading or importing the plugin's default DSL entry point MUST NOT require an Avro or
  Schema Registry artifact to be present, at compile time or at run time.
- **FR-006**: The Java and Kotlin facade MUST satisfy FR-004 and FR-005 on the same terms as the Scala
  DSL.

#### Optional Avro and Schema Registry support

- **FR-007**: Avro and Schema Registry support MUST be declared in a scope consumers do not inherit,
  consistent with the existing treatment of the Avro case-class support library.
- **FR-008**: Installation documentation MUST state, for each of the three documented build tools, the
  exact optional coordinates and the exact repository required to enable Avro and Schema Registry
  support.
- **FR-009**: With those artifacts and that repository added, every Avro and Schema Registry capability
  available before this change MUST remain available and behave identically.

#### Compatibility and migration

- **FR-010**: Published Scala DSL and Java facade entry points MUST continue to compile. Any entry
  point that must move or change MUST be handled under the project's deprecate-before-remove rule, with
  the replacement named.
- **FR-011**: A migration guide entry MUST state which artifacts are no longer inherited, what a
  consumer must add to restore each capability, and which repository to configure.
- **FR-012**: The example-simulation smoke validation MUST keep constructing every documented and
  example simulation against the resulting API.
- **FR-013**: If the shipped Kafka client version line changes, any documented compatibility statement
  MUST be updated in the same change to match what is shipped.

#### Shedding

- **FR-014**: The two unused Kafka Streams implicits MUST be marked deprecated, each naming the release
  in which it will be removed and stating that the plugin itself does not use it.
- **FR-015**: Build dependency declarations that the build never applies MUST be removed.
- **FR-016**: Unused imports of Kafka Streams types in example simulations MUST be removed.
- **FR-017**: Any dependency exclusion that exists only to work around a collision between shed
  artifacts MUST be removed once the collision no longer exists.

#### Resolved scope decisions

- **FR-018**: The mandatory Kafka client dependency MUST remain inherited by consumers and MUST be
  sourced from coordinates published to the default public repository, replacing the vendor rebuild
  currently pinned. A consumer therefore continues to acquire the Kafka client automatically by
  declaring the plugin alone. The client version line consumers observe changes as a consequence, which
  is what FR-013 governs.
- **FR-019**: The Kafka Streams artifact MUST remain inherited in this release and MUST become
  resolvable from the default public repository under FR-001, because the two deprecated implicits are
  published API and cannot compile without it. Shedding the artifact itself is deferred to the release
  in which those implicits are removed. Consumers MUST NOT be required to declare it, and MUST NOT
  experience any compilation or runtime change from this decision.
- **FR-020**: Avro and Schema Registry entry points MUST be reachable through a dedicated opt-in entry
  point that a consumer imports deliberately, so that the default DSL entry point carries no reference
  to an Avro or Schema Registry type. The entry points at their current locations MUST keep compiling
  for at least this release under FR-010, each deprecated with its new location named, and the
  migration guide MUST state the one-line change an Avro consumer makes.

### Key Entities

- **Inherited dependency set**: what a consumer's build acquires automatically by declaring the plugin.
  Today it includes four artifacts unavailable from the default public repository. Its correctness is
  the whole of Story 1.
- **Opt-in dependency set**: artifacts a consumer declares deliberately to enable an optional
  capability, together with any repository required to fetch them. Today it holds the Avro case-class
  library; it must grow to hold Avro serialization and Schema Registry support.
- **Plain-serialization surface**: the entry points reachable and executable with only the inherited
  dependency set. Must be self-sufficient.
- **Avro / Schema Registry surface**: the entry points requiring the opt-in set — Avro serdes, the
  Schema-Registry-backed serde factory, Avro body checks, and the Java Avro entry points. Must be
  inert, not merely unused, when the opt-in set is absent.
- **Deprecated Kafka Streams surface**: two implicits with no caller, exposing types from a product the
  plugin does not use, and the sole reason the Kafka Streams artifact is inherited.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The number of dependencies a consumer inherits that cannot be fetched from the default
  public artifact repository is **zero**, down from four in the current release.
- **SC-002**: A consumer starting from an empty project and following only the installation
  documentation reaches a first successful message send **without configuring any repository** and
  without consulting any source outside that documentation.
- **SC-003**: All three documented installation paths — sbt, Gradle, Maven — resolve, compile, and
  execute a plain simulation from a scratch project, verified independently rather than by analogy from
  one to the others.
- **SC-004**: A consumer enabling Avro and Schema Registry support succeeds by adding **only** the
  coordinates and the repository named in the documentation, with **no source change** beyond what the
  migration guide states.
- **SC-005**: Every Avro and Schema Registry capability available in the previous release remains
  available after opting in, with unchanged behavior.
- **SC-006**: An existing plain-serialization suite upgrades with **zero** changes to its build files
  and sources.
- **SC-007**: Every inherited dependency traces to a plugin code path that uses it, with **exactly one**
  recorded exception — the Kafka Streams artifact, retained solely to keep the deprecated implicits
  compiling until their named removal release. The count of untraceable, unrecorded inherited
  dependencies is **zero**.
- **SC-008**: Every entry point retired by this change still compiles and reports a deprecation naming
  its removal release; the count of published entry points removed without a deprecation period is
  **zero**.
- **SC-009**: The example-simulation smoke validation and the full test suite pass unchanged, with no
  test relaxed or disabled to accommodate the dependency change.

---

## Assumptions

- **The default public artifact repository is Maven Central.** It is the only repository configured by
  default in sbt, Gradle, and Maven, and is what "a consumer following the README" implies.
- **"Not inherited" follows the pattern already established in this build** by the Gatling core and Avro
  case-class dependencies, which consumers declare themselves. This specification states the outcome —
  the consumer does not acquire the artifact automatically — and leaves the mechanism to planning.
- **Confluent's Avro serialization and Schema Registry artifacts have no Maven Central equivalent.**
  Verified on 2026-08-09 at both the published (`7.9.8`) and current (`7.9.9`) versions: absent from
  Maven Central, present in the Confluent repository. Therefore the only way to satisfy FR-001 for them
  is to stop inheriting them; relocation is not an option.
- **The mandatory Kafka client artifact does have a Maven Central equivalent.** Verified on 2026-08-09:
  the Confluent `-ce` and `-ccs` rebuilds are absent from Maven Central at every version checked
  (`7.9.2-ccs`, `7.9.5-ce`, `7.9.5-ccs`, `7.9.8-ce`, `7.9.8-ccs`, `7.9.9-ce`, `7.9.9-ccs`), while
  Apache-released `kafka-clients` and `kafka-streams-scala` are present. FR-018 and FR-019 both depend
  on this: the Kafka client and the Kafka Streams artifact can satisfy FR-001 by relocation, without
  either being pushed onto the consumer to declare.
- **The version numbers in this specification are a snapshot.** Dependency automation moves them
  regularly. Every requirement here is written against the *property* — inherited versus opt-in,
  Central-resolvable versus vendor-only — not against a version string, so a bump landing before
  implementation changes the evidence but not the work.
- **The Apache-released Kafka client is functionally equivalent to the vendor rebuild for this
  plugin's purposes.** The rebuild carries the same upstream code under a vendor version scheme. If
  planning finds a behavioral difference that matters to the plugin, that is a finding to raise, not an
  assumption to work around silently.
- **Consumers run the plugin in a test or load-test configuration**, not as a production runtime
  dependency, so requiring a small number of explicit opt-in declarations is acceptable when documented.
- **The three installation paths in the README — sbt, Gradle Kotlin DSL, and Maven — are the supported
  set.** No other build tool is in scope for verification.
- **A broker and a Schema Registry are available for verification**, via the project's existing
  containerised test infrastructure and Compose stack.
- **The current release line is 1.2.x and this work targets 1.3.0**, a minor release. Deprecations added
  here name their removal in the next major release.
- **The two unused Kafka Streams implicits have no consumers worth preserving compatibility for beyond
  the project's standard deprecation window.** They have never had a caller in this repository, and the
  capability they expose belongs to a different product.

---

## Out of Scope

- Upgrading the Kafka, Avro, Gatling, or Schema Registry version lines for any reason other than
  satisfying FR-001. Version currency is separate work.
- Restructuring the Avro or Schema Registry feature set, changing how serdes are selected, or altering
  check semantics. This work relocates and documents existing capability; it does not redesign it.
- Removing the two deprecated Kafka Streams implicits, and removing the Kafka Streams artifact from the
  inherited set. Per FR-019 both are deferred to the release named in the deprecation.
- Removing the Avro and Schema Registry entry points from their current locations. Per FR-020 they are
  deprecated in place this release and removed later.
- Changing the publishing configuration's repository-stripping behavior, which is correct for a
  Sonatype release and is not the defect.
- Any change to request-reply correlation, tracker, or consumer behavior. Those are covered by prior
  features.
- Publishing a release. This feature ends when the change is merged and verified; tagging follows the
  project's normal release process.

---

## Dependencies

- Access to the published artifact metadata for the current release, to verify the declared dependency
  set as consumers see it.
- Scratch consumer projects for sbt, Gradle, and Maven, resolving from the default public repository
  only, used as the acceptance harness for Stories 1 and 2.
- The project's containerised broker and Schema Registry infrastructure, for executing the plain and
  Avro simulations named in the acceptance scenarios.
- A Maven Central Kafka client release compatible with the brokers the project tests against, since
  FR-018 relocates the client artifact rather than pinning the vendor rebuild.
