# Contract: Published Artifact Metadata

**Feature**: `005-classpath-dependency-shedding` | **Date**: 2026-08-09

The published POM is the plugin's contract with every consumer's build tool. It is the interface this
feature repairs, so it is specified here as an interface rather than left as a side effect of the build
definition.

**Consumers of this contract**: sbt, Gradle, and Maven resolvers in downstream projects; the Sonatype
release pipeline.

---

## C1 — Inherited scopes carry only Maven Central coordinates

Every `<dependency>` whose `<scope>` is inherited by consumers (`compile` or `runtime`, including the
implicit default) MUST be resolvable from `https://repo1.maven.org/maven2/` with no other repository
configured.

**Rationale**: the POM cannot carry a repository — `ThisBuild / pomIncludeRepository := { _ => false }`
strips them, correctly, for a Sonatype release. A coordinate a consumer cannot fetch and cannot be told
where to fetch from is unusable.

**Assertion** (test, runs on every build):

```text
for each dependency in makePom output:
  if scope in {compile, runtime, absent}:
    assert group:artifact:version is fetchable from Maven Central
```

The check may be implemented offline against a deny-list of known vendor-only patterns
(`io.confluent:*`, and any `org.apache.kafka:*` version bearing a `-ce` or `-ccs` suffix) so it does not
depend on network access in CI. A network-backed variant is acceptable but must not be the only gate,
because a network failure would then read as a pass.

**Current state**: FAILS. Four dependencies violate this — on current `main`, `kafka-clients:7.9.9-ce`,
`kafka-streams-scala_2.13:7.9.9-ce`, `kafka-streams-avro-serde:7.9.9`, `kafka-avro-serializer:7.9.9`;
in published `1.2.0`, the same four at `7.9.5-ce` / `7.9.8`. This is the failing-first test required by
Constitution Principle IV.

**The assertion is written over the pattern, not the versions.** These coordinates advance regularly
under dependency automation — they moved a full patch line during this feature's specification, staying
equally unresolvable. A check pinned to specific version strings would go quiet on the next bump, which
is the failure mode that let this defect ship in the first place.

---

## C2 — Optional capabilities are not inherited

A dependency required only by an optional capability MUST be declared in a scope consumers do not
inherit.

Applies to: Confluent Avro serialization, Confluent Schema Registry, avro4s.

**Assertion**: the inherited dependency set contains no `io.confluent` coordinate and no avro4s
coordinate.

**Note**: this is a stronger statement than C1 for these artifacts. C1 alone could in principle be
satisfied by relocation; R1 established that no Central-published equivalent exists, so for these two
the only compliant state is non-inheritance.

---

## C3 — Every inherited dependency is justified

Each inherited dependency MUST map to either a plugin code path that uses it, or a recorded deprecation
naming the release in which it goes away.

**Assertion**: the count of inherited dependencies with neither is zero. At most one may be justified by
deprecation in this release (data-model DR-4) — `kafka-streams-scala`, held by the two deprecated
implicits.

**Rationale**: FR-014 and SC-007. The bound of one is what stops "retained for a deprecation" from
becoming a general-purpose excuse.

---

## C4 — Build and consumer classpaths agree

The Kafka client version the plugin is compiled and tested against MUST equal the version a consumer
resolves from the published POM.

**Rationale**: opt-in dependencies remain on the plugin's *own* compile classpath, and R2 demonstrated
this is not automatic — with the Apache coordinate declared, the build still resolved
`kafka-clients:7.9.9-ccs` via `kafka-schema-registry-client`, silently. A build that compiles and tests
against a different client than it ships against produces test results that do not describe the shipped
artifact.

**Assertion**: `sbt evicted` reports no version conflict for `org.apache.kafka:kafka-clients`. This is
gate G1.

---

## C5 — Metadata that must stay unchanged

This feature changes dependencies only. The following MUST be byte-identical to the previous release
except for the version: `groupId`, `artifactId`, `packaging`, `licenses`, `scm`, `developers`,
`organization`, `url`, and the `provided`/`test` status of the Gatling and testing artifacts.

**Rationale**: guards against a build-file edit accidentally changing publication identity, which for a
Sonatype release is unrecoverable.

---

## Contract test summary

| ID | Gate | Fails today | Speed |
| --- | --- | --- | --- |
| C1 | Inherited ⟹ Central-resolvable | **yes**, 4 violations | fast, offline |
| C2 | Optional ⟹ not inherited | **yes**, 2 violations | fast, offline |
| C3 | Inherited ⟹ justified, ≤1 deprecation-held | **yes** | fast, offline |
| C4 | Build client == shipped client | **yes** (R2) | fast |
| C5 | Publication identity unchanged | no | fast, offline |

C1–C3 and C5 are assertable from `makePom` output without a network or a broker, so they belong in the
normal `sbt test` run. C4 reads `evicted`. None of them requires the scratch-project harness, which
covers the complementary property — that a real build tool, not just the metadata, succeeds — and is
specified in [quickstart.md](../quickstart.md).
