# Contract: Break-Surface Record

**Feature**: `006-v2-cleanup-sweep` | **Requirement**: FR-027, SC-011

This contract defines the artifact that stands in for an automated binary-compatibility guard. It
exists because clarification Q1 chose to release independently of the milestone that wires MiMa
(#217), and a v2.0.0 that removes published symbols from a permanent Sonatype release cannot be
allowed to remove one nobody looked at.

The record is produced **once per release**, from a mechanical diff, reviewed by a human, and checked
in. It is not a build gate and must not become one — that is #217's design space.

---

## R1 — Baseline

The record is a diff against **`org.galaxio:gatling-kafka-plugin_2.13:1.3.0`**, the previous release,
fetched from Maven Central.

- The baseline coordinate MUST be stated in the record itself. A record that does not say what it was
  compared against proves nothing.
- The baseline artifact MUST come from the published repository, not from a local build of an old tag.
  What matters is what consumers actually compiled against.

**Verified during research**: the 1.3.0 jar is present on Maven Central and fetchable without
credentials.

## R2 — Extraction

Public signatures are extracted from both jars mechanically — `javap -public` over every class,
normalised — and diffed. The extraction MUST NOT be hand-transcribed.

**Rationale**: the intended removals are already enumerated in spec.md's verdict tables. The record
exists to catch what those tables cannot state — the **cascades**. Folding the `RequestBuilder` trait
into its implementation changes the declared return type of every documented `send(...)`; removing a
`case class` field changes `apply`, `unapply` and `copy`. A hand-written list reproduces the author's
intent, including its blind spots. A diff does not.

**Scope limit — signatures only.** `javap` compares declarations. A behaviour change behind an
unchanged signature is invisible to it, and this method will report such a release as clean. That is
not hypothetical: in this very release the Java `topic(...)` entry point switched its request name from
a literal to a Gatling EL expression with an identical signature, and the diff did not show it.

So the diff is necessary and not sufficient. It MUST be paired with a **body-diff pass**: read
`git diff` for every published method whose body the release touched, and ask whether an unchanged
signature is doing something different. Findings from that pass belong in the record's `Changed`
section alongside the signature-level entries, and are subject to the same rule as everything else —
no verdict, no release.

## R3 — Every entry is traceable

Each entry in the record MUST cite the verdict that authorises it: `A1`–`A12`, `B1`, `B2`, or the two
US3 freeze artifacts. This applies to entries from the body-diff pass (R2) exactly as it does to
signature entries.

**An entry with no verdict is an unintended break and blocks the release.** Resolving it means one of:

- it is a cascade of an authorised removal — record it as such, naming the parent verdict; or
- it is a mistake — revert it; or
- it is genuinely dead surface the audit missed — add a verdict to spec.md with evidence first
  (data-model RE-5), then re-run the diff.

Silently accepting an unexplained entry is the failure mode this contract exists to prevent.

## R4 — Scope

The record covers the **published** surface only: the Scala DSL under `org.galaxio.gatling.kafka`, the
Java facade under `org.galaxio.gatling.kafka.javaapi`, and the published POM's inherited dependency
set. `private`, package-private and test-only symbols are out of scope — they cannot break a consumer.

The dependency section MUST show the inherited set before and after. Measured target: four coordinates
→ three, with `org.apache.kafka:kafka-streams-scala` removed and the other three unchanged.

## R5 — Shape

The record is a checked-in Markdown file at `specs/006-v2-cleanup-sweep/contracts/removed-api.md`
(this file, completed at release time) or a sibling named in it. Minimum content:

| Section | Content |
|---|---|
| Baseline | Coordinate, source, date fetched |
| Method | The extraction command, so the diff can be regenerated exactly |
| Removed | Every removed signature grouped by authorising verdict, with counts that sum to the diff's total |
| Changed | Behaviour changes behind unchanged signatures, from R2's body-diff pass, each naming its verdict |
| Cascades | Symbols changed as a consequence, each naming its parent verdict |
| Dependencies | Inherited set before → after |
| Reviewer note | Who reviewed it, and confirmation that no entry lacks a verdict |

The `Removed` section groups by verdict rather than listing every symbol individually, and the raw
diff is deliberately not checked in (see the record below). The auditable claim is therefore *the
counts reconcile*: each group's count is stated, they sum to the diff's total, and the diff is
regenerable in seconds from the command in `Method`. A reviewer who wants symbol-level detail runs
that command; a reviewer checking that nothing slipped through reads the counts. A per-symbol table
transcribed into Markdown would be a second copy of machine output that nothing keeps in step with
the artifacts — the failure mode this release exists to remove.

## R6 — Timing

Produced **after US3** — the last story that changes a published signature — and reviewed **before**
the `v2.0.0` tag is pushed. Producing it earlier records an incomplete surface; producing it after the
tag records history rather than gating it.

## R7 — Relationship to the migration guide

Every `published` entry in the record MUST also appear in the README migration guide (FR-006,
data-model RE-2). They serve different readers: the record is for the maintainer deciding whether to
release, the guide is for the user hitting a compilation error. Neither substitutes for the other, and
a symbol in one but not the other is a defect in whichever is missing it.

## R8 — Afterlife

When #217 wires MiMa, this record becomes its first baseline. It is therefore written to be read by
someone who was not present for this release, and it must not assume access to this conversation.

---

## The record — v1.3.0 → v2.0.0

**Baseline**: `org.galaxio:gatling-kafka-plugin_2.13:1.3.0`, fetched from Maven Central on 2026-08-18.

**Method**: `scripts/api-signatures.sh` over both jars (`javap -public` per class, normalised), diffed.
Machine-produced, not hand-transcribed — the classification below is the reviewed output, and the diff
it came from is *not* checked in. It is derived data: both inputs are permanent (the 1.3.0 artifact on
Maven Central, the 2.0.0 tag) and the extraction is a checked-in script, so a copy in the repository
would be a second source of truth that nothing keeps honest. Reproduce it with:

```bash
cs fetch --classpath org.galaxio:gatling-kafka-plugin_2.13:1.3.0 \
  | tr ':' '\n' | grep gatling-kafka-plugin | head -1 \
  | xargs scripts/api-signatures.sh > /tmp/api-1.3.0.txt
sbt package
scripts/api-signatures.sh target/scala-2.13/gatling-kafka-plugin_2.13-*.jar \
  | grep -v -- -tests > /tmp/api-2.0.0.txt
diff /tmp/api-1.3.0.txt /tmp/api-2.0.0.txt
```

**Totals**: 1568 public signatures before, 1453 after — **139 removed, 24 added**.

### Removed, by authorising verdict

| Count | Verdict | What |
|---|---|---|
| 68 | **A3** | The `javaapi` topic-less `send(...)` / `sendWithClass(...)` matrix |
| 14 | **A4** | `KafkaProtocolMessage.responseCode` — the field, and the `apply`/`copy`/`unapply`/`tupled`/`curried` arity that follows it; `KafkaCheckType.ResponseCode`; the `describeMessage` lambda that rendered it |
| 12 | **US3** | The `RequestBuilder` trait, its Java wrapper's constructor, and the `KafkaDsl` conversion that took it |
| 10 | **A1/US3** | `KafkaSerdesImplicits` members inherited by `KafkaMessageTrackerPool`, which mixes the trait in: the two Streams implicits and the `avroSerde` mixin setter |
| 10 | **B2** | `KafkaAttributes.producerTopic` as an `Option`, its companion arity, and the `resolveProducerTopic` lambdas in `KafkaAction` |
| 9 | **B1** | `KafkaCheckMaterializer.avroBody`, `KafkaMessagePreparer.avroPreparer`, `AvroErrorMapper` |
| 6 | **A1/US3** | The same members on `Predef` and `KafkaSerdesImplicits` themselves |
| 5 | **A3/US3** | The Scala topic-less `send(...)` overloads, plus `OnlyPublishStep`'s changed return type |
| 3 | **A6** | `KafkaRequestFailureMessages.buildFailure` |
| 2 | **A5** | `KPProducerSettingsStep.timeout` / `.withDefaultTimeout` |

**Unexplained entries: 0.** Every removed signature maps to a verdict in [spec.md](../spec.md).

### Changed — behaviour behind an unchanged signature

From R2's body-diff pass. `javap` cannot see these; they were found by reading the diff of published
method bodies, and would otherwise have shipped unrecorded.

| Symbol | Before → after | Verdict |
|---|---|---|
| `javaapi.request.builder.KafkaRequestBuilderBase.topic(String)` | The produce-only request name was passed as a literal (`toStaticValueExpression`); it now resolves as a Gatling expression, because the method delegates to the wrapped Scala builder that `KafkaDsl.kafka(String)` built with `toStringExpression`. Makes produce-only consistent with request-reply, which always resolved EL | **US3** (cascade of folding the builder chain) |

Signature unchanged, so this entry has no counterpart in the 139/24 totals. It is in the migration
guide under "Java produce-only request names now resolve Gatling EL".

### Cascades

Four groups are consequences of an authorised removal rather than removals in their own right. They
are what a hand-written list would have missed, and why R2 requires a mechanical diff:

- **`case class` arity.** Removing `KafkaProtocolMessage.responseCode` and changing
  `KafkaAttributes.producerTopic` rewrites `apply`, `copy`, `copy$default$N`, `unapply`, `tupled` and
  `curried` on both companions. 10 of the 24 *additions* are these members at their new arity.
- **Mixin inheritance.** `KafkaMessageTrackerPool` and `Predef` both mix in `KafkaSerdesImplicits`, so
  removing two implicits and turning `avroSerde` into a `def` deletes members from classes this
  feature never edited — including the `..._setter_$avroSerde_$eq` mixin setter whose disappearance is
  the exact hazard the 1.x freeze was protecting against, paid for openly here.
- **Compiler-generated lambdas.** `$anonfun$resolveProducerTopic$1`, `$anonfun$describeMessage$3` and
  their siblings vanish with the method bodies that produced them.
- **Return types.** Folding the `RequestBuilder` trait changes the declared return type of every
  documented `send(...)`, and the Java wrapper's constructor parameter with it.

### Inherited dependencies

| Coordinate | 1.3.0 | 2.0.0 |
|---|---|---|
| `org.scala-lang:scala-library` | inherited | unchanged |
| `org.apache.kafka:kafka-clients` | inherited | unchanged |
| `org.apache.avro:avro` | inherited | unchanged |
| `org.apache.kafka:kafka-streams-scala` | inherited | **removed** |

Four → three, every one `used-by` a code path. `checkPublishedPom` passes with the DR-4 deprecation
allowance gone.

### Reviewer note

Produced and reviewed after US3, the last story to change a published signature, and before any tag.
Every entry carries a verdict; nothing in the diff was accepted without one. Each `published` entry
also appears in the README migration guide (R7).

The body-diff pass required by R2 was run separately from the signature diff and found one entry, in
`Changed` above. It was added after review — the signature diff alone had reported this release clean,
which is precisely why R2 no longer treats that as sufficient.

## Status

**Complete for v2.0.0.** When MiMa lands (#217), this record is its first baseline.
