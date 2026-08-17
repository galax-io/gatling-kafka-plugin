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

## R3 — Every entry is traceable

Each entry in the record MUST cite the verdict that authorises it: `A1`–`A12`, `B1`, or the two US3
freeze artifacts.

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
| Removed | Fully-qualified symbol, kind (method/field/class/trait), authorising verdict |
| Changed | Symbol, before → after signature, authorising verdict |
| Cascades | Symbols changed as a consequence, each naming its parent verdict |
| Dependencies | Inherited set before → after |
| Reviewer note | Who reviewed it, and confirmation that no entry lacks a verdict |

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

## Status

**Not yet produced.** This file currently defines the contract. It is completed with the actual record
at the point described in R6, at which time the sections in R5 replace this status block.
