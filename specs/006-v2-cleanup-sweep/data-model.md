# Phase 1 Data Model: v2.0.0 Cleanup — Validated Removal Sweep

**Feature**: `006-v2-cleanup-sweep` | **Date**: 2026-08-09

This feature's "data" is not runtime state — it is the **removal ledger**: the set of decisions about
what leaves the repository, each with a class, an evidence basis and a rule governing what may be done
with it. Modelling it explicitly is what makes the sweep auditable, and it is what
`contracts/removed-api.md` is checked against before the release tag.

Two runtime entities also change shape (`KafkaProtocolMessage`, the inherited dependency set); both are
modelled below.

---

## Entity: Removal Entry

One row per thing this feature deletes, changes or deliberately keeps.

| Field | Values | Meaning |
|---|---|---|
| `id` | `A1`–`A12`, `B1`, `C1` | Verdict identifier from spec.md *Audit Verdicts*. Stable — referenced by commits, tasks and the break-surface record |
| `class` | `unusable` \| `unreachable` \| `unused` \| `residue` \| `freeze-artifact` \| `keep` | What kind of thing it is; drives the rules below |
| `visibility` | `published` \| `internal` | Whether removing it changes a signature a consumer can compile against |
| `evidence` | free text | How deadness was established, in the current sources |
| `story` | `US1`–`US5` | Which commit removes it |
| `survivor` | symbol or test name, or `none` | What a user or a test relies on instead |

### Classification rules

- **RE-1**: Every entry MUST carry evidence gathered from the current sources. An entry justified only
  by an issue body is not admissible — three such claims were overruled during specification.
- **RE-2**: `visibility = published` ⟹ the entry MUST appear in the break-surface record
  (`contracts/removed-api.md`) **and** in the README migration guide. One implies the other; neither
  alone is sufficient.
- **RE-3**: `class = keep` entries are part of the ledger, not omissions from it. They exist so a later
  reader does not re-derive a deletion that was already considered and refused.
- **RE-4**: An entry whose `class` is `unused` (works, but nothing calls it) MUST name a `survivor`, or
  state explicitly that the capability itself is being withdrawn. `unusable` and `unreachable` entries
  need no survivor — there is nothing to migrate off.
- **RE-5**: No entry may be added during implementation without a verdict. If the sweep uncovers
  further dead surface, it is recorded as a new verdict in spec.md first, with evidence — not deleted
  opportunistically.

### The ledger

| id | class | visibility | story | survivor |
|---|---|---|---|---|
| A1 `sessionWindowedSerde`, `consumedFromSerde` | unused | published | US1 | depend on `kafka-streams-scala` directly |
| A2 dependency chain: `kafka-streams-scala`, 2 override pins, justification entry, DR-4 + its check | unused | published (POM) | US1 | none — capability withdrawn |
| A3 topic-less `send(...)`, Scala + Java | **unusable** | published | US1 | `kafka(name).topic(t).send(...)` |
| A4 `KafkaProtocolMessage.responseCode`, `KafkaCheckType.ResponseCode` | unreachable | published | US1 | `KafkaCheckType.Simple`; failure types on KO paths are unaffected |
| A5 `KPProducerSettingsStep.timeout` / `withDefaultTimeout` | unused | published | US1 | the same methods on `KPConsumeSettingsStep` |
| A6 `KafkaRequestFailureMessages.buildFailure` + 3 spec cases | unreachable | internal | US2 | `sendFailure` |
| A7 `ExpressionBuilder.bytes(String)` | unreachable | internal | US2 | none |
| A8 `AvroBodyCheckBuilder` private type alias | residue | internal | US2 | none |
| A9 `completionCause` `CompletionException` branch | unreachable | internal | US2 | pass `error` straight through |
| A10 22 unused imports (12 files) | residue | internal | US2 | none |
| A11 stale comment + stale scaladoc | residue | internal | US2 | none |
| A12 empty `avroSchemas` + commented scaffolding | residue | internal (build) | US2 | none |
| B1 `KafkaCheckMaterializer.avroBody`, `KafkaMessagePreparer.avroPreparer`, `AvroErrorMapper` | unreachable | published | US1 | `KafkaCheckSupport.avroBody` / `KafkaDsl.avroBody()` — already the only reachable path |
| B2 `KafkaAttributes.producerTopic` `Option` → plain, and `KafkaAction.missingProducerTopicError` | residue (cascade of A3) | published | US1 | none — the state it modelled cannot occur once A3 lands |
| — `LazyGenericAvroSerde` | freeze-artifact | published | US3 | `avroSerde` as `implicit def` |
| — `RequestBuilder` trait | freeze-artifact | published | US3 | `KafkaRequestBuilder` |
| **C1** `idleSweep.cancel(false)` | **keep** | internal | — | not removed; see RE-3 |

---

## Entity: Inherited Dependency

Carried forward from feature 005, amended here. One entry per coordinate a consumer inherits.

| Field | Values |
|---|---|
| `coordinate` | `group:artifact` (Scala binary suffix stripped) |
| `justification` | `used-by: <code path>` |
| `scope` | `compile` \| `runtime` |

### Rules

- **DR-1** *(unchanged)*: every inherited coordinate MUST be resolvable from Maven Central.
- **DR-2** *(unchanged)*: optional capabilities (Confluent, avro4s) MUST NOT be inherited.
- **DR-3** *(unchanged)*: every inherited coordinate MUST appear in the justification map.
- **DR-4** *(**removed by this feature**)*: "at most one inherited dependency may be justified by a
  deprecation". The budget existed for exactly one case, and this feature removes that case. The rule,
  its check in `checkPublishedPom`, and the `deprecated:` prefix convention go with it — leaving a
  budget of one against zero possible claimants is an invitation to spend it.

### State transition

| coordinate | before (1.3.0) | after (2.0.0) |
|---|---|---|
| `org.scala-lang:scala-library` | inherited, `used-by` | unchanged |
| `org.apache.kafka:kafka-clients` | inherited, `used-by` | unchanged |
| `org.apache.avro:avro` | inherited, `used-by` | unchanged |
| `org.apache.kafka:kafka-streams-scala` | inherited, `deprecated:` | **removed** |

Measured during research: the current POM declares exactly these four at `compile` scope. Target is
three, all `used-by`. `kafka-clients` **stays** in `dependencyOverrides` — it is pinned against
Confluent's vendor rebuild, which is a separate concern from the streams pins being removed.

---

## Entity: Kafka Protocol Message (shape change)

The single wire representation. One field is removed; nothing else changes.

| Field | Before | After |
|---|---|---|
| `key` | `Array[Byte]`, nullable | unchanged |
| `value` | `Array[Byte]`, nullable | unchanged |
| `producerTopic` | `String` | unchanged |
| `consumerTopic` | `String` | unchanged |
| `headers` | `Option[Headers] = None` | unchanged — remains the last parameter |
| `responseCode` | `Option[String] = None` | **removed** |

### Rules

- **KM-1**: `responseCode` is the **last** constructor parameter, so removing it leaves positional
  construction of the first five arguments source-compatible. `apply`, `unapply` and `copy` still change
  arity — that is a break, and it belongs in the record under RE-2.
- **KM-2**: The two call sites in `KafkaMessageTracker` that forward `message.responseCode` on
  reply-received paths pass `None` explicitly instead. They have never forwarded anything else.
- **KM-3**: The **reporting slot** is not this field and does not change. `KafkaRequestReplyAction`
  and `KafkaMessageTracker.failPending` populate it with the real failure type on KO paths, and must
  continue to.
- **KM-4**: The trace line in `KafkaLogging.describeMessage` drops the field. It MUST keep describing
  everything it still has, and `KafkaLoggingSpec` is updated to the new exact text — not relaxed to a
  substring match, which would weaken a working assertion to accommodate a removal.

---

## Entity: Unused-Code Guard

| Field | Value |
|---|---|
| `classes` | `imports`, `privates`, `locals`, `patvars` |
| `excluded` | `params`, `implicits` — see FR-010 |
| `enforcement` | fatal, via the build's existing `-Xfatal-warnings` |
| `scope` | main **and** test sources |
| `suppressions permitted` | **zero** |

### Rules

- **UG-1**: The guard MUST be satisfiable with no `@nowarn` and no per-file opt-out. A suppression is
  evidence the wrong warning class was chosen, not a fix.
- **UG-2**: Measured baseline at the moment it is switched on: 23 findings (22 imports + 1 private
  type). Every one is already a ledger entry (A10, A8). A finding that is *not* a ledger entry means
  the sources changed under the plan and needs a verdict before it is deleted (RE-5).
- **UG-3**: The guard goes in with US2 and stays live for US3, US5 and US4, so no later story can leave
  residue behind.

---

## Entity: Test Removal (US4)

| Field | Values |
|---|---|
| `test` | file + test name |
| `reason` | `cannot-fail` \| `shadowed` \| `permutation` \| `duplicate-recipe` |
| `survivor` | the retained test that detects the same failure — **required** |
| `action` | `remove` \| `merge` \| `strengthen` |

### Rules

- **TR-1** *(FR-018)*: `action = remove` or `merge` REQUIRES a named `survivor`. No survivor, no
  removal. This is the mutation argument written down instead of assumed.
- **TR-2** *(FR-019)*: the vacuous pending-request assertion has `action = strengthen`, never `remove`.
  It must be shown failing against a deliberately reverted guard before the strengthened form is kept.
- **TR-3** *(FR-022)*: tests pinning known concurrency races are not eligible for any action except
  `merge` with a near-duplicate, and the survivor must still detect every failure both detected. The
  redesign that would retire them is not part of this feature.
- **TR-4**: A removal MUST NOT be compensated by adding a mock. Where a broker-backed test is merged,
  the survivor is also broker-backed (Constitution II).
- **TR-5**: Example simulations are documentation. Only vacuous validation, dead code and duplicated
  recipes are eligible; their value as illustration is preserved.
