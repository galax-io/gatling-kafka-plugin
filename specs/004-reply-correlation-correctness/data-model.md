# Phase 1 Data Model: Reply Correlation Correctness

**Feature**: `004-reply-correlation-correctness` | **Date**: 2026-08-07

This feature introduces no new entity. It corrects the **state space** of three existing ones: two
states that were being collapsed into one are separated, and one state that had no defined outcome
gains one. Entities below map to the spec's Key Entities section.

---

## 1. Correlation Identity

The value a `KafkaMatcher` extracts from a message to connect a reply to its request.

**Type**: `Array[Byte]`, nullable. Produced by `KafkaMatcher.requestMatch` / `responseMatch`
(`KafkaProtocol.scala:18-36`). Not a new type — `KafkaMatcher` is the single matching contract and is
used as-is.

### State space

| State | Representation | Before | After |
|---|---|---|---|
| Absent | `null` | folded into Empty | distinct; request fails at issue time |
| Empty | `Array.emptyByteArray` | folded with Absent | distinct; tracked normally |
| Present | non-empty `Array[Byte]` | tracked | tracked (unchanged) |

**The defect is the folding, in two places, in opposite directions:**

- `KafkaAction.resolveToProtocolMessage:87` — `key.getOrElse(Array.emptyByteArray)` turns Absent into
  Empty on the produce side.
- `KafkaMessageTracker.matchKeyFor:113-114` — `if (m == null) Array.emptyByteArray else m` turns
  Absent into Empty inside the correlation map.

Together they make every keyless request-reply request share one `MatchKey`.

### Validation rules

- **VR-1** (FR-004): an Absent identity on the request side is invalid for correlation. The request is
  reported KO at issue time, before registration and before the send.
- **VR-2** (FR-001): Absent and Empty MUST NOT compare equal. Satisfied structurally once the folding
  is removed — `Arrays.hashCode(null) == 0`, `Arrays.hashCode(Array.empty) == 1`,
  `Arrays.equals(null, Array.empty) == false`.
- **VR-3** (FR-006): no identity is ever synthesised for a request that supplied none.
- **VR-4**: an Absent identity on the *reply* side is already rejected before matching
  (`KafkaMessageTracker.scala:222-223`) and stays rejected.

---

## 2. MatchKey

Value-equality wrapper over a correlation identity, used as the key of the tracker's correlation map
(`KafkaMessageTracker.scala:105-114`). Private to the tracker.

**Change**: `matchKeyFor` stops substituting. `new MatchKey(m)` for all `m`, including `null`.

**Invariant preserved**: `hashCode` is still computed once at construction, and `equals` still compares
bytes. No branch is added to the per-reply path.

**Reachability after the change**: `MatchKey(null)` becomes unreachable from the publish path (VR-1
rejects those before registration) and from the consume path (VR-4 rejects those before lookup). It is
retained as a total function rather than a partial one — the map must not be able to alias two
identities regardless of which caller reaches it.

---

## 3. Request-Reply Exchange

One request and the reply that answers it: `KafkaMessageTracker.MessagePublished`, held in
`sentMessages: mutable.HashMap[MatchKey, MessagePublished]`.

**No structural change.** `token` (added for #191) already distinguishes registrations sharing an
identity, and `sentTimestamp` already carries the handoff moment (settled by #170).

### Lifecycle

```text
                    ┌─ identity Absent ──────────────► KO "no identity"   [NEW — VR-1]
                    │                                   (never registered, never sent)
request issued ─────┤
                    └─ identity Empty or Present ───► registered ──► sent
                                                          │
                            ┌─────────────────────────────┼──────────────────────────┐
                            ▼                             ▼                          ▼
                     reply matched              identity displaced            reply timeout
                            │                    by a later request                  │
              ┌─────────────┴─────────────┐             │                            ▼
              ▼                           ▼             ▼                           KO
        checks pass                 checks fail        KO "match id reused"
              │                           │
              ▼                           ▼
             OK                          KO
                            │
                            └─ check throws ──► KO   [NEW — FR-008 terminal catch]
```

Every path terminates. The two `[NEW]` edges are the ones this feature adds; before it, the first had
no edge at all (the request was matched wrongly instead) and the last had no edge either (the virtual
user stalled).

---

## 4. Reply Message

A message received on a reply channel: `KafkaProtocolMessage`, built by
`KafkaProtocolMessage.from(consumerRecord, inputTopic)`.

**Key insight — no type change needed.** `KafkaProtocolMessage.key` and `.value` are already
`Array[Byte]` and already nullable: `from` copies `consumerRecord.key()` and `.value()` verbatim, both
of which are `null` for keyless records and tombstones respectively. The produce side was not using
that nullability; the check side was not handling it.

### Value state space

| State | Representation | Body-check outcome before | after |
|---|---|---|---|
| Absent | `null` | **NPE → virtual user stalls** | KO naming the absent payload |
| Empty | `Array.emptyByteArray` | `""` / empty, success | unchanged (FR-010) |
| Present | non-empty | parsed | unchanged |

### Validation rules

- **VR-5** (FR-007): an Absent value produces a `Validation` failure in every reply-content preparer.
- **VR-6** (FR-009): all preparers agree on Absent. Currently `xmlPreparer` and `avroPreparer` are
  guarded by `safely(ErrorMapper)`; `stringBodyPreparer`, `bytesBodyPreparer` and `jsonPathPreparer`
  are not.
- **VR-7** (FR-010): Absent and Empty MUST NOT be conflated. `bodyString.is("")` must not pass on a
  tombstone.
- **VR-8** (FR-008): no check outcome may leave a request without a terminal report — enforced
  independently of VR-5 by the catch in `completeMatched`.

---

## 5. Reply Channel Readiness

The `CompletableFuture[Void]` returned by `DynamicKafkaConsumer.requestTopicSubscription`, completed by
`completeAssignedReadiness` (`DynamicKafkaConsumer.scala:129-141`).

**No structural change** — the same future, completed later and on a stronger condition.

### State transition

| Stage | Consumer state | Readiness before | after |
|---|---|---|---|
| Subscribed | topic in `subscription()` | pending | pending |
| Assigned | topic in `assignment()` | **completed** | pending |
| Positioned | `position(tp)` resolved for every assigned partition | (already completed) | **completed** |
| Position unresolvable | timeout / error | n/a | completed exceptionally, **that topic only** |

### Contract

- **VR-9** (FR-011): once a topic's readiness future completes successfully, a record published to that
  topic from that moment on is delivered to `onRecord`.
- **VR-10** (R3): a position failure fails only the awaiting topic's futures. It MUST NOT reach
  `markConsumerFailed`, which latches `consumerFailure` and poisons every present and future
  subscription for the run — the #143 terminal state.
- **VR-11**: readiness resolution stays on the poll thread. No other thread may touch the consumer.

---

## Entity relationships

```text
KafkaProtocolMessage ──── KafkaMatcher.requestMatch ────► Correlation Identity
   (single wire type)                                            │
        │                                                        │ matchKeyFor
        │ toProducerRecord                                       ▼
        │  (null key ⇒ round-robin partitioner)               MatchKey
        ▼                                                        │
   ProducerRecord                                                │ keys
                                                                 ▼
                                              sentMessages: HashMap[MatchKey, MessagePublished]
                                                                 │
                                                                 │ resolved by
                                                                 ▼
                                              Reply Message ◄── Reply Channel (readiness: positioned)
```

**Unchanged contracts**: `KafkaProtocolMessage` remains the single wire representation and
`KafkaMatcher` the single matching contract (Principle III). Neither is extended; no parallel type is
introduced. Every change is to how existing values are interpreted, not to what values exist.
