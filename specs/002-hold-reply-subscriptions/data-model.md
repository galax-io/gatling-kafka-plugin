# Phase 1 Data Model: Run-Scoped Reply Channels

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md)

No persisted data; all entities are in-memory runtime state inside the `client` layer. The change
is almost entirely a *lifetime* change: two entities lose their mid-run removal transitions, and
the bookkeeping that existed only to drive those transitions is deleted.

## Entities

### TrackerRegistration (changed representation and lifetime)

The per-`(consumerTopic, matcher-identity)` entry in `KafkaMessageTrackerPool.trackers`
(`consumerTopic → (MatcherRef → value)` two-level `ConcurrentHashMap`).

| Field | Before | After |
|-------|--------|-------|
| `actor` | `TrackerEntry.actor: ActorRef[TrackerMessage]` | the map value itself: `ActorRef[TrackerMessage]` |
| `refCount` | `TrackerEntry.refCount: AtomicInteger` | **removed** — no reader remains |

`MatcherRef` (reference-identity key) is unchanged: distinct matcher instances stay isolated, one
protocol's shared matcher instance converges on one registration (spec Key Entities: "matching
rule").

**State transitions**:

```text
                    ┌──(establishment fails/times out: nothing was inserted — FR-006)──▶ Absent
Absent ──(slow path: readiness completes, compute get-or-create)──▶ Held
Held ──(any number of requests, completions, idle gaps)──▶ Held          (FR-001..FR-004)
Held ──(consumer failure: broadcast + trackers.clear())──▶ Cleared       (existing semantics)
Held ──(actor-system termination: pool torn down)──▶ Released            (FR-009)
```

Removed transitions: `Held ──(refCount hits 0)──▶ Absent` and the topic-level
`──▶ unsubscribe` side effect. "Establishing" is deliberately *not* a map state: the registration
is inserted only after readiness succeeds, so a failed establishment leaves the map untouched —
FR-006's "no partially-established channel" holds structurally.

**Validation rules**:

- Insertion happens only inside `trackers.compute` get-or-create; concurrent first uses converge
  on one actor, losers of the race never construct a second one (FR-005; unchanged from 001).
- Fast-path lookup is a plain read (`get` × 2): correct because no concurrent remover exists any
  more; the only wholesale clear (consumer failure) races benignly, exactly as today — a stale
  actor receives the failure broadcast or times out, per existing semantics.

### ReplyChannel / subscription set (changed lifetime)

The topic set `DynamicKafkaConsumer` is subscribed to, plus per-topic pending readiness
(`awaitingAssignment`).

**Per-topic states**:

```text
Unrequested ──(requestTopicSubscription)──▶ Queued ──(updateSubscription: subscribe())──▶ Subscribed
Subscribed ──(partitions assigned; completeAssignedReadiness)──▶ Assigned
Assigned ──(stays for the whole run)──▶ Assigned                          (FR-001, FR-007)
any ──(consumer failure / close)──▶ Terminal(failed readiness, consumer closed)
```

The subscription set is now **monotone within a run**: it only grows. Removed with the removal
machinery (research R4):

| Removed item | Was |
|--------------|-----|
| `topicsToRemove: ConcurrentLinkedQueue[String]` | pending-removal queue |
| `removeTopicSubscription(topic)` | producer of that queue (sole caller: `releaseTracker`) |
| `updateSubscription` removal handling | `-- toRemove` arithmetic, abandoned-readiness failure, `unsubscribe()`-when-empty branch |

Consequence for received traffic: an `Assigned` topic keeps delivering messages for the rest of
the run, including replies to already-completed requests and third-party traffic. Those flow to
`MessageConsumed` and are discarded by the matching step (below) — FR-008.

### PendingSubscription (unchanged, from 001)

`(topic, readiness: CompletableFuture[Void])` queue entries and `awaitingAssignment` parking are
untouched, including failure/close draining. Retry-after-failed-establishment (FR-006) is the
same mechanism as 001's R5: a later `acquireTracker` issues a fresh readiness request; an
already-subscribed topic completes it from the current assignment on the next poll cycle.

### MessagePublished (changed message shape)

Tracker-actor protocol message; loses its resource-management appendage.

| Field | Status |
|-------|--------|
| `matchId, sentTimestamp, replyTimeout, checks, session, next, requestName` | unchanged (FR-010: timestamp semantics untouched) |
| `onComplete: () => Unit` | **removed** — sole production purpose was pairing completion with `releaseTracker` (research R3) |

Completion of a tracked request (matched / timed out / consumer failure) now has **no resource
side effects**: it logs the response and passes the session on. `MessageConsumed`,
`ConsumerFailure`, `TimeoutScan` are unchanged.

**Unmatched-message rule (FR-008)**: `MessageConsumed` whose `responseMatch` yields no key or
whose key finds no `sentMessages` entry logs and discards — no stats entry, no `next` tell, no
state change. This behavior exists today; holding channels makes it hot, so it gains a pin-down
test.

## Relationships

```text
KafkaRequestReplyAction ──acquireTracker(onReady,onFailure)──▶ KafkaMessageTrackerPool
KafkaMessageTrackerPool ──requestTopicSubscription (first use only)──▶ DynamicKafkaConsumer
DynamicKafkaConsumer ──readiness──▶ (setup executor) ──▶ TrackerRegistration insert ──▶ onReady
KafkaMessageTracker (actor) ◀─MessagePublished (no onComplete)── onReady closure
KafkaMessageTracker ──(completion)──▶ statsEngine + next        [no pool/consumer interaction]
```

The dashed back-edge that existed before —
`KafkaMessageTracker ──onComplete──▶ releaseTracker ──▶ removeTopicSubscription` — is gone; that
edge *was* the defect.

`KafkaProtocolMessage`, `KafkaMatcher`, and the remaining tracker message protocol are not
modified — single wire representation and matching contract preserved (Principle III). The
published DSL, `javaapi`, protocol options, and defaults are untouched (FR-011).
