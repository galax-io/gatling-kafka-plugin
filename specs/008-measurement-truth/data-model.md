# Data Model: Measurement Truth in Request-Reply

**Feature**: `008-measurement-truth` | **Date**: 2026-08-24

This feature stores nothing and changes no serialized format. Its "data model" is the shape of what
a run reports and the small amount of state the tracker keeps to report it truthfully.

## 1. Reported request outcome

One row in a run's statistics. What actually survives to a report is fixed by Gatling and verified
in [research.md](./research.md) (C3): everything else passed to the statistics engine is discarded.

| Field | Survives to the report | Owned by | Changes in this feature |
|---|---|---|---|
| Request name | Yes | The action, from the simulation's declared name | **Yes** — derived for rejections (see §2) |
| Group hierarchy | Yes | The session | No |
| Start instant | Yes | The action or the tracker | No |
| End instant | Yes | The action or the tracker | No |
| Status (`OK`/`KO`) | Yes | The reporting site | No |
| Failure message | Yes | The reporting site | Timeout message only (see §4) |
| Response code | **No** — dropped before the run's data is written | The action | No. Deliberately untouched: it cannot carry anything. |

**Invariant preserved**: exactly one outcome per request, and every outcome goes through
`logResponse` with a real `KO`, so `global.allRequests` and `global.failedRequests` are unchanged by
this feature.

## 2. Request name

Two forms after this feature.

```
Declared name         := whatever the simulation passed to kafka("...")
Derived name          := "<declared name> [rejected: <kind>]"
kind                  := "no correlation id" | "no reply channel"
```

**Who reports which**

| Reporting site | Name used |
|---|---|
| The tracker, for any outcome of a request that was published — matched, checked, timed out, delivery-failed, channel-stopped, consumer-failed | Declared |
| The action, when the matcher yields no correlation id | Derived, kind `no correlation id` |
| The action, when the reply channel cannot be acquired | Derived, kind `no reply channel` |
| The action, when the protocol has no consumer settings | **None.** Does not reach a run — refused at build time (see §5) |

**Rule**: a derived name is used if and only if no record reached the broker. That is the whole
definition; duration plays no part in it.

**Consequences that are the point**

- `details("<declared>").responseTime` describes only requests that were sent.
- `details("<declared>").failedRequests` no longer counts rejections. `global.failedRequests`
  still does.
- Two new rows can appear in the requests table, each with a real duration distribution: near-zero
  for the correlation-id kind, the true acquisition wait for the channel kind.
- The run-wide response-time aggregate is unaffected by any of this — it is fed independently of
  request name (research.md C1).

## 3. Rejection

Not a type. A classification of an outcome, defined by one property: **no record reached the
broker**. It has a kind, a real measured interval, and the failure text it already carries today.

| Kind | Trigger | Interval | Message today | Message after |
|---|---|---|---|---|
| `no correlation id` | The configured matcher returns nothing for the request | Near zero, and reported as such | Names the matcher and the remedy (`KafkaRequestFailureMessages.missingCorrelationId`) | Unchanged |
| `no reply channel` | Acquisition fails — assignment timeout, pool shutting down, consumer already failed | The real wait | The underlying exception's message | Unchanged |

A rejection is never silent and never becomes a run-level error: those are absent from the
failed-request total (research.md C4), which FR-005 of `004-reply-correlation-correctness` forbids.

## 4. Uncorrelatable reply

A record received on a held reply channel whose correlation id the configured matcher cannot derive.
The canonical producer is a tombstone answered to a value-correlated request.

**Not** an uncorrelatable reply, and deliberately excluded: a record whose id *is* derivable but
matches no pending request. That is ordinary traffic — a duplicate, a late reply for a request
already reported, another scenario's reply on a shared channel — and the tracker has always been
silent about it.

### New tracker state

| State | Type | Scope | Lifetime |
|---|---|---|---|
| Uncorrelatable-reply count | Non-negative integer | One `KafkaMessageTracker`, i.e. one `(reply topic, matcher)` pair | The tracker's life; reset never, released with the tracker |

Actor state on a single-threaded actor, so it needs no synchronisation and adds no work to the reply
path beyond an increment on a branch that already exists and already logs.

### Effect on the timeout outcome

| Count | Reported failure message |
|---|---|
| Zero | `Reply timeout after <N> ms` — unchanged |
| Non-zero | The same, plus a clause naming how many replies arrived on this channel that the configured matcher could not correlate |

Only the periodic timeout scan's message changes. Delivery failures, consumer failures, channel
stops and check failures are untouched — a request that failed for a known reason must not be given
a second, speculative one.

## 5. Protocol configuration validity

| Condition | Depends on session data | When it is decidable | Reported as |
|---|---|---|---|
| Protocol has no consumer settings, and the scenario contains a request-reply | No | Scenario build, before any virtual user starts | A build failure that names the missing setting. Zero request rows. |
| Protocol has no consumer settings, and the scenario only publishes | No | — | **Nothing. Valid.** The produce-only shape the DSL documents; publishing never asks for a reply channel. |
| The matcher yields no correlation id for this request | Yes — the key or value is an expression over the session | Per request | A rejection (§3) |
| The reply channel cannot be acquired | Yes — depends on broker state at the time | Per request | A rejection (§3) |

The first row is why it moves out of the request path entirely: it is a property of the simulation,
not of any request in it. The second row is why the check lives in the request-reply builder and
nowhere else: the two rows differ only in what the scenario contains, and publishing reaches a
different builder that never reads the tracker pool. Absent consumer configuration is a supported
state — what is unsupported is a request-reply that needs one and has none.

## 6. Entities explicitly unchanged

Named because each was a candidate and each was rejected in [research.md](./research.md):

- **`KafkaProtocolMessage`** — the single wire representation. No field added, none reinterpreted.
- **`KafkaMatcher`** — the single matching contract. No member added; adding one breaks every
  external implementer, and #218 reopens this contract in `v2.11.0`.
- **`KafkaProtocol`** — no new setting. A toggle for the naming behaviour would be a permanent
  public surface whose default is the wrong number.
- **`KafkaRequestReplyAction`'s constructor** — a binary contract. The build-time check lives in the
  builder; the action asserts the same invariant once at construction.
