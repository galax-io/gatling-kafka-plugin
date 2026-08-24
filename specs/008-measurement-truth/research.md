# Research: Measurement Truth in Request-Reply

**Feature**: `008-measurement-truth` | **Date**: 2026-08-24 | **Spec**: [spec.md](./spec.md)

Phase 0 for issues #227 and #228, milestone `v2.1.0 Measurement truth`.

The spec's Clarifications already record the three Gatling facts that closed its open questions.
This document does not restate them as findings; it records the design decisions that follow from
them, and the ones the spec deliberately left to planning.

## Verified constraints carried forward

Each was confirmed against the bytecode of the released artifacts in `~/.m2/repository/io/gatling`,
not from documentation. They bound every decision below.

| # | Constraint | Where it was verified |
|---|---|---|
| C1 | Every request record updates the run-wide response-time digest, keyed `(None, None, None)`, with no status or name condition | `gatling-charts-3.13.5.jar` → `GeneralStatsBuffers.updateRequestGeneralStatsBuffers`, four unconditional `update(responseTime)` calls |
| C2 | Per-request digests are keyed by `(Some(name), group, status)` and `(Some(name), group, None)` — **the request name is the only field a plugin controls that segregates samples** | same method, first two calls |
| C3 | `responseCode` is never read by the response serializer; groups, name, start, end, an OK boolean and the message are all that reach `simulation.log` | `gatling-core-3.13.5.jar` → `ResponseMessageSerializer.serialize0` |
| C4 | `logRequestCrash` emits an error event carrying only a message and a timestamp — outside `failedRequests`, outside the request table, outside every assertion path | `gatling-core-3.13.5.jar` → `DataWritersStatsEngine.logRequestCrash` |
| C5 | The assertion API resolves `responseTime` against the blended bucket unconditionally; `TimeMetric` has exactly one case object and no status selector exists | `gatling-shared-model_2.13-0.0.11.jar` → `AssertionValidator.resolveTimeTargetActualValue` |
| C6 | Name decoration has upstream precedent: the default redirect naming strategy is `s"$requestName Redirect $redirectCount"`, a first-class user-overridable protocol setting | `gatling-http-3.13.5.jar` → `HttpProtocol$.$anonfun$apply$1` |

C2 and C3 together are the whole shape of this feature: **the request name is the only lever, and it
is the only lever.** C1 is why the spec promises per-request truth and documents the aggregate
instead of promising it.

## Decisions

### D1 — Refuse a missing consumer configuration in the action builder, not per request

**Decision**: `KafkaRequestReplyActionBuilder.build` resolves `KafkaComponents.trackersPool` and
fails there when it is empty. `KafkaRequestReplyAction` asserts the same invariant once, at
construction, instead of branching on it per request.

**Rationale**: `build` runs while the scenario is being materialised, before any virtual user is
injected, and both language surfaces reach it — `javaapi`'s `RequestReplyBuilder` wraps the same
Scala builder ([RequestReplyBuilder.java:12](src/main/java/org/galaxio/gatling/kafka/javaapi/request/builder/RequestReplyBuilder.java:12)),
so one check covers Scala, Java and Kotlin. The condition depends on protocol configuration and on
nothing else, so nothing is learned by waiting: today it produces one KO per request issued, which
is the single largest source of samples that measure nothing, and it produces them for a defect no
run can recover from.

**Why the check belongs in the request-reply builder and nowhere else.** Absent consumer
configuration is not an error — it is the produce-only shape the DSL offers and documents.
[`KafkaProtocolBuilder.properties(...)`](src/main/scala/org/galaxio/gatling/kafka/protocol/KafkaProtocolBuilder.scala:21)
constructs a protocol with `consumeSettings = Map.empty` by design, and its own scaladoc directs
readers to it for exactly that: *"Use `properties(...)` for a produce-only protocol."* Four
simulations in this repository and all three published `ProducerSimulation` examples are built that
way.

They are unaffected because publishing takes a different route end to end:
`kafka(name).topic(t).send(...)` builds a
[`KafkaRequestActionBuilder`](src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestActionBuilder.scala),
whose `KafkaRequestAction` uses `components.sender` and never reads `trackersPool`. Only
`kafka(name).requestReply…` reaches the builder this check lives in. Verified across the suite: every
`properties(...)` protocol in the repository is attached to produce-only scenarios only —
`KafkaGatlingTest` attaches protocols per scenario, and each of its request-reply scenarios gets a
`producerSettings(...).consumeSettings(...)` protocol.

**Blast radius this does widen, stated rather than glossed**: Gatling allows `.protocols(...)` at
the `setUp` level, applying one protocol to every scenario. A simulation that attaches a produce-only
protocol that way *and* contains one request-reply scenario goes from "one scenario reports KOs" to
"nothing runs at all". Correct — that request-reply could never have worked — but it is a wider
failure than the one it replaces, and the Migration Guide must say so.

**Alternatives rejected**:

- *Leave it per-request and relocate it under the derived name.* Correct but wasteful: it converts
  thousands of meaningless samples into thousands of meaningless samples under another name, when
  the run should not have started.
- *Change the action's constructor to take a non-optional pool.* Cleaner internally, but
  `KafkaRequestReplyAction` is public and its constructor is a binary contract (Constitution I). A
  construction-time precondition achieves the same removal of the per-request branch with no
  signature change.
- *Keep the `case None` branch as defence in depth.* It becomes unreachable, and Constitution III
  forbids merging dead code. The precondition is the honest replacement: it asserts the invariant at
  the layer that could violate it, once.
- *Refuse a protocol with no consumer settings wherever it is built.* Would break every produce-only
  simulation, including three published examples. Absent consumer configuration is a supported state;
  what is unsupported is a request-reply that depends on one and has none.

**Consequence**: [KafkaRequestReplyAction.scala:181-187](src/main/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyAction.scala:181)
loses its `case None` arm. Its message text moves to the builder and keeps its wording, which is the
only part of it any reader has seen.

### D2 — Segregate rejections by request name, one derived name per rejection kind

**Decision**: A request-reply outcome in which no record reached the broker is reported under
`<declared name> [rejected: <kind>]`, with exactly two kinds after D1:

| Kind | Derived name | Interval reported |
|---|---|---|
| The configured matcher yielded no correlation id | `<name> [rejected: no correlation id]` | The real interval, which is near zero |
| The reply channel could not be acquired | `<name> [rejected: no reply channel]` | The real acquisition wait |

Status stays `KO`, the failure message stays exactly what it is today, and `logResponse` stays the
entry point — so `global.failedRequests` and `global.allRequests` are unchanged.

**Rationale**: C2 makes the name the only lever, and C6 makes decorating it a precedented use of
that lever rather than an invention. Two names rather than one because the two kinds differ in
meaning (a simulation defect versus an infrastructure failure) and by three orders of magnitude in
duration — merging them produces a bimodal row that describes neither. Two names also discharge the
spec's requirement that a reader tell the kinds apart from the report alone, without relying on the
errors table.

**Alternatives rejected**:

- *Fill the `responseCode` slot instead.* C3: it is discarded before the run's data is written. It
  would change nothing an open-source run can display. (This is also a finding about existing
  behaviour — see [Out of scope](#out-of-scope) below.)
- *`logRequestCrash`.* C4: it removes the rejection from `failedRequests`, which breaches FR-005 of
  `004-reply-correlation-correctness`. It is what gatling-http does for its own pre-wire failures,
  and this project has already decided against it.
- *One derived name for all rejections.* Loses the distinction the spec requires and blends two
  incomparable durations.
- *A protocol setting to opt in.* A permanent public API surface (Constitution I) bought to make a
  correctness fix optional, defaulting to the wrong number. Rejected on principle: the default would
  be the behaviour the milestone exists to end.

**Collision risk, stated rather than assumed**: a simulation could declare a request named
`foo [rejected: no correlation id]`. Nothing can make that impossible — request names are free-form
strings, and Gatling's own redirect strategy carries the identical exposure. The bracketed suffix is
documented as reserved in the Migration Guide, and that is the whole mitigation. FR-006 is met in
the sense available: the marker is unambiguous to a reader and improbable by construction.

### D3 — Relocate the acquisition wait, do not flatten it

**Decision**: the acquisition-failure rejection reports its measured interval unchanged, under its
derived name.

**Rationale**: it is the one rejection whose duration *inflates* rather than deflates, so it is
worth the most to move — and it is real: a virtual user genuinely waited that long. Reporting it as
zero to make rejections uniform would trade one wrong number for another, which is the opposite of
this milestone. Under its own name the acquisition cost becomes readable for the first time, rather
than hidden inside a request's percentile.

**Consequence for the 1.1.0 note**: `README.md` currently says of this path that "reported results
are unchanged: the same KO, the same error message, and the same response-time span". The span and
the message stay true; the name does not. The Migration Guide entry must say so.

### D4 — Count uncorrelatable replies in the tracker, and name them in the timeout

**Decision**: `KafkaMessageTracker` counts the replies it receives whose correlation id the matcher
could not derive — the branch at
[KafkaMessageTracker.scala:297-300](src/main/scala/org/galaxio/gatling/kafka/client/KafkaMessageTracker.scala:297)
that logs `no messageMatcher key for read message` and drops the record. When the periodic timeout
scan fails a pending request and that count is non-zero, the reported failure says so instead of
reading as a bare timeout.

**Rationale**: the tracker is already the one place that sees both halves — the replies that could
not be correlated and the requests that timed out — and it is scoped to exactly one
`(reply topic, matcher)` pair, which is the scope in which "replies arrived that nothing could
match" is a true statement. No new type, no new collaborator, no change to `KafkaMatcher` or
`KafkaProtocolMessage` (Constitution III). The counter is actor state on a single-threaded actor, so
it needs no synchronisation.

**Scope of the count, deliberately narrow**: only replies whose id could not be *derived*. A reply
whose id is present but matches no pending request is ordinary — a duplicate, a late reply for a
request already reported, third-party traffic on a held channel — and the tracker has always been
silent about those. Counting them would put a diagnosis on healthy runs.

**Alternatives rejected**:

- *Extend `KafkaMatcher` with a fallback for absent fields.* Compatibility-bearing under
  Constitution I — an added abstract member breaks every external implementer, and the trait is
  public. #218 reopens the same contract one milestone later, so extending it here would be
  designing it twice. Settled in the spec's Clarifications.
- *Report the uncorrelatable reply as its own KO.* It belongs to no request; attributing it to one
  would be the cross-attribution failure #167 exists to prevent.
- *Fail at build time when value correlation meets a compacted reply topic.* Not knowable: the
  plugin does not read topic configuration, and a service on a non-compacted topic can answer with a
  tombstone anyway.

### D5 — Header correlation is the documented shape for tombstone-answering services

**Decision**: document `matchByMessage` over a correlation header as the supported configuration for
a service that may answer with tombstones, and prove it against a real broker rather than asserting
it in prose.

**Rationale**: the DSL already supports it and the README already shows the extractor
([README.md:391](README.md:391), compiled by
[ReadmeExamplesCompileOnly.scala:43](src/test/scala/org/galaxio/gatling/kafka/examples/ReadmeExamplesCompileOnly.scala:43)).
What is missing is the statement that it is *required* for this class of service, placed where a
reader hits the problem, and evidence that it actually correlates a tombstone. Both are cheap; the
API change that would make `matchByValue` work is not.

### D6 — Broker-backed coverage lands in the existing failure-modes harness

**Decision**: `KafkaFailureModesGatlingTest` gains the two scenarios this feature needs, and
`EchoResponder` gains the one capability they need.

**Rationale**: it already runs a real responder against a real broker, already answers `myTopic6`
with tombstones, and already exists to hold exactly this class of by-design failure. Its own
docstring is explicit that expected failures live there so a healthy positive suite prints nothing.
Adding a third harness would split coverage the reader has to reconcile.

**What has to change in the responder**: it builds fresh `RecordHeaders` carrying only
`x-responded-at` and drops the request's own headers
([EchoResponder.scala:80-88](src/test/scala/org/galaxio/gatling/kafka/examples/EchoResponder.scala:80)).
A header-correlated reply cannot be matched unless the responder echoes the correlation header, so
it must copy a named header from request to reply. Narrow, and it is what a real service does.

**Counts are pinned with `is(n)` in this harness by deliberate policy** — a new failure fails the
run and so does an expected one starting to pass. Every count this feature moves must be updated
with it, not loosened.

### D7 — Compatibility assessment: minor, with two report-shape changes

| Change | Kind | Verdict |
|---|---|---|
| Rejections reported under a derived name | Observable behaviour, no signature | Minor. Needs a Migration Guide entry and approval, which the spec's Clarifications record. |
| Missing consumer settings fails before the run | Observable behaviour, no signature | Minor, and strictly a failure that used to arrive later and louder. |
| Timeout message names uncorrelatable replies | Message text only | Minor. |
| Correlation header echoed by the test responder | Test-only | None. |

No published Scala or Java signature changes. No `!:` marker. `KafkaMatcher`, `KafkaProtocolMessage`,
protocol defaults and serialized formats are all untouched, so the three example consumer projects
compile unchanged.

**What breaks for a downstream user**: an assertion of the form
`details("<name>").failedRequests.count.is(n)` where `<name>`'s failures were rejections. It stops
counting them; `global.failedRequests` still does. This is the intended consequence of D2 and the
Migration Guide must lead with it.

## Blast radius in this repository

Established by reading every assertion on request statistics under `src/test` and `examples/`.

| Site | Assertion | Effect |
|---|---|---|
| [KafkaFailureModesGatlingTest.scala:193](src/test/scala/org/galaxio/gatling/kafka/examples/KafkaFailureModesGatlingTest.scala:193) | `details("Request Reply Keyless Key").failedRequests.count.is(3)` | **Breaks.** Must move to the derived name. |
| [KafkaRequestReplyActionSpec.scala:129](src/test/scala/org/galaxio/gatling/kafka/actions/KafkaRequestReplyActionSpec.scala:129) | rejection keeps the declared request name, "reported against the request, not swallowed" | **Breaks by design.** The spec's intent survives — the outcome is still reported — but the assertion now names the derived name. |
| `KafkaFailureModesGatlingTest.scala:190,191` | `global.failedRequests`, `global.allRequests` | Unchanged: `logResponse` and `KO` are retained. Would break only under `logRequestCrash`, which D2 rejects. |
| `KeylessCorrelationSpec.scala:197-244` | message text, response count, nothing reached the broker | Name-agnostic. Unaffected. |
| `KafkaRequestFailureMessagesSpec` | message construction only | Unaffected. |
| `KafkaGatlingTest`, `KafkaConcurrencyLoadTest`, `KafkaJavaapiMethodsGatlingTest` | `global.*` counts on runs with no rejections | Unaffected. |
| All nine example simulations across `examples/{scala,java,kotlin}` | `global.allRequests`, `global.successfulRequests.percent` | Unaffected — no example exercises a rejection, and none uses `details(...)`. |
| The no-consumer-settings rejection | — | **No test exists anywhere.** The literal appears once, in the action. D1 gives it its first coverage. |
| The acquisition-failure rejection | — | **No test asserts how it is reported.** `TrackerAcquisitionIsolationSpec` drives the pool's own failure callback, not the action's reporting. |

`RecordingStatsEngine` discards `responseCode` already
([RecordingStatsEngine.scala:36](src/test/scala/io/gatling/core/stats/RecordingStatsEngine.scala:36))
and no-ops `logRequestCrash`, which independently matches C3 and C4 — the unit-test double already
models what Gatling actually keeps.

## Out of scope

- **The `responseCode` slot is inert in open-source Gatling** (C3), so the exception-type failure
  codes this plugin has recorded since 2.0.0 never reach a report, and `README.md:605-609` claims
  otherwise. Real, verified, and not this milestone's issue. Filed separately.
- **The run-wide response-time aggregate** (C1, C5). Nothing available here cleans it. The
  Migration Guide states the limit and points at the per-request figure and `failedRequests`.
- **`KafkaMatcher`'s contract** — reopened by #218 in `v2.11.0`.
