# Feature Specification: Measurement Truth in Request-Reply

**Feature Branch**: `008-measurement-truth`

**Created**: 2026-08-24

**Status**: Draft

**Input**: User description: "https://github.com/galax-io/gatling-kafka-plugin/milestone/16" — milestone `v2.1.0 Measurement truth`, issues #227 and #228

## Context

A load test exists to produce numbers a reader can act on. Two figures this plugin reports for
request-reply are currently not true, and both are silent — nothing in a run distinguishes them from
an ordinary result.

| What the report says | What the reader concludes | What actually happened |
|---|---|---|
| A latency sample of ~0 ms, marked failed | The system under test answered instantly and wrongly | The request never left the JVM. The plugin rejected it while setting the request up. |
| `Reply timeout after N ms` | The system under test never answered | The service answered with a tombstone. The reply arrived, could not be correlated, and was dropped. |

Both were surfaced by the v1.2.0 review and deferred deliberately, because each needs a decision
about observable behaviour rather than a patch.

### 1. Outcomes that measure nothing are counted as measurements

Request-reply reports three outcomes that are decided **before** the record is handed to the
producer:

| Rejection | When it fires | Elapsed time reported |
|---|---|---|
| The configured matcher yields no correlation id (the guard added for #167) | Per request, while the request is being built | Near zero |
| The protocol declares no consumer settings | Per request, from the first one onward | Near zero |
| The reply channel could not be acquired | Per request, after the consumer-assignment wait | The whole assignment wait |

The first two measure nothing at all. They are session-setup rejections, and the request they
describe was never sent. The third measures a real wait, but a wait on the plugin's own reply
channel — the interval the success path deliberately excludes, and has excluded since 1.1.0 ("the
clock starts after the reply channel exists").

All three are reported as ordinary request entries, and that is on purpose: it is what keeps them
counted in the run's failed-request total. The obvious alternative — reporting them as run errors
instead — makes them invisible to that total, which would breach FR-005 of
`004-reply-correlation-correctness` ("a request that cannot be correlated must be visible as a failed
request"). Trading a distorted percentile for a silently missing failure is the wrong direction, so
these samples cannot simply be moved off the request path.

The distortion is not the one the issue title suggests. A near-zero sample does not raise a
percentile; it drags it toward zero. What is inflated is the **sample count**: the reported latency
population contains requests the system under test never saw. A simulation that misconfigures its
correlation id therefore reports a *better* p99 than a correct one, and an assertion written as
"p99 under N ms" passes on a run in which nothing was measured. `README.md` already warns about this
for the immediate-rejection path; the warning is documentation standing in for a fix.

### 2. A reply that arrived is reported as a reply that never came

Correlating on the record value derives the correlation id from the payload. A tombstone carries no
payload, so there is nothing to derive an id from, and the reply can never be matched to its
request. The plugin already sees this happening — it logs that the reply had no matcher key and
drops the record — but the request behind it goes on to fail on its reply timeout, which is
indistinguishable from a system under test that never answered.

This is the shape a compacted topic produces, which is exactly where tombstones are ordinary
traffic. Two capabilities shipped in v1.2.0 therefore do not compose: correlating on the value
(so that keyless request-reply works at all) and failing cleanly when a reply carries no payload.
The clean absent-payload failure is structurally unreachable on the value-correlated path, because
the reply never reaches its request in the first place.

**Neither problem is a defect introduced by a change. Both are limitations the v1.2.0 work made
visible, and the milestone exists to decide what the plugin should do about them.**

## Clarifications

### Session 2026-08-24

Three decisions were open. All three were settled by a read-only review of Gatling 3.13.5 itself,
because each of the candidate directions turned out to depend on a fact about Gatling that had been
assumed rather than checked. The three facts, each verified against the bytecode of the released
artifacts on disk:

| Fact | Evidence | What it rules out |
|---|---|---|
| The run-wide response-time distribution is fed **independently of the request name** — every request record updates it unconditionally, KO included | `gatling-charts-3.13.5.jar`, `GeneralStatsBuffers.updateRequestGeneralStatsBuffers`: four unconditional `update(responseTime)` calls, the third keyed `(None, None, None)` | Renaming a rejection cannot clean the run-wide aggregate. No option can. |
| The `responseCode` argument is **discarded before it is written** — the response serializer persists groups, name, start, end, an OK boolean, and the message, and never reads it | `gatling-core-3.13.5.jar`, `ResponseMessageSerializer.serialize0` | Filling the empty `responseCode` slot changes nothing any open-source run can display. |
| **No statistics entry point records a request-counted failure without also contributing a duration.** The only alternative writes error entries that are absent from the failed-request total, from the request table, and from every assertion path | `gatling-core-3.13.5.jar`, `DataWritersStatsEngine.logRequestCrash` → an error event carrying only a message and a timestamp | "Count it but skip the timing" does not exist and cannot be built. |

Also established: the HTML report and its data file already publish every metric three times — blended,
successful-only, and failed-only — so a person reading the report is not misled today. Only the blended
column and the assertion API are, and the assertion API has no successful-only scope for response time.
And the closest official analogue — the JMS request-reply action's own "no correlation id" failure —
reports exactly what this plugin reports today, so the current behaviour is the upstream norm rather
than a deviation from it.

- Q: How should a rejection be kept out of the request's latency percentiles, given it must stay a
  failed request with a specific error? → A: Report it under a derived request name. It is the only
  option with any effect: the response-code route is inert, and accepting the status quo leaves the
  per-request figure wrong where it can be made right. What it fixes is the per-request statistic and
  the request table; the run-wide aggregate is beyond reach and gets documented instead of promised.
- Q: Does the reply-channel acquisition failure belong in the same set as the two instant rejections?
  → A: Yes. All three share the one property that matters — no record reached the broker — and this is
  the one whose duration *inflates* rather than deflates, so excluding it is worth more than excluding
  the other two. Its measured wait is preserved under the derived name rather than discarded.
- Q: Is a missing consumer configuration really a per-request outcome? → A (raised during the review):
  No. It depends on no session data, so it is knowable before the run starts. Emitting one failed
  request per virtual user for a single static misconfiguration is the largest source of meaningless
  samples of the three, and the only one removable outright rather than relocatable.
- Q: What does v2.1.0 do about tombstone replies under value correlation? → A: Report the
  uncorrelatable replies so the failure names them instead of reading as a bare timeout, and document
  header correlation as the supported shape for such services. The matching contract is not extended:
  that is compatibility-bearing, and the same contract is reopened by #218 one milestone later.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A latency number describes only requests that were sent (Priority: P1)

A performance engineer runs a request-reply simulation and reads the reported response-time
percentiles for the request. Some requests in that run were rejected by the plugin before they
reached the broker — a feeder row carrying no key, or a reply channel that never became available.
The engineer needs the reported latency to describe the system under test and nothing else, while
still seeing every rejection counted as a failure. A colleague on the same team publishes without
ever waiting for a reply, and nothing about their simulation may change.

**Why this priority**: Latency percentiles are the headline output of every request-reply run and
the figure assertions are written against. A distorted percentile is worse than a missing one,
because it is acted on. This is also the smaller of the two changes and delivers value on its own.

**Independent Test**: Run a scenario in which a known number of requests are rejected before
publication and the rest complete normally, then compare the percentiles reported for the declared
request name against a run containing only the completing requests. The two must agree, the
rejections must appear under their own name, and the failed-request count must still include them.

**Acceptance Scenarios**:

1. **Given** a request-reply scenario where every request is rejected before publication, **When**
   the run finishes, **Then** the run reports those rejections as failed requests and makes no
   latency claim about that request name.
2. **Given** a scenario mixing rejected and completing requests, **When** the run finishes, **Then**
   the percentiles reported for the request name are those of the completing requests alone.
3. **Given** a run containing rejections, **When** the engineer reads the report without opening the
   log, **Then** they can tell which kind of rejection occurred — no correlation id, or reply channel
   unavailable — and that neither reached the broker.
4. **Given** a scenario containing a request-reply whose protocol carries no consumer configuration,
   **When** the simulation is started, **Then** it refuses to run and says why, rather than
   producing one failed request per virtual user.
5. **Given** a produce-only scenario whose protocol carries no consumer configuration, **When** the
   simulation is started, **Then** it runs exactly as it does today — publishing has never needed a
   consumer, and the refusal in scenario 4 must not reach it.
6. **Given** a scenario in which every request completes normally, **When** the run finishes,
   **Then** every reported number is what it was before this feature, under the same name.

---

### User Story 2 - A reply that arrived is never reported as one that never came (Priority: P2)

A performance engineer tests a service that answers on a compacted topic and correlates replies on
the message value. The service answers correctly, and some of its answers are tombstones. Today the
run reports reply timeouts, and the engineer investigates a responsiveness problem that does not
exist. The engineer needs the run either to correlate those replies, or to say plainly that replies
arrived which could not be matched.

**Why this priority**: This turns a working system under test into a false negative, which is the
most expensive kind of wrong number — it sends someone to debug the wrong system. It ranks below
US1 only because it affects one correlation strategy against one class of service, where US1 affects
the headline metric of every request-reply run.

**Independent Test**: Point a scenario that correlates on the value at a responder that answers with
tombstones, and confirm the run distinguishes that outcome from a responder that stays silent.

**Acceptance Scenarios**:

1. **Given** a value-correlated scenario against a service that answers every request with a
   tombstone, **When** the run finishes, **Then** no failure in it reads as a bare reply timeout —
   the report attributes the outcome to replies that arrived and could not be correlated.
2. **Given** the same scenario against a service that never answers at all, **When** the run
   finishes, **Then** the failures read as reply timeouts, distinguishable from the case above.
3. **Given** a documented correlation configuration for tombstone-answering services, **When** a
   scenario adopts it, **Then** every tombstone reply is correlated to its request and the request's
   checks run against it.
4. **Given** a scenario correlating on the message key, **When** a tombstone reply arrives, **Then**
   it correlates as it does today and the existing absent-payload failure applies to it unchanged.

---

### User Story 3 - An upgrading reader knows which of their numbers moved (Priority: P3)

An engineer upgrading a pinned suite reads the Migration Guide before the upgrade and can predict
what will change in their reports: which figures move, in which direction, and which scenarios are
affected. Nothing surprises them mid-run.

**Why this priority**: Required by the project's compatibility principle for any observable-behaviour
change, and cheap. It is P3 because it has no value until at least one of US1 and US2 lands, and
delivers none on its own.

**Independent Test**: Read the Migration Guide entry for this release against the list of behaviour
changes the feature made; every change is named, with its direction of movement.

**Acceptance Scenarios**:

1. **Given** the release notes and Migration Guide for this version, **When** an engineer reads the
   entry for it, **Then** every reported figure whose value can move is named, with the direction it
   moves and the scenarios affected.
2. **Given** a suite that compiled against the previous release, **When** it is rebuilt against this
   one, **Then** it still compiles, with no source change required.

---

### Edge Cases

- **A run in which every request is rejected**: the declared request name reports no samples at all.
  An assertion on its response time must not silently pass on an empty population, and the run-wide
  aggregate — which still contains the rejections — must not be presented as the safe alternative.
- **The run-wide aggregate remains blended**: Gatling feeds it independently of request name, so no
  choice available here cleans it. It is a documented limit, not a promise this feature can keep.
- **A rejection whose wait is long**: the reply-channel acquisition failure is the one rejection that
  inflates rather than deflates. Relocating it must preserve its measured wait, or the feature trades
  one wrong number for another.
- **A produce-only protocol carrying no consumer configuration**: the shape the DSL offers for
  publishing, used by simulations that never wait for a reply. It is valid, it is documented, and the
  refusal must not touch it. A scenario that only publishes never asks for a reply channel.
- **One protocol attached to every scenario at once**: a simulation can attach a produce-only
  protocol at the top level and still contain one request-reply scenario. The refusal then stops the
  whole simulation, including the produce-only scenarios that were fine — a wider blast radius than
  the per-request failures it replaces. Correct, because that request-reply could never have worked,
  but it must be stated in the Migration Guide rather than discovered.
- **An empty value under value correlation**: an empty payload is a *present* correlation id, and
  every request with an empty payload shares it — the same collision #167 fixed for keys, one field
  over. A tombstone and an empty payload must not be treated as the same case.
- **A custom extractor that reads the value**: an extractor returning nothing for a tombstone
  behaves exactly like value correlation and must be covered by the same outcome.
- **An uncorrelatable reply with no request pending**: third-party traffic on a held channel, a
  duplicate, or a reply for a request already timed out. It must not be attributed to any request
  and must not fail one.
- **A rejection under a load profile that reuses request names across groups**: whatever
  distinguishes a rejection in the report must survive grouping without colliding with a real
  request name a simulation could also declare.
- **Existing assertions on a named request's failure count**: these break by design. A rejection
  moves to a different name, so `details("<name>").failedRequests` stops counting it while
  `global.failedRequests` still does. This repository contains exactly one such assertion, in
  `KafkaFailureModesGatlingTest`, plus one unit spec asserting the rejection keeps the declared name;
  both encode the old contract and must be updated with the change, not worked around.
- **A rejection inside a group**: the derived name must remain distinguishable when the same
  declared name appears under several groups, and must not collide with a name a simulation could
  itself declare.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: A request-reply configuration that cannot work for any request, and that depends on no
  per-request data, MUST be refused before the run starts rather than reported once per request. The
  case this feature covers is a scenario that contains a request-reply whose protocol carries no
  consumer configuration.
- **FR-001a**: Absent consumer configuration is NOT invalid in itself — it is the produce-only shape
  the DSL offers and documents. A scenario that only publishes MUST be unaffected: it MUST build,
  start and run exactly as it does today, whether its protocol declares consumer settings or not.
- **FR-002**: A request-reply outcome decided before the record reaches the producer MUST NOT
  contribute a latency sample to the response-time statistics reported for the request name the
  simulation declared. Such outcomes MUST be reported under a derived name, distinct from the
  declared one and identifiable as a rejection.
- **FR-003**: FR-002 governs every outcome in which no record reached the broker: the matcher
  yielding no correlation id, and the reply channel failing to be acquired. The set is defined by
  that property, not by how long each one took.
- **FR-004**: A rejection MUST report the interval it actually took. A rejection that waited MUST
  report that wait under the derived name; a rejection that waited for nothing MUST report as much.
  Discarding a real wait to make a rejection uniform is not acceptable.
- **FR-005**: Every outcome governed by FR-002 MUST remain counted in the run's failed-request
  total and MUST keep the specific error text it carries today. Reporting it as a run-level error
  instead is not acceptable, because such entries are absent from that total.
- **FR-006**: The derived name MUST be constructed so that it cannot collide with a request name a
  simulation could itself declare, and MUST let a reader tell the rejection kinds apart from one
  another without reading the run log.
- **FR-007**: A request that was published MUST report exactly what it reports today — the same
  status, the same measured interval, the same failure text, under the same name.
- **FR-008**: A run MUST NOT describe a system under test as unresponsive when it answered. Where a
  request fails on its reply timeout on a reply channel that received replies it could not
  correlate, the reported failure MUST say so.
- **FR-009**: A documented, supported configuration MUST exist under which a service that answers
  with tombstones can be load-tested end to end, with the request's checks running against the
  reply. The single matching contract MUST NOT be extended to deliver it.
- **FR-010**: A tombstone reply and an empty-payload reply MUST remain distinguishable. An absent
  payload and an empty one are different findings and MUST NOT be collapsed into one outcome.
- **FR-011**: Each behaviour change this feature makes MUST be exercised against a real broker by a
  test that fails before the change and passes after it.
- **FR-012**: Each behaviour change MUST ship with a Migration Guide entry naming the reported
  figures that move and the direction they move in, in the same change that makes it. The entry MUST
  state plainly that the run-wide response-time aggregate still blends rejections in, and MUST
  direct the reader to the per-request figure and to the failed-request count instead.
- **FR-013**: No published Scala or Java signature may be removed or altered in a way that breaks a
  simulation compiled against the previous release.

### Key Entities

- **Reported request outcome**: one entry in the run's statistics. What a run can actually show of
  it is the request name, the start and end instants, whether it passed, and a failure message —
  those, and nothing else, survive to the report. It is the unit both problems are about, and the
  request name is the only field in it that can segregate one class of outcome from another.
- **Rejection**: an outcome in which no record reached the broker. Carries a cause and, sometimes, a
  real elapsed wait — but never a measurement of the system under test.
- **Correlation id**: the bytes the configured matching strategy derives from a record, on the
  request side and again on the reply side. Absent when the field it reads is absent.
- **Uncorrelatable reply**: a record received on a held reply channel whose correlation id cannot be
  derived, or can be derived but matches no pending request. Currently dropped with a log line and
  no report presence.
- **Reply channel**: the consumer and tracker serving one reply topic for one matching strategy;
  the scope within which a reply and a request find each other, and therefore the scope in which
  "replies arrived that nothing could match" is a meaningful statement.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A simulation containing a request-reply whose protocol carries no consumer
  configuration fails before any virtual user starts, and produces zero request entries — down from
  one per request issued.
- **SC-001a**: A produce-only simulation whose protocol carries no consumer configuration runs
  unchanged: same request count, same success rate, same reported figures as the previous release.
  Zero produce-only simulations regress.
- **SC-002**: In a run mixing rejected and completing requests, the response-time percentiles
  reported for the declared request name are identical to those of an otherwise-identical run in
  which the rejected requests were absent.
- **SC-003**: In a run where every request is rejected, the declared request name reports zero
  samples, and the run's failed-request count still equals the number of rejections.
- **SC-004**: A reader given only the run report, and no log, can correctly name which rejection
  occurred for 100% of rejected requests.
- **SC-005**: A rejection that waited for a reply channel reports that wait to the same precision it
  reports today — the measurement is relocated, not lost.
- **SC-006**: A run in which every request completes normally reports the same figures under the
  same names as the previous release — zero regressions on the measured path.
- **SC-007**: Against a service that answers every request with a tombstone under value correlation,
  0% of the run's failures read as a bare reply timeout, and 100% identify that replies arrived
  which could not be correlated.
- **SC-008**: Against a service that never answers, 100% of failures still read as reply timeouts —
  the two cases are never confused in either direction.
- **SC-009**: A documented configuration exists under which a tombstone-answering service is
  correlated successfully: 100% of tombstone replies matched to their request in a broker-backed
  harness run.
- **SC-010**: Every behaviour change in this feature has a Migration Guide entry naming the affected
  figure and its direction of movement, and the run-wide aggregate's remaining blend is stated
  explicitly rather than left to be discovered.
- **SC-011**: A simulation that compiled against the previous release compiles unchanged against
  this one, in Scala, Java, and Kotlin, verified by the three example consumer projects.

## Assumptions

- **Milestone scope is exactly issues #227 and #228.** The other open issues on this repository
  belong to later milestones and are out of scope, including the reply-channel redesign (#218),
  which puts the same matching contract on the table from a different direction.
- **The target release is `v2.1.0`, a minor.** Nothing published is removed, and a change that
  requires every implementer of a public contract to update is out of budget for this release unless
  it can be made source-compatible.
- **Reporting a rejection as a run-level error is off the table.** It removes the rejection from the
  failed-request total and breaches FR-005 of `004-reply-correlation-correctness`. Named here so it
  is not re-proposed as the obvious fix.
- **The run-wide aggregate cannot be cleaned by anything this feature does**, because Gatling feeds
  it without reference to the request name. Verified, not assumed. Truth is therefore delivered per
  request name, and the aggregate's remaining blend is documented rather than quietly left to be
  found.
- **The response-code slot is inert in open-source Gatling.** It is dropped before the run's data is
  written, so it cannot carry a rejection's identity to any report or assertion. This also means the
  failure types the plugin has been putting there since 2.0.0 are invisible in an open-source run —
  a separate finding, out of scope here, worth its own issue.
- **A missing consumer configuration is static, and only a request-reply cares.** Whether the
  protocol carries consumer settings depends on no session data, so a scenario that contains a
  request-reply can be refused before the run starts — removing the largest volume of meaningless
  samples at no cost. Publishing has never needed a consumer, so the produce-only shape the DSL
  documents is untouched by this and must be proved untouched, not assumed.
- **Tombstone handling is only a problem for value-derived correlation.** Key correlation and
  header correlation see a present id on a tombstone and match it normally; the existing
  absent-payload check failure then applies.
- **Behaviour is validated against a real broker**, per the project's testing principle, not against
  a stub of the consumer or tracker.
- **The layer boundaries hold**: one wire representation and one matching contract, extended rather
  than duplicated. A parallel matcher type for tombstones is not an acceptable shape.
- **Documentation-only is a legitimate outcome for either issue**, provided it is chosen rather than
  defaulted to, and recorded. Both issues say so explicitly.
