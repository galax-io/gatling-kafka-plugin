# Feature Specification: Reply Correlation Correctness

**Feature Branch**: `004-reply-correlation-correctness`

**Created**: 2026-08-07

**Status**: Draft

**Input**: User description: "https://github.com/galax-io/gatling-kafka-plugin/milestone/3"

Milestone **v1.2.0 Reply correlation correctness** collects the defects that make a request-reply run
report numbers that are *wrong rather than missing*. Its own summary is the shape of the problem:
results are silently wrong — replies attributed to the wrong virtual user, stalled users, and false
timeouts against a system that answered.

One of its four issues has landed: request-reply latency no longer excludes the produce round trip
(#170). Three remain, and each produces a report a performance engineer would read as a finding about
the system under test when it is actually an artefact of the tool:

- **Every keyless request is tracked as if it were the same request** (#167). When a request-reply
  request carries no key, the plugin substitutes an empty key, and an empty key is indistinguishable
  from an absent one in the correlation bookkeeping. Every keyless user therefore shares one slot, and
  each new request displaces the one before it. Since v1.1.0 a displaced request is at least failed
  rather than lost — but it is failed with an explanation about reusing a correlation id, which the
  engineer never did, and the request that *replaces* it goes on to be resolved by the displaced
  request's reply. So the surviving defect is a reply credited to a virtual user that did not send the
  matching request, alongside an advisory failure that misdescribes its own cause. The same
  substitution also puts a *present* key on the wire where there should be none. Kafka hashes it, and
  `murmur2` of an empty input is a constant, so every keyless message lands on one partition for the
  whole run — and a log-compacted topic, which requires a key, silently accepts records it should have
  rejected.
- **A reply with no payload hangs the virtual user that receives it** (#168). Body checks against a
  reply whose payload is absent — routine on compacted topics, and legitimate for any service that
  answers with a deletion marker — fail in a way the reply-handling path does not catch. The virtual
  user is never continued: no success, no failure, no next request. It simply stops, and the run's
  reported user count silently diverges from the load actually applied.
- **A reply channel reports ready before it can receive** (#193). Readiness is declared when the
  broker has assigned partitions, which happens before the channel has resolved where in each
  partition to read from. The plugin reads from the end of the topic by default, so a reply produced
  in that window is skipped and its request fails on the reply timeout — indistinguishable from a
  system under test that never answered. Today this is not fixed but *masked*, by a broker-side
  rebalance-delay setting present in this project's own Compose stack and CI and absent from any real
  target system. The project therefore cannot currently detect the defect it is shipping.

The first is a correctness defect that misattributes results between users. The second turns a normal
message into a lost virtual user. The third manufactures failures against a healthy system. All three
are invisible in the report: nothing in the output distinguishes them from a genuine finding.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A reply is always reported against the user that sent the request (Priority: P1)

A performance engineer runs a request-reply scenario in which requests carry no key. Many virtual users
run concurrently. Where the scenario correlates on something the request actually carries — a
self-describing payload, or a header the service echoes — every reply is attributed to the virtual
user whose request it answers and to no other. Where the scenario correlates on the key it never set,
the engineer is told so immediately and in those terms, instead of receiving results in which one user
is credited with another's reply.

**Why this priority**: This is the defect that makes the report actively misleading rather than
incomplete. A misattributed reply corrupts two measurements at once — a false success for one user and
a false timeout for another — and both look exactly like real results. Every other number in the run
is downstream of correlation being right.

**Independent Test**: As a load simulation in CI against a real broker and the echo responder — run
concurrent keyless request-reply virtual users correlating on a non-key field, and assert that the
count of successful responses equals the count of requests, that no request is reported as timed out,
and that no reply is reported against a user that did not send it. Separately, run the same scenario
correlating on the key and assert every request fails at issue time with a reason naming the missing
key. Concurrency is essential: a one-user-at-a-time profile cannot expose this defect.

**Acceptance Scenarios**:

1. **Given** two or more virtual users concurrently issue request-reply requests that carry no key,
   **When** the system under test answers each of them, **Then** each virtual user is reported with
   the outcome of its own reply and none is reported as timed out.
2. **Given** a keyless request-reply request is in flight, **When** a second keyless request is issued
   before the first has been answered, **Then** the first request remains tracked and is still able to
   be matched by its own reply.
3. **Given** a request-reply scenario whose requests carry no key and whose configured matching
   strategy correlates on the key, **When** the scenario runs, **Then** each such request is reported
   as a failure naming the missing identity as the cause, at the moment it is issued rather than after
   a reply timeout, and no wrong match is ever reported.

---

### User Story 2 - A reply with no payload fails cleanly instead of stopping the user (Priority: P2)

A performance engineer runs a request-reply scenario against a service that can answer with an empty
payload — a deletion marker on a compacted topic, or an acknowledgement carrying no body. The scenario
applies a body-content check to every reply. When such a reply arrives, the check fails, the request is
reported as a failure with a reason the engineer can read, and the virtual user continues to its next
request. The run finishes with the number of users it started with.

**Why this priority**: A stalled virtual user is worse than a failed one. It removes load the profile
was supposed to apply without recording anything, so the achieved throughput no longer matches the
configured throughput and nothing in the report explains the gap. Reply-content checks are the normal
way request-reply scenarios are written, so any target that emits empty payloads triggers this.

**Independent Test**: As a load simulation in CI against a real broker — run a request-reply scenario
with a reply-content check against a responder that answers with an absent payload, and assert both
that every request is reported as a failure with a stated reason and that every virtual user reaches
the end of its scenario. The second assertion is the one that catches the defect: a stalled user
produces no failure, so a failure-count assertion alone would go green on a hung run.

**Acceptance Scenarios**:

1. **Given** a request-reply request with a check on the reply's textual content, **When** the reply
   arrives with no payload, **Then** the request is reported as a failure whose message identifies the
   missing payload and the virtual user proceeds to its next action.
2. **Given** the same scenario using a structured-content check instead of a textual one, **When** the
   reply arrives with no payload, **Then** the outcome is the same failure-and-continue behaviour, not
   a different one per check type.
3. **Given** a run in which every reply has no payload, **When** the run completes, **Then** the number
   of virtual users that finished equals the number that started.

---

### User Story 3 - A reported timeout always means the system did not answer (Priority: P3)

A performance engineer starts a run whose request-reply traffic begins immediately, against a service
that answers every request well within the configured reply timeout. No request is reported as timed
out. The engineer can treat a reply timeout in the report as evidence about the system under test,
including on the very first requests of a run and in any environment, without needing to tune the
broker to make the tool behave.

**Why this priority**: This defect fabricates failures rather than misattributing them, and it is
concentrated at the start of a run where it is easiest to dismiss as warm-up. It is ranked below the
first two because its blast radius is narrower — a bounded window at channel start rather than every
keyless request or every empty reply — but it is the one the project currently cannot see at all,
because the environment that would reveal it is configured to hide it.

**Independent Test**: As a load simulation in CI — run request-reply traffic that starts at the very
beginning of the run against the echo responder, in an environment using the broker's default
group-rebalance behaviour with the test-only tuning removed, and assert zero unexpected reply
timeouts. Removing that tuning is part of the test: with it in place the simulation passes whether or
not the defect is fixed.

**Acceptance Scenarios**:

1. **Given** a reply channel is being established for a topic, **When** the plugin reports that channel
   as ready, **Then** a reply published from that moment onward is received and matched.
2. **Given** the project's own automated verification, **When** it runs against a broker configured
   with default group-rebalance behaviour and no test-only tuning, **Then** request-reply scenarios
   answered by a real responder report zero reply timeouts.
3. **Given** a run whose first request-reply request is issued as early as the profile allows,
   **When** the system under test answers it within the reply timeout, **Then** it is reported as a
   success and not as a timeout.

---

### User Story 4 - Keyless traffic spreads across partitions like a real producer's (Priority: P4)

A performance engineer runs a throughput scenario that publishes keyless messages to a multi-partition
topic, in order to measure how the system under test scales across partitions and consumer-group
members. The messages are distributed across all of the topic's partitions, the way a message with no
key is distributed by any ordinary producer, so the run measures the partitioned system the engineer
intended to test.

**Why this priority**: This is realism rather than correctness — the reported numbers are true, but
they describe a single-partition workload the engineer did not ask for. It is ranked last because it
misleads about *what was tested* rather than about *what happened*, and because it shares a root cause
with User Story 1 and will follow from it.

**Independent Test**: As an integration test against a real broker — publish keyless messages through
the DSL, read them back, and assert every record carries no key. The assertion has to read the records
themselves; the defective behaviour raises no error. Placement is deliberately not asserted: it is the
broker's decision and changed in Kafka 3.3, so pinning it would test the broker rather than the plugin.

**Acceptance Scenarios**:

1. **Given** a scenario publishes messages that carry no key, **When** those records are read back
   from the broker, **Then** every one of them carries an absent key rather than an empty one — which
   is what lets the broker apply its keyless placement at all.
2. **Given** a scenario that publishes messages which do carry a key, **When** it runs, **Then** the
   existing per-key partition placement is unchanged, so keyed ordering guarantees still hold.

---

### Edge Cases

- Two keyless request-reply requests are in flight at once and both replies arrive nearly
  simultaneously — each must resolve its own request, and neither may resolve the other's.
- A request is failed for colliding with another request's correlation identity — the reported reason
  must distinguish an identity the scenario genuinely reused from one the scenario never supplied,
  because the corrective action differs and only the first is a duplicate.
- A request supplies a key that resolves to an empty value, distinct from supplying no key at all —
  the two must not be treated as the same request, and the distinction must be stable across both the
  Scala and Java surfaces.
- A reply arrives carrying no key, no payload, or neither, on a channel where some requests are keyed
  and others are not.
- A reply arrives for a request that has already been reported as timed out — the late reply must not
  resurrect the request or be attributed to a different one.
- A body check is applied to a reply whose payload is present but empty, as opposed to absent — an
  empty payload is a value and must keep its current behaviour.
- The broker reassigns partitions mid-run, so a channel that was ready becomes unready and ready again
  — readiness must be re-established on the same terms, not assumed to persist.
- A reply channel is established for a topic that has no messages yet and no committed position.
- A run applies a ramp or a delayed start, so the first request-reply request is issued long after the
  protocol was built.

## Requirements *(mandatory)*

### Functional Requirements

**Correlation identity**

- **FR-001**: The plugin MUST distinguish a request that supplies no key from a request that supplies
  an empty key, and MUST NOT treat the two as the same request for correlation purposes.
- **FR-002**: Concurrent request-reply requests that each supply a distinct correlation identity MUST
  remain independently tracked; issuing one MUST NOT displace or resolve another, and supplying no key
  MUST NOT be what makes two such requests collide.
- **FR-003**: A reply MUST be reported only against the request it answers. The plugin MUST NOT report
  a reply against any other request under any timing of arrival.
- **FR-004**: When a request-reply request cannot be correlated to a reply under the configured
  matching strategy — because it supplies no identity that strategy can match on — the plugin MUST
  report that request as a failure at the moment it is issued, rather than send it and produce a match
  that may be wrong or a timeout that misrepresents the system under test.
- **FR-005**: The failure reported under FR-004 MUST name the reason — that the request supplies no
  identity for the configured matching strategy — so an engineer can act on it directly rather than
  infer it from timeouts.
- **FR-006**: The plugin MUST NOT invent an identity on the engineer's behalf for a request that
  supplies none. Nothing may be added to a message that the scenario did not ask to be there.

**Absent payloads**

- **FR-007**: A check against the content of a reply that carries no payload MUST produce a reported
  failure for that request, with a message identifying the absent payload as the cause.
- **FR-008**: A reply that carries no payload MUST NOT prevent the virtual user from continuing to its
  next action. Every request MUST reach a terminal reported outcome — success, failure, or timeout.
- **FR-009**: All reply-content check types MUST behave consistently for an absent payload; a given
  reply MUST NOT succeed under one check type and stall the user under another.
- **FR-010**: A reply whose payload is present but empty MUST retain its existing behaviour and MUST
  NOT be reclassified as a failure by this change.

**Channel readiness**

- **FR-011**: The plugin MUST NOT report a reply channel as ready until a reply published from that
  moment onward would be received by it.
- **FR-012**: A reply published after a channel is reported ready MUST be delivered to correlation,
  irrespective of how quickly it follows the request.
- **FR-013**: Reliable reply delivery MUST NOT depend on non-default broker configuration. The
  plugin MUST behave correctly against a broker in its default state, since a target system is not
  something a performance engineer can retune to accommodate the tool. (The matching verification
  obligation is FR-022.)

**Partition distribution**

- **FR-014**: Messages that supply no key MUST be published with an absent key, not with an empty one,
  so that the broker applies its keyless placement strategy instead of hashing a constant. How records
  are then distributed is the broker's decision and MUST NOT be specified here — it varies by client
  version, and pinning it would test the broker rather than the plugin.
- **FR-015**: Messages that supply a key MUST retain their existing partition placement, so per-key
  ordering guarantees relied on by existing scenarios are unaffected.

**Compatibility**

- **FR-016**: The published Scala DSL and Java facade MUST keep compiling unchanged for existing
  scenarios; this feature MUST NOT require a source change to a scenario that does not rely on the
  defective behaviour.
- **FR-017**: Any change to observable outcomes for an existing scenario — a keyless request-reply
  request that previously reported a match and now reports a failure, a reply with no payload that
  previously stalled its user and now fails it, and keyless messages moving off a single partition —
  MUST ship with a Migration Guide entry describing what changes, what an engineer should expect to
  see differently in their results, and how to restore a working scenario in each case.

**Verification**

- **FR-018**: Every user story in this specification MUST be proven by a load simulation in the
  project's own automated verification, running against a real broker and a real responder in CI.
  Unit-level assertions alone MUST NOT be accepted as proof for any of them, because every defect in
  this feature is a property of a concurrent run rather than of a function.
- **FR-019**: Correlation coverage MUST inject more than one virtual user concurrently into the same
  reply channel. A single-user profile cannot observe a reply being attributed to the wrong user, so
  it MUST NOT be relied on as evidence for User Story 1.
- **FR-020**: Request-reply with requests that supply no key MUST be covered by a simulation, both in
  the form that correlates on a non-key field and in the form that correlates on the key. Existing
  keyless coverage is produce-only and MUST NOT be counted as request-reply coverage.
- **FR-021**: Replies carrying no payload MUST be covered by a simulation in which the responder
  answers with an absent payload, asserting both the resulting failure count and that every virtual
  user reaches the end of its scenario.
- **FR-022**: Readiness coverage MUST run against a broker using its default group-rebalance
  behaviour. The test-only rebalance tuning MUST be removed from every broker definition the project
  controls, so that CI itself is what proves the gap is closed rather than what hides it.
- **FR-023**: Key absence on the wire MUST be asserted by reading published records back and inspecting
  their keys, rather than inferred from the absence of errors — the defective behaviour raises none.
  The assertion MUST fail rather than pass if fewer records are read back than were published.
- **FR-024**: Simulation assertions MUST pin exact expected counts in both directions, following the
  convention already established in the project's verification: a scenario that stops failing MUST
  fail the run exactly as a new failure does. Upper-bound-only assertions MUST NOT be used for any
  criterion in this feature.
- **FR-025**: Each simulation added or amended for this feature MUST fail against the behaviour that
  exists before the change and pass after it, and that ordering MUST be demonstrated rather than
  assumed.

### Key Entities

- **Request-Reply Exchange**: One request sent by one virtual user and the single reply that answers
  it. Carries the identity used to correlate the two, the moment the request was handed off, and the
  deadline past which it is reported as timed out. Must belong to exactly one virtual user for its
  whole life.
- **Correlation Identity**: The value that connects a reply back to its request under the configured
  matching strategy. Must be able to represent "this request has no identity to correlate on" as a
  state distinct from "this request's identity is empty", because those two lead to different
  reported outcomes.
- **Reply Channel**: The shared means by which replies for one topic are received on behalf of every
  virtual user awaiting one. Has a readiness state that, once reported ready, promises delivery of
  everything published from that point on.
- **Reply Message**: A message received on a reply channel. Its identity and its payload are each
  independently allowed to be absent, and each absence has a defined reported outcome.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In a run of at least 20 concurrent keyless request-reply virtual users correlating on a
  non-key field against a responder that answers every request, 100% of requests are reported with the
  outcome of their own reply, 0 are reported as timed out, and 0 replies are reported against a user
  that did not send the matching request.
- **SC-002**: Across the same run, the number of reported successes plus failures equals the number of
  requests issued — no reply is counted twice and none is unaccounted for.
- **SC-003**: In a run whose requests supply no key while correlating on the key, 100% of requests are
  reported as failures naming the missing key, 0 are reported as successes, and 0 are reported as
  timeouts — the outcome is immediate and identical for every request rather than dependent on timing.
- **SC-004**: In a run where every reply carries no payload and every request has a reply-content
  check, 100% of requests are reported as failures with a stated reason and 100% of virtual users
  reach the end of their scenario.
- **SC-005**: Request-reply verification passes against a broker using default group-rebalance
  behaviour with 0 reply timeouts, where the same verification against the current behaviour fails.
- **SC-006**: A reported reply timeout corresponds to a request the system under test did not answer
  within the configured window in 100% of observed cases.
- **SC-007**: In a run publishing at least 20 keyless messages, 100% of the records read back from the
  broker carry no key, and the count read back equals the count published.
- **SC-008**: Every simulation in the project's examples and documentation continues to compile and
  run against the released interface without source changes.
- **SC-009**: Each of the four user stories has at least one simulation assertion in CI that fails
  against the pre-change behaviour and passes after the change — 4 of 4 stories covered, 0 relying on
  unit-level assertions alone.
- **SC-010**: The full CI verification passes with the test-only rebalance tuning removed from every
  broker definition the project controls, and 0 such settings remain.
- **SC-011**: Request-reply simulation coverage includes at least one scenario running concurrent
  virtual users against a shared reply channel, where the pre-change behaviour produces at least one
  misattributed or timed-out request and the post-change behaviour produces none.

## Assumptions

- **Scope is the three open issues in milestone v1.2.0** — #167 (keyless correlation and partition
  distribution), #168 (absent payloads in reply-content checks), and #193 (channel readiness meaning
  positioned rather than merely assigned, and removing the broker-side rebalance-delay setting that
  masks it from this project's Compose stack and CI). Issue #170 in the same milestone has already
  landed and is not revisited here.
- **A request that supplies no identity to correlate on fails, and the plugin does not invent one.**
  Decided during specification (FR-004 to FR-006). The alternatives considered were generating a
  correlation identity per request, and rejecting the scenario before the run starts. Generating one
  was rejected because it puts a key on the wire the scenario never asked for: the system under test
  sees it, the message becomes keyed and so hashes to a single partition, contradicting FR-014, and
  matching then depends on the target echoing a value it was never told about. Build-time rejection
  was rejected as a *replacement* because keys are session-derived and their absence is often knowable
  only at runtime, so a runtime path is required regardless; it remains available as an addition
  during planning if the absence is statically detectable.
- **This is a deliberate, breaking behaviour change for keyless key-matched scenarios.** Such
  scenarios report matches today and will report failures after this change. Those matches are the
  defect, so the change is a correction rather than a regression — but it is observable, so it is
  governed by Constitution Principle I and carries the Migration Guide obligation in FR-017.
- **An absent payload is a failure, not an empty value.** A reply-content check applied to a reply with
  no payload is treated as a failed check rather than as a check against an empty string. This follows
  #168's stated intent and keeps a deliberate check honest; scenarios that want to accept such replies
  are expected to express that intent explicitly rather than have it inferred.
- **Partition distribution applies to every send, not only request-reply.** Restoring ordinary
  no-key partition behaviour changes fire-and-forget scenarios as well. This is treated as correcting
  the plugin's realism rather than as a new capability, and is covered by the Migration Guide
  requirement in FR-016.
- **Reply topics stay dynamically discovered.** Reply topics may depend on session data and cannot be
  harvested before a run starts, so this feature makes readiness *correct*, not unnecessary. A
  protocol-level declaration of reply topics for warm-up is explicitly out of scope — #193 records it
  as an optimisation with no defect behind it.
- **The v1.1.0 baseline is assumed, not re-solved.** A request displaced from a correlation slot is
  already failed explicitly rather than silently lost, and a request is already registered before it is
  handed to the producer. This feature builds on both. What remains of #167 is that keyless requests
  share one slot at all, that the resulting failure describes a reuse the engineer did not commit, and
  that the request which replaces a displaced one can still be resolved by the displaced one's reply.
- **The correlation table stays where it is.** Moving correlation state out of the tracker is out of
  scope: #193 records that register-before-send plus mailbox ordering already establish the required
  happens-before, and that work landed with milestone v1.1.0.
- **Verification uses a real broker and a real responder, at simulation level.** The echo responder
  introduced in #196 is the oracle for the readiness and correlation criteria, and every user story is
  proven by a load simulation in CI rather than by unit assertions (FR-018 to FR-025). Two gaps in the
  current verification are known and in scope to close: request-reply is exercised only one virtual
  user at a time, which structurally cannot observe a reply attributed to the wrong user; and the
  existing keyless coverage publishes without expecting a reply, so it exercises no correlation at
  all. The convention of pinning exact failure counts in both directions is already established in the
  project's verification and is adopted here rather than invented.
- **Existing measurement semantics are unchanged.** Request-reply latency is measured from the moment
  the request is handed to the producer, as decided and shipped with #170. Nothing in this feature
  moves the reported clock.
- **No new dependency is expected**, and no change to the published Scala DSL or Java facade
  signatures is expected. If the resolution of FR-004 requires either, it is an API-surface decision
  requiring approval before implementation.
