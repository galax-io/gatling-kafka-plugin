# Feature Specification: Request-Reply Reliability Hardening

**Feature Branch**: `003-request-reply-hardening`

**Created**: 2026-08-04

**Status**: Draft

**Input**: User description: "https://github.com/galax-io/gatling-kafka-plugin/milestone/2"

Milestone **v1.1.0 Request-reply reliability** exists to make request-reply usable outside a
single-user, start-at-t0 profile. Three of its issues have landed — non-blocking tracker acquisition
and the coalesced no-op subscribe (#163, #164, spec `001`), and per-request reply-channel churn
(#165, spec `002`). Four remain open, and together they are what still stands between the plugin and
a trustworthy request-reply run:

- **A reply the plugin received can be thrown away** (#191). Bookkeeping for a sent request and
  delivery of its reply travel two independent paths with no ordering between them. When the round
  trip is fast enough, the reply arrives first, finds no record of the request, and is discarded.
  The request then fails on its reply timeout, indistinguishable from a system under test that never
  answered.
- **A run that does not start request-reply immediately loses it entirely** (#143). The shared
  reply-receiving machinery starts a fixed initialization wait when the protocol is built, before
  any virtual user runs. If nothing has asked for a reply topic when that wait expires, the
  machinery tries to receive replies for nothing, fails, and refuses every reply topic for the rest
  of the run. Any ramp, warm-up, or delayed start longer than the wait is enough.
- **Retired reply channels keep working forever** (#166). Bookkeeping released when a channel goes
  idle leaves its periodic timeout watch running and its state reachable for the remainder of the
  run. Growth tracks every channel the run has *ever* used, which is precisely what releasing on
  idleness was introduced to avoid.
- **The gate that is supposed to catch all of this proves nothing** (#196). The CI simulation has no
  responder: its request-reply scenarios are "answered" by sibling fire-and-forget scenarios that
  happen to publish a matching key or value at a fixed delay. It exercises the matching code without
  ever exercising the round trip, and it is green because of timing rather than correlation.

The first three are defects a performance engineer experiences as unexplained failures, unexplained
memory growth, or a request-reply run that never worked. The fourth is why the project cannot
currently tell the difference.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A reported timeout always means the system under test did not answer (Priority: P1)

A performance engineer runs a request-reply scenario against a service that answers every request.
Every reply that service sent is matched to its request and reported as a success, with the round-trip
time it actually took. When the report shows a reply timeout, it means the service did not answer
within the configured timeout — never that the tool received the answer and dropped it.

**Why this priority**: The report is the deliverable. A received-then-discarded reply is reported
identically to a genuinely unanswered request, so the engineer cannot separate a defect in the system
under test from a defect in the tool — the one distinction a load test exists to preserve. It is
silent loss, not delay: nothing in the output hints that a reply was seen. It is also live today
under ordinary conditions (a sub-few-millisecond round trip and concurrent users), and it predates
this milestone rather than being introduced by it.

**Independent Test**: Run sustained concurrent request-reply load against a responder that answers
every request, with a round-trip time far shorter than the configured reply timeout so registration
and reply delivery genuinely race. Every request the responder answered must be reported as a
success.

**Acceptance Scenarios**:

1. **Given** a request whose reply arrives before the request has finished being recorded as
   awaiting one, **When** the reply is received, **Then** it is matched to that request and reported
   as a success with its true round-trip time — not discarded.
2. **Given** sustained concurrent request-reply traffic against a responder answering every request,
   **When** the run completes, **Then** no request is reported as a reply timeout.
3. **Given** a request that is genuinely never answered, **When** its reply timeout elapses, **Then**
   it is reported as a timeout exactly as it is today.
4. **Given** a reply topic whose channel has just been established, so the gap between sending a
   request and recording it is at its widest, **When** replies arrive during that gap, **Then** they
   are still matched — establishing a channel does not open a loss window.

---

### User Story 2 - Request-reply works no matter how far into the run it starts (Priority: P1)

A performance engineer's simulation ramps for several minutes, or runs a produce-only warm-up phase,
before its first request-reply request. That request is served, and so is every request after it. The
shared reply-receiving machinery does not fail merely because nothing needed a reply topic during the
opening minutes of the run.

**Why this priority**: The failure is total and delayed. The machinery fails around the initialization
wait, long before any virtual user asks it for anything, and every request-reply request afterwards
fails with an error that names nothing about the cause. Ramps, warm-ups, pacing and delayed starts are
ordinary load-profile shapes, not corner cases — and a protocol that merely *declares* reply settings
for scenarios that never do request-reply fails too, on machinery nobody uses.

**Independent Test**: Build the reply machinery, request no reply topic, let the initialization wait
elapse (parameterised so a test need not wait the production duration), then request a reply topic.
It must be served normally, and the machinery must have reported no failure at any point.

**Acceptance Scenarios**:

1. **Given** a simulation whose first request-reply request happens well after the initialization
   wait has elapsed, **When** that request is sent, **Then** it is served normally and matched to its
   reply.
2. **Given** reply machinery for which no reply topic has ever been requested, **When** the
   initialization wait elapses, **Then** no failure is reported and every later reply-topic request
   is still served.
3. **Given** a protocol that declares reply settings for a simulation that performs no request-reply
   at all, **When** the simulation runs to completion, **Then** no failure of the reply machinery is
   reported.
4. **Given** a reply topic requested while the initialization wait is still running, **When** it
   arrives, **Then** behaviour is unchanged from today, including a request landing exactly as the
   wait expires.

---

### User Story 3 - A long run holds only the reply channels it is still using (Priority: P2)

A performance engineer runs a long simulation whose reply topics or matching rules vary over time —
a reply topic derived per virtual user, or several matching rules across scenarios. Background
activity and retained state track the reply channels in use at that moment, not every channel the run
has ever opened.

**Why this priority**: Growth is proportional to the **total** number of reply channels created over
the run rather than the number concurrently active, so topic or matcher churn accumulates without
bound until the simulation ends. Each retired channel keeps a once-per-second timeout watch firing and
keeps its reporting, timing and matching state reachable. This directly undermines idle release, whose
entire purpose is to free resources when a topic goes quiet — the channel is let go while everything
hanging off it is kept.

**Independent Test**: Run a scenario that creates and retires many reply channels in sequence, with
reply timeouts configured so each one arms a timeout watch. After the idle grace has elapsed for all of
them, verify that background timeout activity and retained per-channel state correspond to the channels
currently held, not to the number created.

**Acceptance Scenarios**:

1. **Given** a reply channel released after going idle, **When** the release completes, **Then** its
   periodic timeout watch stops firing and its per-channel state is no longer retained.
2. **Given** a scenario that uses many distinct reply topics one after another, **When** the run
   proceeds, **Then** background timeout activity is bounded by the channels concurrently held rather
   than growing with the number of channels created.
3. **Given** requests still in flight on a channel, **When** other requests on that channel complete,
   **Then** the channel is not released, its timeout watch keeps running, and outstanding requests are
   still timed out and reported correctly.
4. **Given** a simulation ending with channels still held, **When** it shuts down, **Then** every
   channel and everything scoped to it is released, as today.

---

### User Story 4 - The CI simulation proves a real round trip (Priority: P3)

The project's request-reply gate answers each request with a responder that received it, so a green
run means the plugin correlated a reply with the request that caused it. The one scenario designed to
time out does so on a topic the responder does not serve, and the assertion names that request rather
than tolerating any single failure.

**Why this priority**: No user-facing behaviour changes here, so it ranks below the three defects. But
it is what makes the other three verifiable: today a request-reply scenario passes because a sibling
produce-only scenario publishes a matching key or value at a fixed delay, so the gate demonstrates
matching without ever demonstrating a round trip, and it cannot be tightened past "at most one failure"
because it cannot distinguish the expected failure from a new one.

**Independent Test**: Delete the sibling produce-only scenarios from the simulation and run it. Every
request-reply scenario must still pass — proof that its replies came from the responder rather than
from coincidence.

**Acceptance Scenarios**:

1. **Given** the request-reply scenarios in the CI simulation, **When** the simulation runs, **Then**
   each reply is produced by a responder answering the request it received, and no scenario depends on
   another scenario's publish for its reply.
2. **Given** scenarios that match by key and by value respectively, **When** replies come back, **Then**
   key and value are preserved end to end and the existing payload checks pass unchanged.
3. **Given** a responder reply, **When** the simulation inspects it, **Then** a response timestamp is
   available through a message header, leaving key and value untouched.
4. **Given** the scenario that is designed to receive no answer, **When** it runs on a request topic the
   responder does not serve, **Then** it times out, and the assertion fails the run both if that request
   succeeds and if any other request fails.
5. **Given** the topics the simulation uses, **When** it runs on the local stack and in CI, **Then** both
   topic lists contain all of them and the simulation is green in both.
6. **Given** a responder that answers only after receiving a request, **When** the simulation is run
   without the broker's zero rebalance-delay setting, **Then** whether that setting is still required is
   established by observation and recorded either way.

---

### Edge Cases

- **A reply for a request that already finished** — matched earlier, already timed out, or answered
  twice: discarded silently. Not reported as a failure, not attributed to an unrelated later request,
  not counted twice.
- **A reply arriving after its channel was released as idle**: discarded without producing a failure
  and without reviving the channel.
- **Two reply-tracking registrations on the same reply topic with different matching rules**: a reply
  is offered to both; only the one holding a matching pending request claims it and the other discards
  it silently. Neither registration's lifecycle affects the other's.
- **Two in-flight requests sharing one match key**: existing behaviour is preserved — the tracking key
  is derived from the match identifier, so requests that produce the same identifier are not
  distinguishable from one another and a reply resolves one of them. Making them distinguishable is not
  part of this feature.
- **Reply machinery fails while requests are pending**: each pending request is reported as failed with
  the underlying cause, and every background timeout watch stops rather than continuing to scan
  bookkeeping that has been abandoned.
- **Initialization wait expires, then the simulation ends without ever requesting a reply topic**:
  shutdown completes cleanly and no failure is reported for machinery that was never used.
- **A reply topic requested exactly as the initialization wait expires**: served, not lost to the race
  between the two.
- **A channel released as idle and later re-established for the same topic**: the new channel gets its
  own timeout watch and bookkeeping; nothing from the previous one is still running or still retained.
- **The last reply channel is released**: the receiving machinery must not be left with nothing to
  receive on, or its next attempt fails and takes the whole run's request-reply capability with it —
  the same failure mode as the initialization-wait path, reached from the other end.
- **A request in flight when the idle grace expires for its channel**: the channel is not released;
  release waits until nothing is in flight for a full grace period.
- **The CI responder cannot answer a request** (its send fails): the failure is visible in the run's
  output, because a responder that silently stops echoing is indistinguishable from the plugin losing
  replies.

## Requirements *(mandatory)*

### Functional Requirements

**Reply delivery (#191)**

- **FR-001**: A reply that arrives before the request it answers has finished being recorded as
  awaiting one MUST still be matched to that request and reported with its true round-trip time.
  Recording a sent request and delivering its reply MUST have a defined order relative to one another;
  a reply MUST NOT become discardable because bookkeeping performed on a different path has not yet
  been applied.
- **FR-002**: A reply that matches no pending request MUST continue to be discarded silently — not
  reported as a failure and not matched to an unrelated request. FR-001 MUST NOT be satisfied by
  retaining unmatched replies indefinitely in the hope that a request appears for them.
- **FR-003**: A reported reply timeout MUST mean that no reply for that request was received within
  its timeout. It MUST NOT be produced by the plugin discarding a reply it did receive.
- **FR-004**: FR-001 MUST hold on every path by which a request is recorded, including the first
  request on a newly established reply channel, where the gap between sending and recording is widest.

**Availability of the reply machinery (#143)**

- **FR-005**: The shared reply-receiving machinery MUST NOT fail because no reply topic was requested
  within its initialization wait, and MUST remain able to serve a reply topic requested at any later
  point in the run.
- **FR-006**: The machinery MUST distinguish "a reply topic was requested" from "the initialization
  wait expired with none requested", and MUST NOT attempt to receive replies while it has nothing to
  receive them for.
- **FR-007**: A protocol that declares reply settings for scenarios that never perform request-reply
  MUST run to completion without reporting a failure of the reply machinery.
- **FR-008**: The initialization wait MUST be configurable for testing, so a regression test can drive
  its expiry without waiting the production duration.

**Lifetime of per-channel work (#166)**

- **FR-009**: Releasing a reply channel MUST release everything scoped to it: its periodic timeout
  watch MUST stop firing, and its per-channel bookkeeping MUST stop being retained.
- **FR-010**: Background timeout activity and retained per-channel state MUST be bounded by the number
  of reply channels currently held, not by the total number created over the run.
- **FR-011**: A channel MUST NOT be released while requests are in flight on it, and releasing one
  channel MUST NOT disturb requests in flight on any other. Requests outstanding on a held channel MUST
  continue to be timed out and reported as they are today.

**Verification of the round trip (#196)**

- **FR-012**: Every request-reply scenario in the CI simulation MUST be answered by a responder
  replying to the request it received. No scenario's replies may be supplied by another scenario's
  publish.
- **FR-013**: The responder MUST preserve the request's key and value end to end, so key-based
  matching, value-based matching, and the existing payload checks all pass unchanged. Round-trip
  metadata — specifically a response timestamp — MUST be carried in a message header rather than in the
  key or value.
- **FR-014**: The scenario designed to receive no answer MUST use a request topic the responder does
  not serve, and the simulation's assertion MUST pin the expected failure to that request, failing the
  run both when that request unexpectedly succeeds and when any other request fails.
- **FR-015**: Every topic the simulation uses MUST be present in both the local stack's topic list and
  CI's, and the simulation MUST be green against both.

**Compatibility (all four)**

- **FR-016**: The published Scala and Java DSLs, protocol configuration options, defaults, and
  observable result semantics MUST remain unchanged. No new user-facing configuration option is
  introduced by this feature.
- **FR-017**: Reported response-time semantics MUST remain unchanged: a successful request-reply
  measures from message sent to reply received; a failed one spans request start to failure detection.

### Key Entities

- **Reply channel**: the plugin's ability to receive replies for one reply topic. Established on first
  use, held while in use and for an idle grace period after, released when that grace elapses or when
  the simulation ends.
- **Reply-tracking registration**: the per-(reply topic, matching rule) bookkeeping that correlates a
  sent request with its reply. Shares the reply channel's lifetime. Owns the pending-request records
  and the background timeout watch below — the subject of #166 is that today it does not let go of
  either when it is itself let go.
- **Pending-request record**: the record that one request has been sent and is awaiting a reply,
  carrying its match identifier, send time, reply timeout, checks and continuation. Written when the
  request is sent, read and removed when its reply arrives or its timeout elapses. The subject of #191
  is that a reply can be looked up before this record exists.
- **Background timeout watch**: the periodic activity, one per reply-tracking registration, that finds
  pending-request records past their reply timeout and reports them as failures.
- **Shared reply-receiving machinery**: the single receiving path serving every reply channel of a
  protocol. Started when the protocol is built, before any virtual user runs. Its failure ends
  request-reply for the whole run — the subject of #143.
- **Echo responder** (verification only): a stand-in for the system under test that replies to the
  request it received, preserving key and value. Belongs to the test simulations, not to the published
  plugin.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Under sustained concurrent request-reply load against a responder answering every request
  — at least 5,000 requests, with a round-trip time far below the configured reply timeout — zero
  requests are reported as reply timeouts. *Baseline*: 0–2 lost replies per ~6,760 requests across five
  runs on current code, and 14–17 per ~6,500 before idle release removed the amplifier that made each
  re-registration re-arm the next loss.
- **SC-002**: A regression test that reliably forces a reply to arrive before its request is recorded
  fails against the pre-change behaviour and passes after it — rather than depending on incidental
  timing to reproduce.
- **SC-003**: A simulation whose first request-reply request occurs after the initialization wait has
  elapsed completes with 100% of its request-reply requests served. *Today*: every one of them fails.
- **SC-004**: A simulation whose protocol declares reply settings but performs no request-reply
  completes with no failure of the reply machinery reported anywhere in its output.
- **SC-005**: In a run that creates and retires at least 20 reply channels in sequence, background
  timeout activity once all of them are idle past their grace corresponds to the channels still held —
  zero held, zero activity — rather than to the 20 created.
- **SC-006**: No per-channel bookkeeping for a channel released more than one idle grace period ago is
  still retained at any point in such a run.
- **SC-007**: Removing every produce-only scenario from the CI simulation leaves all of its
  request-reply scenarios passing — demonstrating that replies come from a responder rather than from a
  sibling scenario's coincidental publish.
- **SC-008**: The CI simulation's assertion fails the run in both directions around the one expected
  failure: it fails if that request succeeds, and it fails if any additional request fails.
- **SC-009**: Whether the CI simulation still requires the broker's zero rebalance-delay setting is
  determined by running it both ways and is stated explicitly in the change that lands the responder.
- **SC-010**: The complete existing verification suite — unit and integration tests, both CI Gatling
  simulations, and construction of every README and example simulation — passes unchanged.

## Assumptions

- **Scope is the four issues still open in milestone v1.1.0**: the dropped reply (#191), the poisoned
  reply machinery after the initialization wait (#143), the leaked per-channel timeout watch and
  bookkeeping (#166), and the CI simulation's coincidental matching (#196). The milestone's other three
  issues have landed under specs `001` (#163, #164) and `002` (#165) and are treated here as
  established behaviour to preserve, not to revisit.
- **Out of scope, and separately tracked**: consolidating the CI broker definition with the local Compose
  stack (#192); readiness meaning "positioned" rather than merely "subscribed", optional up-front
  declaration of reply topics, and moving the reported response-time clock to when the message is handed
  to the sending machinery (#193). The last of these would move reported percentiles and needs its own
  migration entry.
- **How the four fixes are structured is a planning decision, not a scope decision.** Each issue remains
  one issue, one commit, one PR under the project's rules; this specification states the behaviour each
  must produce and deliberately does not prescribe the mechanism. #191 in particular is left open on
  mechanism because it touches the core registration path rather than only the pool around it.
- **#191's fix must not be bought with unbounded retention.** Holding every unmatched reply until a
  request appears for it would trade a dropped reply for unbounded memory growth and for replies being
  matched to requests they do not answer. FR-002 stands as a constraint on FR-001's solution.
- **The duration of the initialization wait is not in scope.** #143 concerns what happens when the wait
  expires with nothing requested, not how long it lasts. Its production value is unchanged; only its
  configurability for tests is required (FR-008).
- **Idle release (#165) is in place and preserved.** #166's fix attaches to the same release point:
  idle release already bounds the number of channels; this feature makes what hangs off a channel share
  its fate. The two together are what issue #78 requires of a per-user reply topic.
- **The reply machinery must never be left with nothing to receive on.** The last subscription is kept
  deliberately, for the same reason #143 exists: a receiver with nothing to receive on fails and takes
  the run's request-reply capability with it. This feature must not reintroduce that state from either
  the startup end or the release end.
- **The echo responder is verification-only** and follows the shape the existing concurrency load
  simulation already uses. It does not become part of the published plugin and adds no dependency to it.
- **#196 and #192 touch the same files.** Both change the CI broker definition and the topic list it
  shares with the local Compose stack, so they are sequenced rather than developed in parallel. Either
  order works provided both topic lists stay identical.
- **Everything is validated against a real broker** — Testcontainers for the integration tests, the
  Compose stack for the Gatling simulations. Reply correlation, consumer lifetime and timeout handling
  are exactly the behaviours a stub reproduces incorrectly.
- **Each fix ships with a test that fails before it and passes after it**, per the project's
  test-first rule for behaviour change. For #191 and #143 that test must force the condition
  deterministically rather than waiting for it to occur by chance.
