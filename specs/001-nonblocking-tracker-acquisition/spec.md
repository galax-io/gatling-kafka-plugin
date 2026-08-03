# Feature Specification: Non-blocking Reply-Tracker Acquisition for Request-Reply Sends

**Feature Branch**: `001-nonblocking-tracker-acquisition`

**Created**: 2026-08-03

**Status**: Draft

**Input**: User description: "https://github.com/galax-io/gatling-kafka-plugin/issues/163"

Issue #163: preparing reply tracking for a request-reply operation (acquiring a tracker for the
reply topic, which on first use waits for the reply channel to become ready) currently runs on the
shared delivery-notification path of the message producer. That path serves every in-flight send in
the simulation. While one operation waits — up to the configured reply timeout, 60 seconds by
default — delivery confirmations for **all** other sends stall, inflating measured latencies and
cascading into spurious send timeouts on unrelated traffic. In a tool whose purpose is accurate
latency measurement, this makes results wrong exactly when the system is under the load it exists
to measure.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Slow reply-channel setup on one topic must not stall other traffic (Priority: P1)

A performance engineer runs a simulation containing request-reply scenarios against several topic
pairs, all sharing one message producer. Reply-channel preparation for one topic is slow (e.g. the
reply consumer takes long to become ready). Requests on every other topic must keep flowing:
their sends are confirmed promptly, their replies are matched, and their reported latencies are
unaffected by the slow topic.

**Why this priority**: This is the defect itself. One slow topic today freezes delivery
confirmations for the whole simulation, corrupting every measurement and causing spurious
failures on unrelated scenarios. Fixing it restores the core promise of the tool: numbers that
reflect the system under test, not the plugin's internals.

**Independent Test**: Run a simulation with request-reply on two topic pairs where reply-channel
readiness for topic B is artificially delayed close to the configured timeout. Verify topic A's
requests complete with the same latency profile as a baseline run without topic B, and that no
send on any topic fails with a send-pipeline timeout.

**Acceptance Scenarios**:

1. **Given** a running simulation with request-reply scenarios on topic pairs A and B sharing one
   producer, **When** reply-channel preparation for B stalls for a period approaching the reply
   timeout, **Then** requests on A continue to be sent, confirmed, matched to replies, and reported
   with latencies matching a baseline run without B.
2. **Given** the same simulation, **When** reply-channel preparation for B is in progress,
   **Then** delivery confirmations for already-sent messages on any topic are processed without
   waiting for B's preparation to finish.
3. **Given** a simulation where many virtual users hit the same new reply topic concurrently,
   **When** the reply channel for that topic becomes ready, **Then** all waiting requests proceed,
   the channel was prepared only once, and at no point did any of them delay delivery
   confirmations for other messages.

---

### User Story 2 - Reply-channel setup failure affects only the requesting operation (Priority: P2)

A performance engineer runs a simulation where reply-channel preparation for a topic ultimately
fails (for example, times out because the reply topic is misconfigured). Only the requests that
needed that topic are marked as failed, each with a descriptive error; every other scenario keeps
running and measuring normally, and a later request to the same topic may attempt preparation
again.

**Why this priority**: Failure isolation is what makes a long soak test survivable: one
misconfigured scenario must produce clearly attributed failures for itself, not poison the shared
send machinery for the rest of the run.

**Independent Test**: Configure a request-reply scenario against a reply topic that can never
become ready, alongside a healthy scenario on another topic. Verify the doomed requests fail with
a descriptive error after the configured timeout, the healthy scenario's results are unaffected,
and the run completes normally.

**Acceptance Scenarios**:

1. **Given** a request-reply scenario whose reply topic never becomes ready, **When** preparation
   reaches the configured timeout, **Then** that request is reported as failed with an error
   naming the topic and the timeout, and the virtual user continues its scenario.
2. **Given** the same failure, **When** other scenarios continue running, **Then** their sends,
   confirmations, and reply matching proceed unaffected, with no send-pipeline errors attributable
   to the failed preparation.
3. **Given** a preparation failure for a topic, **When** a later request targets the same topic,
   **Then** preparation is attempted again rather than the topic being permanently failed.

---

### User Story 3 - Reported latency reflects the system under test, not plugin setup (Priority: P3)

A performance engineer reads the simulation report. Request-reply response times measure the
round-trip of the message exchange — from the message being sent to the reply arriving — and do
not include time the plugin spent preparing its own reply tracking, consistent with how successful
requests are measured today.

**Why this priority**: Even with stalls eliminated, silently folding setup cost into response
times would shift reported percentiles and mislead capacity decisions. Measurement semantics must
stay stable across plugin versions.

**Independent Test**: Run a scenario where the first request to a topic triggers reply-channel
preparation with an induced delay. Verify the first request's reported response time is in line
with subsequent requests' (round-trip only), not inflated by the preparation delay.

**Acceptance Scenarios**:

1. **Given** a topic whose reply-channel preparation is slow, **When** the first request to it
   completes successfully, **Then** its reported response time reflects the message round-trip
   and excludes the preparation delay, matching current behavior for successful requests.
2. **Given** a request that fails during preparation, **When** it is reported, **Then** its
   failure timing spans from request start to failure detection, matching current behavior for
   failed requests.

---

### Edge Cases

- Preparation times out: only the affected request fails (descriptive error); the shared send
  pipeline stays healthy; the topic remains eligible for a fresh preparation attempt.
- The send itself fails after reply tracking was already prepared for the request: the tracking
  reservation made for that request is released — no leaked reservations or reply-channel
  subscriptions accumulate from failed sends.
- A reply arrives immediately after the send: it is still matched. The change in when tracking is
  prepared relative to the send must not introduce any new window in which a reply can arrive
  before the system is able to match it (replies that would be matched today must still match).
- The shared reply consumer has already failed: a request-reply attempt fails fast for that
  request with the existing "consumer failed" error, without occupying the shared
  delivery-notification path.
- Simulation shuts down while a preparation is in flight: shutdown completes cleanly; the waiting
  request is resolved (failed or abandoned per existing shutdown semantics) rather than blocking
  termination.
- Request-reply and fire-and-forget scenarios share one producer: fire-and-forget sends and their
  confirmations are never delayed by any request-reply preparation.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Preparing reply tracking for a request-reply operation MUST NOT block or delay the
  shared delivery-notification path that serves all sends of the simulation. Waiting for a reply
  channel to become ready happens off that path.
- **FR-002**: Processing of a delivery confirmation for any message MUST NOT wait on reply-tracking
  preparation of any other message, regardless of topic.
- **FR-003**: When reply-tracking preparation for a topic fails or times out, the system MUST fail
  only the affected request(s) — reported as failed with an error naming the topic and cause —
  while the virtual user continues its scenario and the shared send machinery remains fully
  usable. A subsequent request to the same topic MUST be able to attempt preparation again.
- **FR-004**: Replies MUST NOT be lost as a consequence of reordering preparation relative to
  sending: every reply that is matched under current behavior MUST still be matched. No new
  interval may exist in which a request has been sent but its reply cannot be matched.
- **FR-005**: Reported response-time semantics MUST remain unchanged: successful request-reply
  response time measures from message sent to reply received, excluding reply-tracking
  preparation; failed requests continue to span request start to failure detection.
- **FR-006**: If a send fails after reply tracking was prepared for that request, the tracking
  reservation made for it MUST be released, leaving no residual reservation or reply-channel
  subscription attributable to the failed send.
- **FR-007**: Concurrent first use of the same reply topic by multiple virtual users MUST result
  in the channel being prepared once, with every waiting request proceeding when it is ready and
  none of them occupying the shared delivery-notification path while waiting.
- **FR-008**: The published Scala and Java DSLs, protocol configuration options, defaults, and
  observable result semantics MUST remain unchanged. The fix is internal to the request-reply
  execution flow.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In a two-topic-pair simulation where one pair's reply-channel readiness is delayed
  to near the configured timeout, the unaffected pair's response-time distribution (median and
  p95) matches a baseline run without the delayed pair, and 100% of its requests complete without
  send-pipeline errors.
- **SC-002**: During the entire preparation-delay window, delivery confirmations for unrelated
  in-flight sends continue to be processed — zero unrelated sends fail with pipeline timeouts
  attributable to the delay (today: cascading timeouts on the shared producer).
- **SC-003**: A regression test that reproduces the stalled-confirmation scenario fails against
  the pre-fix behavior and passes after the fix.
- **SC-004**: When preparation for a topic times out, exactly the requests targeting that topic
  are reported failed, each with an error naming the topic and timeout; 100% of other in-flight
  requests complete and report normally.
- **SC-005**: The complete existing verification suite — unit and integration tests, the CI
  Gatling simulations, and construction of every README/example simulation — passes unchanged.

## Assumptions

- Per-request failure remains the contract for preparation timeout: the request is reported
  failed and the virtual user continues; no automatic in-request retry is introduced.
- Successful-request response time today excludes reply-tracking preparation (the timer starts at
  the message-sent moment recorded after preparation); this existing semantic is preserved rather
  than redefined.
- The single shared producer per protocol instance remains the delivery model; this feature does
  not introduce per-scenario producers.
- Scope is bounded to issue #163 — keeping reply-tracking preparation off the shared
  delivery-notification path and its direct failure/release semantics (FR-001 … FR-008).
  Sibling milestone defects remain separately tracked and are not prerequisites: the consumer
  crash after full unsubscription (#143), the no-op re-subscription that never signals readiness
  (#164), per-request subscription churn (#165), and the tracker timer/actor leak (#166). Those
  issues make slow preparation *common*; this feature removes its *amplification* across the
  shared pipeline.
- Reply-channel preparation may still legitimately take up to the configured reply timeout; this
  feature changes where that waiting happens and who it affects, not how long it may take.
