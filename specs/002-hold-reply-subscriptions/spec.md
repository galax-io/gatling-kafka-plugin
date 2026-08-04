# Feature Specification: Idle-Released Reply Channels for Request-Reply

**Feature Branch**: `002-hold-reply-subscriptions`

**Created**: 2026-08-04

**Status**: Draft

**Input**: User description: "https://github.com/galax-io/gatling-kafka-plugin/issues/165"

Issue #165: the plugin's ability to receive replies on a reply topic — its *reply channel* — is torn
down the moment the last in-flight request using it completes, and re-established from scratch on
the very next request. A sequential request-reply scenario therefore pays a full reply-channel
re-establishment **per request**, even though every request targets the same topic.

The cost lands in three places, all of which corrupt what the tool exists to measure:

- **Throughput.** Wall-clock time per request grows by the whole establishment cost, so requests per
  second collapse and the scenario's configured pacing no longer describes the load actually applied.
- **Reported latency.** Establishment monopolises the shared reply-receiving machinery, so replies
  already in flight — including replies for *other* topics sharing it — are detected later than they
  arrived, inflating reported round-trip times.
- **Reliability.** Every teardown reopens a window in which the reply channel is absent or being
  rebuilt. That window is what sibling defects in this milestone turn into spurious assignment
  timeouts (#164) and a hard failure of the shared receiving machinery once everything has been torn
  down (#143).

This feature breaks the cycle by changing *when* a channel is released: not when its last request
finishes, but when it has been idle long enough to be genuinely finished with.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A steady request-reply scenario pays reply-channel setup once, not per request (Priority: P1)

A performance engineer runs a request-reply scenario that sends many requests, one after another, to
the same topic pair. The first request establishes the reply channel; every request after it reuses
it immediately. Sustained throughput is governed by the system under test and the scenario's
configured pacing — not by repeated plugin setup — and reported round-trip times reflect when replies
actually arrived.

**Why this priority**: This is the defect. Today a sequential scenario pays setup on every single
request, which both caps achievable throughput and distorts the load profile the engineer configured.
No other improvement in this milestone matters if the tool cannot sustain a plain sequential
request-reply loop at the rate it was asked to.

**Independent Test**: Run a sequential request-reply scenario of at least 50 requests against one
topic pair in an environment where reply-channel establishment takes a measurable amount of time.
Verify establishment occurs exactly once for the run, every request succeeds, and total wall-clock
time contains one establishment cost rather than fifty.

**Acceptance Scenarios**:

1. **Given** a sequential request-reply scenario issuing N requests to the same topic pair, **When**
   the run completes, **Then** the reply channel for that topic was established exactly once and all
   N requests were matched to their replies.
2. **Given** the same scenario, **When** request 2 and every later request is sent, **Then** it
   proceeds without waiting for any reply-channel setup, and its reported response time is in line
   with the first request's round-trip rather than inflated by setup.
3. **Given** two request-reply scenarios running concurrently on different topic pairs, **When**
   scenario B repeatedly completes and starts new requests, **Then** scenario A's reported response
   times match a baseline run in which scenario B is absent — B's request cadence no longer disturbs
   A's reply detection.

---

### User Story 2 - Reply channels survive gaps and concurrency without failing requests (Priority: P2)

A performance engineer runs a scenario with pauses, pacing, or ramping virtual users, so requests to
the same reply topic are separated by idle gaps and arrive from different users at overlapping times.
Every request finds a working reply channel. No request fails because the channel was unavailable,
being rebuilt, or in the middle of being taken down by another user finishing.

**Why this priority**: Any realistic load profile — think time, pacing, ramp-up — produces exactly
these gaps and overlaps. Keeping the channel across them removes a whole class of intermittent,
hard-to-diagnose failures that today appear as flakiness in the system under test rather than in the
tool. The grace is sized for exactly this: it must outlast think time, not request latency.

**Independent Test**: Run a scenario in which one virtual user completes its request and a pause
longer than the previous request's duration precedes the next request to the same topic, alongside a
second user interleaving requests on the same topic. Verify zero failures attributable to
reply-channel availability and zero reply-channel re-establishments after the first.

**Acceptance Scenarios**:

1. **Given** a request-reply scenario with a pause between two requests to the same topic, **When**
   the second request is sent, **Then** the reply channel is still available, no re-establishment
   occurs, and the request is matched to its reply.
2. **Given** several virtual users issuing overlapping requests to the same topic, **When** one user
   finishes its last request while another is about to send, **Then** the second user's request is
   unaffected — the channel is not taken down underneath it and no request fails for lack of one.
3. **Given** a reply topic whose first establishment fails or times out, **When** a later request
   targets the same topic, **Then** establishment is attempted again and can succeed; the failed
   attempt left no partially-established channel that silently drops replies.

---

### User Story 3 - Reply channels are released when they go idle, and when the simulation ends (Priority: P3)

A performance engineer runs several simulations in one session. Everything a run holds open for its
duration is released when that run ends, so the next simulation starts from a clean state and results
from one run are never influenced by resources left behind by another.

**Why this priority**: A fix for per-request churn that simply stopped releasing would trade a runtime
cost for unbounded accumulation — reverting issue #78, where per-user reply topics grow subscriptions,
actors and timers without limit. Releasing on idleness keeps both properties, and makes end-of-run
cleanup the backstop rather than the only reclamation.

**Independent Test**: Run two simulations back-to-back in the same process. Verify the second
establishes its own reply channels, completes normally with results matching a solo run, and that no
reply channel, receiving machinery, or background activity from the first run is still active after
it ended.

**Acceptance Scenarios**:

1. **Given** a scenario that uses a reply topic once and never returns to it, **When** the idle grace
   elapses, **Then** the channel is released and its topic unsubscribed, and a later request to that
   topic re-establishes and succeeds.
2. **Given** a completed simulation that still held reply channels, **When** it ends, **Then** every
   channel and its associated bookkeeping is released and no background activity from that run
   remains.
3. **Given** a simulation ending with requests still awaiting replies, **When** shutdown runs,
   **Then** it completes without hanging and each outstanding request is resolved under the existing
   shutdown semantics.
4. **Given** two simulations run consecutively in the same process, **When** the second runs, **Then**
   its results match those of running it alone.

---

### Edge Cases

- **Idle gap shorter than the grace**: the channel is still there; the next request pays no setup.
- **Idle gap longer than the grace**: the channel is released and the topic unsubscribed. The next
  request re-establishes once and succeeds — one establishment per idle period, not per request.
- **Releasing the last channel**: the consumer must not be left with no subscription at all, or its
  next poll fails and takes the pool with it. The final subscription is deliberately kept.
- **Reply with no matching pending request** (its request already completed, timed out, or was
  answered twice): it is discarded silently — not reported as a failure, not attributed to an
  unrelated later request.
- **Third-party traffic on a live reply topic**: while a channel is held, messages published by
  systems outside the simulation are received. They must be discarded without producing failures,
  without being matched to any request, and without degrading reply detection for real replies.
- **Establishment fails or times out**: only the requesting operation fails, with a descriptive error
  naming the topic and cause; the topic stays eligible for a fresh attempt later; nothing half-built
  is left behind.
- **High-cardinality reply topics**: a scenario deriving a distinct reply topic per user or per
  request holds one channel per topic used within the last grace period, not one per topic ever used.
  A high enough rate of *new* topics still holds many at once (see Assumptions).
- **Shared reply machinery fails mid-run**: existing failure semantics apply; held channels neither
  mask the failure nor delay its propagation to waiting requests.
- **Simulation ends while a first establishment is still in flight**: shutdown completes cleanly and
  the waiting request is resolved rather than blocking termination.
- **Two topic pairs whose replies are matched by different matching rules**: each is established and
  held independently; neither one's lifecycle affects the other.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Completion of the last request using a reply channel MUST NOT tear it down. The channel
  MUST remain available at least until it has had nothing in flight for an idle grace period that is
  far longer than the gaps a realistic load profile puts between requests.
- **FR-002**: The reply-tracking registration MUST share the channel's lifetime, so a later request
  reuses it directly. Holding the channel without holding its registration is insufficient — the
  request would still take the establishment path.
- **FR-003**: A request targeting an already-established reply channel MUST proceed without any
  establishment wait.
- **FR-004**: Reply-channel establishment MUST NOT recur per request. For a scenario whose requests
  are separated by less than the idle grace, establishment MUST occur exactly once, independent of
  request count, virtual-user count, and the order in which requests complete.
- **FR-005**: Concurrent first use of the same reply topic by multiple virtual users MUST result in a
  single establishment, with every waiting request proceeding once it is ready.
- **FR-006**: A failed or timed-out establishment MUST fail only the requesting operation, reported
  with an error naming the topic and cause, and MUST leave the topic eligible for a fresh attempt by a
  later request, with no partially-established channel remaining.
- **FR-007**: Reply-channel establishment MUST NOT be re-triggered by the completion of any request.
  Because establishment temporarily occupies the shared reply-receiving machinery, its effect on reply
  detection for other topics is thereby bounded to each topic's first use rather than recurring on
  every request.
- **FR-008**: A message arriving on a held reply channel that matches no pending request MUST be
  discarded without being reported as a failure and without being matched to an unrelated request.
- **FR-009**: A reply channel that has had nothing in flight for the grace period MUST be released
  and its topic unsubscribed, so a scenario deriving reply topics per virtual user does not accumulate
  channels for the run. Everything still held when the simulation ends MUST be released, leaving
  nothing active that could affect a subsequent simulation in the same process.
- **FR-012**: Releasing a channel MUST NOT leave the consumer with no subscription at all: a consumer
  with neither a subscription nor an assignment fails on its next poll and takes the whole pool with
  it.
- **FR-010**: Reported response-time semantics MUST remain unchanged: a successful request-reply
  measures from message sent to reply received; a failed one spans request start to failure detection.
- **FR-011**: The published Scala and Java DSLs, protocol configuration options, defaults, and
  observable result semantics MUST remain unchanged. No new configuration option is introduced by this
  feature.

### Key Entities

- **Reply channel**: the plugin's ability to receive replies for one reply topic. Created on first
  use, held for the rest of the simulation run, released at its end. Previously scoped to the set of
  in-flight requests using it.
- **Reply-tracking registration**: the per-(reply topic, matching rule) bookkeeping that correlates a
  sent request with its reply and owns the set of outstanding requests. Shares the reply channel's
  run-scoped lifetime.
- **Simulation run**: the lifetime boundary for both of the above — from the first request that needs
  a reply channel to the end of the simulation.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In a sequential request-reply scenario of at least 50 requests against one topic pair,
  the reply channel is established exactly once and 100% of requests are matched to their replies
  (today: one establishment per request).
- **SC-002**: A sequential run of N requests performs one reply-channel establishment, not N.
  *Measured*: a 50-request sequential scenario took 31.9 s before the change, re-establishing on every
  request; after it, establishment happens once and the scenario is bound only by round-trip time.
  Under sustained concurrent load the effect is smaller but real — with 30 concurrent users the
  refcount rarely reached zero, so the churn was less frequent; reply loss there fell from 0.21–0.25%
  to 0–2 KO of ~6,750 across runs (see the load-harness figures in tasks.md T028).
- **SC-009**: A reply channel with nothing in flight is released within a bounded time of going idle,
  and its topic unsubscribed, so a scenario using each reply topic once does not accumulate channels
  for the run. A later request to the same topic re-establishes and succeeds.
- **SC-003**: In a two-scenario run where one scenario repeatedly completes and restarts requests on
  its own topic pair, the other scenario's median response time is no more than 1.5× its median in a
  baseline run without the first scenario, and its slowest request stays below the cost of one
  reply-channel establishment. *This is a forward guard, not a demonstration of the defect*: measured
  against the pre-change code, the second scenario's churn did not move the first's median outside
  that bound, because re-establishing after the initial group join is far cheaper than assumed (see
  SC-002 and research.md R5).
- **SC-004**: A scenario combining idle gaps and overlapping virtual users on one reply topic reports
  zero failures attributable to reply-channel availability across at least 100 requests.
- **SC-005**: Messages arriving on a held reply channel that match no pending request produce zero
  reported failures and zero incorrect matches.
- **SC-006**: Two simulations run consecutively in one process both complete, and the second's results
  match a solo run of it; no receiving machinery or background activity from the first remains after
  it ends.
- **SC-007**: A regression test reproducing per-request reply-channel churn fails against the
  pre-change behavior and passes after the change.
- **SC-008**: The complete existing verification suite — unit and integration tests, the CI Gatling
  simulations, and construction of every README/example simulation — passes unchanged.

## Assumptions

- **Release on idleness, not on completion.** Issue #165 offers two remedies: hold reply channels for
  the simulation's duration, or expire them after an idle period. This specification adopts the idle
  period. Holding for the whole run also fixes #165, but it reverts issue #78 — closed by commit
  `0ae53a1` and released since v0.22.10 — under which per-user reply topics accumulate subscriptions,
  actors and timers without bound. The count of requests in flight is an accurate measure of current
  work and a poor predictor of future use; idleness is the signal that distinguishes "quiet for a
  moment" from "finished". See research.md R1/R2.
- **Reply topics stay dynamically discovered.** A reply topic may be derived from a feeder or a
  session expression, so channels are still established on first use rather than declared up front.
  Optional up-front declaration of reply topics (#193, point 6) is a separate, opt-in feature and is
  out of scope here.
- **High-cardinality reply topics are bounded by the grace window, not by the run.** A scenario
  deriving a unique reply topic per user or per request holds one channel per topic used within the
  last grace period, not one per topic ever used. That is the property issue #78 requires. It is a
  bound, not a cap: a high enough rate of *new* topics still holds many channels at once, which is
  why the grace is a think-time scale rather than a longer one.
- **First use still pays establishment cost.** This feature changes how often that cost is paid, not
  how long a single establishment may take.
- **Response-time semantics are preserved, not redefined.** Whether the reported clock should instead
  start when the message is handed to the sending machinery is an open decision recorded in #193 and
  is out of scope; changing it would move reported percentiles and requires its own migration entry.
- **Scope is bounded to issue #165** — reply-channel and reply-tracking lifetime (FR-001 … FR-011).
  Sibling milestone defects remain separately tracked and are not prerequisites: the no-op
  re-subscription that never signals readiness (#164), the crash after everything is unsubscribed
  (#143), the per-registration background timer leak (#166), the reply dropped before its request
  finishes registering (#191), and readiness meaning "positioned" rather than merely "subscribed"
  (#193). Removing the teardown/re-establish cycle closes the windows #164 and #143 depend on, but
  neither issue is claimed as fixed here.
- **Interaction with #166.** A tracker's periodic timeout scan is released with its channel, so idle
  release bounds the timer count by *active* channels rather than by every channel ever used. The
  discarded `Cancellable` itself remains #166's concern.
- **Interaction with #143.** Releasing the last channel would leave the consumer with no subscription
  and no assignment, whose next poll fails the whole pool. Idle release makes that a routine event
  rather than a rare one, so the final subscription is kept. This narrows #143's trigger from
  production paths; the issue stays open for the defensive behaviour it also covers.
- **Non-blocking acquisition (#163) is already in place** and is preserved: establishment continues to
  happen off the shared send-confirmation path.
