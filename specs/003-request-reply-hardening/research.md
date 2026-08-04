# Phase 0 Research: Request-Reply Reliability Hardening

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md)

The specification deliberately left the mechanism for #191 and #143 unstated, because both issues
decline to prescribe one. This document resolves them, plus the smaller open questions in #166 and
#196. Every finding below was read out of the code or the Gatling 3.13.5 sources, not assumed.

---

## R1 — #143: where to put the guard, and what to do with the wait's result

**Decision**: Keep the poll loop as it is, and guard the poll itself. In `DynamicKafkaConsumer.run()`,
capture what `initLatch.await(...)` returns and log the distinction; then, inside the loop, skip
`consumer.poll(...)` on any iteration where the consumer holds neither a subscription nor an
assignment, sleeping the poll interval instead and still calling `updateSubscription()` so a topic
requested later is picked up on the next turn.

**Rationale**:

- The exception is thrown by exactly one call. `consumer.poll()` raises
  `IllegalStateException: Consumer is not subscribed to any topics or assigned any partitions` when
  both `subscription()` and `assignment()` are empty. Guarding that call covers every route into the
  state, not just the startup one the issue currently describes.
- The other route is already guarded, from the other end. `updateSubscription()` at
  [`DynamicKafkaConsumer.scala:176-181`](../../src/main/scala/org/galaxio/gatling/kafka/client/DynamicKafkaConsumer.scala)
  refuses to shrink the subscription to nothing, a guard #165 added when idle release made emptying
  the set routine. That protects a subscription that *shrank* to nothing; it cannot protect one that
  was never established, because there is nothing to keep. The two guards are complementary and both
  are needed — which is why the fix belongs at the poll rather than as a second guard in
  `updateSubscription`.
- Discarding the `await` result is what makes the failure unattributable. The issue asks for it to be
  honoured. It does not need to change control flow — the poll guard already handles both outcomes —
  but logging "initialization wait expired with no reply topic requested" at debug turns a silent
  90-second gap into something a reader can find.

**Alternatives considered**:

- *Do not start the poll loop until the first `requestTopicSubscription` lands.* Rejected: it turns
  the bounded `initLatch.await` into an unbounded one, and the loop's `finally` is what closes the
  consumer, so a consumer that never enters the loop needs a second close path. `close()` already has
  to work on a consumer that was never run; adding a second way to not-run it doubles the paths
  through shutdown for no gain.
- *Subscribe to a sentinel topic at construction so the consumer is never empty.* Rejected: it
  creates a topic-shaped side effect on the broker (or an `UNKNOWN_TOPIC_OR_PARTITION` warning loop
  if it does not exist) purely to dodge a client-side precondition, and it would put a topic nobody
  asked for into the consumer group's assignment.
- *Catch the `IllegalStateException` around the poll and continue.* Rejected: control flow by
  exception, which Principle III forbids outright, and it would also swallow a genuine
  `IllegalStateException` from a different cause.

**When it fires, restated from the code**: the wait starts when `KafkaMessageTrackerPool` is
constructed, and the pool is constructed by `ProtocolKey.newComponents` for any protocol whose
`consumeSettings` carry `bootstrap.servers` — before any virtual user runs. So the clock is against
wall time from simulation start, not against first use.

---

## R2 — #166: how to stop a Gatling actor, given that Gatling has no way to stop one

**Decision**: Add a terminal `Stop` message to `KafkaMessageTracker` whose handler cancels the
periodic scan and returns `die`. Store the `TrackerEntry`'s actor ref as today; the tracker keeps
ownership of its own `Cancellable`. `sweepIdleTrackers`, the consumer-failure broadcast, and pool
shutdown each send `Stop` to every entry they drop — *after* removing it from the map, and outside
the `ConcurrentHashMap` compute lambda.

**Findings from the Gatling 3.13.5 sources** (`io/gatling/core/actor/`), all of which shaped this:

- **`ActorSystem` has no `stop`.** `actorOf` returns an `ActorRef` with exactly `!`, `name` and
  `replyPromise`. The only termination path is `Actor.die`, an `Effect` reachable only from inside
  the actor's own behaviour — so stopping a tracker *requires* sending it a message. There is no
  external kill.
- **`die` swaps the behaviour, it does not deallocate.** A dead actor logs
  `Dropping msg '...' as actor is dead` at INFO for anything that arrives afterwards. Reachability is
  what actually frees the tracker, and reachability is held by the scheduled task, not by the actor
  system: `ActorSystem` keeps no registry of spawned actors.
- **The `Cancellable` is the leak.** `Scheduler.scheduleAtFixedRate` returns
  `() => future.cancel(true)` over a `ScheduledFuture` on the actor system's scheduler, and that
  scheduler is a **single** `newSingleThreadScheduledExecutor` shared by the whole simulation. So
  every leaked tracker is not just retained memory — it is one more wakeup per second on one shared
  thread, and the task's closure captures `self`, which transitively retains the tracker's
  `statsEngine`, `clock` and matcher closures. Cancelling is therefore both the memory fix and the
  CPU fix.
- **The `Cancellable` is created inside the actor**, in `triggerPeriodicTimeoutScan`, on the first
  `MessagePublished` carrying a positive reply timeout. The pool never sees it. Having the actor
  cancel its own on `Stop` needs no plumbing at all; having the pool own it would mean hoisting the
  scheduling out of the actor.

**Ordering constraints the fix must respect**:

1. Send `Stop` only after the entry is removed from `trackers`, so no acquisition can hand out a
   tracker that is about to die. `sweepIdleTrackers` already re-checks the entry's state inside
   `computeIfPresent`, and already performs its one side effect (`removeTopicSubscription`) outside
   the lambda — follow that precedent rather than stopping inside the lambda.
2. Release only happens at `refCount <= 0` sustained for a full idle grace, and `refCount` is
   decremented by `onComplete` *after* the request has been reported. So a stopped tracker never has
   outstanding work. That is what makes it safe to stop the very thing that would otherwise time
   those requests out.
3. The consumer-failure path already clears `trackers` wholesale; it must stop what it clears, or the
   timers outlive a pool that can no longer be used at all.

**Residual, accepted**: a `MessageConsumed` broadcast reading a stale snapshot can reach a just-dead
tracker and log one INFO line. The window is the gap between the sweep's removal and the poll thread's
iteration, and the topic is unsubscribed immediately after, so this is bounded to a sweep-versus-poll
race rather than to ongoing third-party traffic.

**Alternatives considered**:

- *Fold deadlines into the pool's existing idle sweep* (#193 point 4). Rejected **for v1.1.0**: it is
  the better end state and #193 should keep it, but it moves timeout detection out of the tracker,
  changes what `acquireTracker` must hand back, and turns a contained bug fix into the v1.2.0
  redesign. Principle III's "no abstraction without a second real caller" applies — there is one
  caller today.
- *Cancel without stopping.* Rejected: it fixes reachability and the shared-thread cost, but leaves a
  live actor that would still process a stale `MessageConsumed` against bookkeeping the pool has
  abandoned. `Stop` costs one case in the behaviour.
- *Stop without cancelling.* Rejected: `die` swaps the behaviour but the scheduled task keeps firing
  and keeps `self` reachable, so the leak survives almost intact.

---

## R3 — #191: why register-before-send is sufficient, and a concurrent correlation table is not needed

**Decision**: Move tracker acquisition and pending-request registration ahead of
`sender.send(...)` in `KafkaRequestReplyAction.sendKafkaMessage`. Leave `sentMessages` as the actor's
private `mutable.HashMap`.

**The finding that makes this work**: Gatling's mailbox preserves enqueue order across producer
threads. `AtomicRunnableActorRef` backs its mailbox with
`PlatformDependent.newMpscQueue[Message]()` — a JCTools multi-producer/single-consumer queue. Producers
claim slots by atomic increment and the single consumer drains in slot order, stopping at any slot
whose element is not yet published rather than skipping ahead. `!` is `mbox.offer(msg)` followed by
`async()`, so the message is in the queue before `!` returns. The 20-message drain limit re-schedules
rather than reorders.

So enqueue order *is* processing order. What is missing today is not ordering — it is that the
enqueue happens too late:

```text
today:   sender.send ──► broker ──► responder ──► broker ──► poll ──► offer(MessageConsumed)
                └─► ack ──► acquireTracker ──► offer(MessagePublished)          ← may lose the race
```

The two offers come from unrelated threads with no happens-before between them, and the ack callback
can be delayed by exactly the producer-I/O-thread pressure the load creates. That is precisely what
#191's log correlation shows: `Record received` and `Received with MatchId` are logged before
`Published with MatchId` for the same key.

Registering first replaces the race with a causal chain:

```text
after:   offer(MessagePublished) ──► sender.send ──► broker ──► responder ──► broker ──► poll
                                                                          └─► offer(MessageConsumed)
```

`offer(MessagePublished)` completes (program order) before `send` is called, and every step to the
reply is causally downstream of that send. A reply cannot exist before the request is sent, so
`MessageConsumed` can never be offered before `MessagePublished`. FR-001's "defined order" is
satisfied structurally, not statistically.

**What this buys**: #193's point 3 — a pool-owned correlation table keyed by correlation id, written
by the sending side and read by the consumer thread — is **not required** to close #191. It remains
valuable for point 4 (folding deadlines into the pool sweep), which is a v1.2.0 concern. Recording
this explicitly because #193's map still lists point 3 as part of #191's fix, and it is not.

**Second-order consequences, all checked**:

- **Acquisition moves off the producer I/O thread onto the virtual user's thread.** This does not
  undo #163: acquisition has been non-blocking since PR #190, so the VU thread is not blocked either.
  The slow path's continuation now performs the `send` on the pool's setup executor rather than on the
  VU thread. `KafkaProducer.send` can block up to `max.block.ms` on metadata or a full buffer; on the
  single-threaded setup executor that would stall other acquisitions. Bounded in practice because the
  slow path runs once per reply topic, but it is a real risk and belongs in the PR description.
- **The refcount is now held across the send.** Acquisition increments it before the record is
  handed over, so a send failure must remove the pending record and release the refcount, or the
  channel never goes idle. This is the leak PR #144 anticipated ("release the already-acquired
  tracker to avoid a ref-count leak") and it is handled by a `SendFailed` message rather than by the
  action reaching into the tracker.
- **A failed acquisition no longer publishes the request.** Deliberate, and the one item in the
  plan's Complexity Tracking table.
- **`KafkaRequestAction` (produce-only) is untouched** — it never acquires a tracker.
- **Throttling is untouched** — `KafkaAction.sendRequest` still wraps the whole of
  `sendKafkaMessage`, whatever order it does things in.

**Historical note**: PR #144 (open, unmilestoned) already proposed "move tracker acquisition before
send". It was written against the blocking `tracker()` API and aimed at #143; #163/PR #190 solved the
blocking problem differently, by making acquisition asynchronous. The ordering idea in #144 turns out
to be the right shape for #191 rather than for #143, now that it no longer implies blocking the
virtual user. Worth saying so on #144 when this lands.

---

## R4 — #191: keeping the ack-based response-time clock

**Decision**: Two-phase completion inside the tracker. `MessagePublished` (enqueued before the send)
creates the pending record. A new `MessageAcked(matchId, sentTimestamp)` (enqueued from the producer
ack callback) supplies the timestamp the report is measured from. `MessageConsumed` completes the
request if the ack has landed, and otherwise holds the reply on the record until it does.

**Why it is needed**: FR-017 keeps the reported clock starting at the producer ack, and after R3 the
ack timestamp is not known at registration. Both events can therefore arrive in either order — and the
order that used to lose the reply (reply before ack) is exactly the one this feature exists to fix, so
it cannot be treated as too rare to handle.

**Why not simply move the clock**: registering before the send makes "when the record was handed to
the producer" the natural start, which is also what #170 asks for and what Gatling's HTTP support
measures. #193 records both as one decision: they move reported percentiles for the same reason and
must ship together with a Migration Guide entry. Making that change here would smuggle a
percentile-moving change into a bug fix — precisely what Principle I forbids.

**Timeout clock**: `TimeoutScan` measures from the record's registration time, not from the ack. A
request whose ack never arrives is stuck and must still time out; measuring from the ack would leave
it pending forever.

**Alternatives considered**:

- *Complete immediately using the registration timestamp when the ack has not landed.* Rejected: the
  requests it affects — the ones whose reply beat the ack — would be reported with a latency that
  includes the produce round trip while every other request excludes it. An invisible inconsistency in
  a measurement tool is worse than one extra message type, and it lands on exactly the population
  under study.
- *Write the ack timestamp into the record from the producer thread via an `AtomicLong`.* Rejected:
  it makes the record concurrent state again for no benefit — the completion still has to run on the
  actor to execute checks and log stats, so a message is needed regardless. Keeping the record
  single-threaded is what makes the join trivially correct.

---

## R5 — #191: the surface this touches, and what is deliberately left alone

**`MessagePublished` keeps its exact field list.** `(matchId, sentTimestamp, replyTimeout, checks,
session, next, requestName, onComplete)` is unchanged; at registration `sentTimestamp` carries the
handoff time and `MessageAcked` supersedes it for reporting. The pending record gains its extra state
inside the actor's private map value, not on the public message.

**Two additive cases on `sealed trait TrackerMessage`**: `MessageAcked` and `SendFailed`. The trait is
sealed, so no external code extends it, and an external exhaustive match on it is not meaningful.

**Untouched**: `KafkaMessageTrackerPool.acquireTracker` and `releaseTracker` signatures; both primary
constructors; `KafkaProtocolMessage`; `KafkaMatcher`; every DSL and `javaapi` entry point.

**A test workaround comes out.** `TrackerLifetimeSpec.send` currently re-publishes the reply in a
loop, with a comment naming #191 as the reason: "a reply which arrives before its own request finishes
registering is dropped silently (issue #191, out of scope here)". Once #191 lands, that loop is no
longer covering for a defect and should be simplified — leaving it would hide a regression, since a
re-published reply masks a dropped one.

---

## R6 — #196: responder shape, topic layout, and the assertion

**Decision**: Follow `KafkaConcurrencyLoadTest`'s shape exactly — a plain `KafkaSender` driven from a
`DynamicKafkaConsumer`, started in `before`, closed in `after`, with a readiness probe before the
simulation begins. One responder consuming both request topics, mapping `myTopic1 → test.t1` and
`myTopic2 → test.t2`.

**Constraints read out of `KafkaGatlingTest`**:

| Scenario | Protocol | Matcher | Check | What the echo must not break |
|---|---|---|---|---|
| `scnRR` | `kafkaProtocolRRString`, 60 s | key (default) | `jsonPath("$.m").is("dkf")` | key and the JSON body |
| `scnRR2` | `kafkaProtocolRRBytes`, 5 s | `matchByValue` | `bodyBytes.is("tstBytes")` | the value byte-for-byte |
| `scnRRwo` | `kafkaProtocolRRBytes2`, 1 s | `matchByValue` | none | must receive nothing at all |

A byte-level echo satisfies all three: the responder consumes and produces `Array[Byte]`, so a
`String`-serialised request round-trips unchanged. Any rewrite of key or value breaks one of the three
columns above, which is why the response timestamp goes in a header.

**Response timestamp**: `KafkaProtocolMessage` already carries `headers: Option[Headers]`, and
`KafkaGatlingTest` already imports `Headers`/`RecordHeaders`. So the responder attaches a
`RecordHeaders` carrying the response time and nothing else changes.

**Topic for `scnRRwo`**: give it a new request topic — `myTopic4` — that the responder does not
consume. Its reply topic stays `test.t2`, per #196's text, which asks only for a dedicated request
topic. That keeps a useful side effect: `scnRR2`'s echoes land on `test.t2` while `scnRRwo` also holds
a registration there under a *different* matcher instance, so the simulation exercises the
"two registrations on one reply topic, one discards" edge case from the spec for free.

**Assertion**: `global.failedRequests.count.is(1)` plus
`details("Request Reply Bytes wo").failedRequests.count.is(1)`. Today's `lte(1)` passes when the
by-design timeout silently stops failing; pinning both directions is what makes the gate meaningful.

**Broker definitions**: `myTopic4` goes into `docker-compose.kafka.yml`'s init command and into
`KAFKA_CREATE_TOPICS` in `.github/workflows/ci.yml`. Note the two lists already differ — compose
creates `test.t`, CI does not. That divergence is #192's subject; this change adds the same topic to
both and does not attempt to reconcile the rest.

**The rebalance-delay question is to be answered by running, not by reasoning.** SC-009 requires the
simulation be run with and without `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0` and the result stated in
the PR. The expected answer is that it is *still* required, because #193 point 5 is unfixed —
readiness resolves on assignment, which precedes position resolution, and the protocol leaves
`auto.offset.reset` at `latest`, so a reply produced in that gap is still skipped. But the issue asks
for an observation and an observation is what it should get.

---

## R7 — making the initialization wait testable without a signature break

**Decision**: Add an overloaded `DynamicKafkaConsumer.apply` taking the initialization wait, and a
secondary `KafkaMessageTrackerPool` constructor that passes one through. Both default to today's
values; production code calls neither.

**Why not the alternatives**:

- *Mutate the companion's `private val initializationTimeout`.* It is object-level state shared by
  every consumer in the JVM, so one test's value would leak into another's. The pool's
  `idleGraceMillis` precedent is not comparable — that is a per-instance field.
- *Make it a per-instance `private[client] var` on the consumer.* The pool constructs its consumer in
  its own constructor, so a test holding only the pool has no moment at which to write the field
  before it is read.
- *Change the primary constructor.* This is the binary break spec `002` explicitly refused, for the
  same reason: it would force a major version for a bug fix.

A secondary constructor and an overload are both purely additive, so nothing compiled against the
current release is affected.

**Test reach**: the consumer-level overload covers the unit assertion (a consumer whose wait expires
with no topic requested does not fail). The pool-level constructor covers FR-005 and FR-007 — build a
pool, request nothing, let the wait expire, then request a topic and see it served.

---

## Summary of decisions

| # | Question | Decision |
|---|---|---|
| R1 | #143 — where to guard | Guard the poll when nothing is subscribed and nothing assigned; honour and log the init-wait result |
| R2 | #166 — how to stop an actor | Terminal `Stop` message: the tracker cancels its own `Cancellable` and returns `die`; the pool sends it after removing the entry |
| R3 | #191 — mechanism | Register before send; mailbox MPSC ordering then gives the happens-before. No pool-owned correlation map |
| R4 | #191 — measurement | Two-phase completion via `MessageAcked`; a reply that beats its ack is held, not misreported |
| R5 | #191 — surface | Two additive `TrackerMessage` cases; every existing signature unchanged; `TrackerLifetimeSpec`'s re-publish workaround removed |
| R6 | #196 — shape | `KafkaConcurrencyLoadTest`-style responder, byte-level echo, timestamp in a header, `myTopic4` for the timeout scenario, two-sided assertion |
| R7 | #143 — testability | Additive `apply` overload and secondary pool constructor; production defaults unchanged |
