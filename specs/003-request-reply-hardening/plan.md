# Implementation Plan: Request-Reply Reliability Hardening

**Branch**: `003-request-reply-hardening` | **Date**: 2026-08-04 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/003-request-reply-hardening/spec.md`

## Summary

Close the four issues still open in milestone **v1.1.0 Request-reply reliability** — #143, #196,
#166, #191 — in that order, as four independent commits.

The technical core is #191, and the research reached a conclusion that makes it far smaller than the
issue implies. The reply is dropped not because the actor mailbox is unordered, but because the
pending-request record is created **after** the record is handed to the producer: the request is on
the wire, and answerable, before anything is watching for its answer. Gatling's mailbox is an MPSC
queue that preserves enqueue order across producer threads, so moving registration *before* the send
establishes a genuine happens-before chain — registration → produce → broker → responder → consume →
delivery — and the race disappears without a concurrent correlation table, without touching
`acquireTracker`'s signature, and without the pool-owned correlation map that #193 sketches for
v1.2.0.

The one real cost is measurement. Today the response-time clock starts in the producer ack callback,
and the spec's FR-017 keeps it there. Registering before the send means the ack timestamp is not
known at registration, so the tracker gains a two-phase completion: a reply that arrives before its
own ack is held until the ack lands, rather than being reported against the wrong clock. That, plus
two additive messages on an internal sealed trait, is the whole of #191's surface.

The other three are small and self-contained: guard the poll when there is nothing to receive on
(#143), cancel the timeout scan and stop the tracker when its channel is released (#166), and give
the CI simulation a real echo responder plus a dedicated topic for the scenario that must time out
(#196).

## Technical Context

**Language/Version**: Scala 2.13 on sbt; Java 17+ (Temurin in CI)

**Primary Dependencies**: Gatling 3.13.5 (`gatling-core` actor system, `StatsEngine`, `Clock`);
Confluent Kafka clients 7.9.2-ccs. Avro4s 4.1.2 + Schema Registry stay `provided` and untouched.
**No new dependency is introduced.**

**Storage**: N/A — all state is in-process and run-scoped.

**Testing**: munit + ScalaTest under `sbt test`, with Testcontainers
(`ConfluentKafkaContainer`, `confluentinc/cp-kafka:7.9.5`) for anything touching broker behaviour;
Gatling simulations (`KafkaGatlingTest`, `KafkaJavaapiMethodsGatlingTest`) against the
`docker-compose.kafka.yml` stack in CI; `ExampleSmokeValidation` for API construction.

**Target Platform**: JVM library published to Sonatype, consumed by Gatling simulations.

**Project Type**: Library / Gatling protocol plugin.

**Performance Goals**: zero reply loss across ≥5,000 sustained concurrent request-reply requests
against an echo responder (SC-001); per-channel background timer count equal to channels currently
held, not channels ever created (SC-005).

**Constraints**:

- No change to the published Scala DSL, the `javaapi` facade, protocol defaults, or wire formats.
- Response-time semantics preserved exactly (FR-017) — the ack-based clock start stays, because
  moving it belongs to #170/#193 and must ship with a Migration Guide entry.
- `KafkaMessageTrackerPool`'s primary constructor signature stays as it is; spec `002` already
  established that changing it is a binary break that would force a major version for a bug fix.
- No MiMa in the build, so binary compatibility is a review obligation rather than a gate.

**Scale/Scope**: 4 issues → 4 commits. 5 main-source files, 5 test files, 2 broker definitions.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

*Source: `.specify/memory/constitution.md` v1.0.0.*

- [x] **I. Published API Compatibility** — **No** change to any public Scala DSL signature, `javaapi`
      signature, default protocol setting, or serialized format. Internal changes are additive:
      two case classes on the sealed `TrackerMessage` trait, an overloaded
      `DynamicKafkaConsumer.apply`, and a secondary `KafkaMessageTrackerPool` constructor. Every
      existing signature — including `acquireTracker`, `releaseTracker`, and both primary
      constructors — is untouched, and `MessagePublished`'s field list is deliberately preserved.
      `ExampleSmokeValidation` is unaffected because nothing it constructs changes.
      **One deviation requires approval before #191 merges** — see Complexity Tracking.
- [x] **II. Real Broker Over Mocks** — every behaviour claim is asserted against a real broker.
      #143 and #166 get Testcontainers integration tests; #191 gets both a Testcontainers test that
      forces the race deterministically and the CI simulation as its oracle; #196 *is* a broker test.
      Mocks stay confined to `StubClock` / `RecordingAction` — units with no Kafka interaction —
      exactly as the existing client specs already use them.
- [x] **III. Layer Separation & Single Wire Contract** — `KafkaSender` still only sends,
      `DynamicKafkaConsumer` still only consumes, `KafkaMessageTracker` and its pool still only
      track. #191 changes the *order* in which `KafkaRequestReplyAction` calls its injected
      collaborators, not who owns what; the action still constructs none of them.
      `KafkaProtocolMessage` and `KafkaMatcher` are unchanged — #196's response timestamp rides in
      the existing `headers` field rather than in a new message type. No new abstraction is
      introduced: the correlation table stays where it is, because the ordering fix removes the
      reason to move it.
- [x] **IV. Test-First for Behavior Change** — all four are bug fixes, so each ships a test that
      reproduces the defect against pre-change code. #143 and #191 in particular must *force* their
      condition (a parameterised initialization wait; a responder faster than the ack path) rather
      than wait for it to occur by chance, which is what the issues ask for.
- [x] **V. One Concern per Change, Always Green** — spec, plan and tasks land first as one
      `docs(speckit):` commit. Then one semantic commit per issue, each green on its own under
      `sbt scalafmtCheckAll scalafmtSbtCheck compile test`, each PR carrying the
      `v1.1.0 Request-reply reliability` milestone and `Closes #NNN`.
- [x] **Constraints** — no new dependency and no upgrade; Avro/Schema Registry stay `provided` and
      optional (nothing here touches them); no supported Gatling version changes, so the README
      compatibility table is untouched.

### Post-Design Re-check

Re-evaluated after Phase 1. No gate changed verdict. Two design decisions were made *because* of the
gates rather than despite them:

- Principle III steered #191 away from #193's pool-owned correlation map. Once the ordering argument
  in [research.md](research.md) R3 held, moving the table would have been an abstraction with no
  second caller — introduced in anticipation of v1.2.0's deadline consolidation, which Principle III
  forbids.
- Principle I steered #166 away from hoisting the timeout scan into the pool sweep (#193 point 4).
  The tracker already owns its `Cancellable`; having it cancel its own scan on a terminal message is
  additive, whereas hoisting would change what the pool hands back from `acquireTracker`.

The single deviation in Complexity Tracking survived the re-check and still needs sign-off.

## Project Structure

### Documentation (this feature)

```text
specs/003-request-reply-hardening/
├── plan.md              # This file
├── research.md          # Phase 0 output — R1..R7, decisions with alternatives
├── data-model.md        # Phase 1 output — state each fix owns and its transitions
├── quickstart.md        # Phase 1 output — how to run each fix's verification
├── contracts/
│   └── internal-api.md  # Phase 1 output — internal signatures and guarantees, red/green per issue
├── checklists/
│   └── requirements.md  # From /speckit-specify
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created here)
```

### Source Code (repository root)

```text
src/main/scala/org/galaxio/gatling/kafka/
├── actions/
│   └── KafkaRequestReplyAction.scala      # #191 — acquire and register before send; ack and
│                                          #        send-failure callbacks feed the tracker
├── client/
│   ├── DynamicKafkaConsumer.scala         # #143 — honour the init-wait result; never poll with
│   │                                      #        nothing subscribed and nothing assigned;
│   │                                      #        additive apply overload for the wait duration
│   ├── KafkaMessageTracker.scala          # #166 — terminal Stop cancels the scan and dies
│   │                                      # #191 — two-phase completion (ack / reply join)
│   └── KafkaMessageTrackerPool.scala      # #166 — TrackerEntry carries the scan handle; sweep,
│                                          #        failure broadcast and shutdown all stop it
└── protocol/, checks/, request/           # unchanged

src/test/scala/org/galaxio/gatling/kafka/
├── client/
│   ├── DynamicKafkaConsumerSpec.scala     # #143 — no-subscription poll guard
│   └── KafkaMessageTrackerSpec.scala      # #191 — reply-before-ack join; #166 — Stop behaviour
├── integration/
│   ├── ConsumerStartupSpec.scala          # #143 — NEW: late first request-reply, real broker
│   ├── TrackerLifetimeSpec.scala          # #166 — timer released with the channel;
│   │                                      # #191 — the reply re-publish workaround comes out
│   └── ReplyRegistrationRaceSpec.scala    # #191 — NEW: echo faster than the ack path
└── examples/
    └── KafkaGatlingTest.scala             # #196 — echo responder, own topic for the timeout
                                           #        scenario, pinned assertion

docker-compose.kafka.yml                   # #196 — new topic in the init list
.github/workflows/ci.yml                   # #196 — same topic in KAFKA_CREATE_TOPICS
```

**Structure Decision**: The existing single-module sbt layout is unchanged. Every source edit lands
in `actions/` or `client/`, which is where the constitution already locates send/track/consume
responsibilities. Two new integration specs are added rather than extending `KafkaIntegrationSpec`,
because each needs its own broker configuration — a shortened initialization wait for #143, an
in-process echo responder for #191 — and folding either into the shared spec would impose that
configuration on unrelated tests.

## Implementation Sequence

Four commits, in this order. The ordering is not arbitrary — #193's own sequencing note asks for
#196 before #191 so the hardest fix has a deterministic CI oracle rather than a coincidence.

| # | Issue | Scope | Why here |
|---|-------|-------|----------|
| 1 | **#143** | `DynamicKafkaConsumer` + one integration spec | Fully self-contained; removes a terminal, run-ending failure; touches nothing the other three touch. |
| 2 | **#196** | `KafkaGatlingTest` + both broker definitions | Test-only. Lands the echo responder that makes #191's fix verifiable in CI instead of by inference. |
| 3 | **#166** | `KafkaMessageTrackerPool` + `KafkaMessageTracker` | Small and independent. Lands before #191 so #191 rebases onto a tracker whose lifecycle is already correct. |
| 4 | **#191** | `KafkaRequestReplyAction` + `KafkaMessageTracker` | Largest, and the only one with an approval gate. Benefits from all three above. |

Each PR carries the `v1.1.0 Request-reply reliability` milestone and `Closes #NNN`. Once all four
merge the milestone is tag-ready; note that its title already starts with `v1.1.0`, so
`scripts/check-linkage.sh --for-tag v1.1.0` resolves it.

## Complexity Tracking

> One deviation. It is not extra complexity — it is a deliberate behaviour change that Principle I
> requires be proposed and approved rather than arriving as a side effect.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|--------------------------------------|
| **#191 changes what the system under test receives when tracker acquisition fails.** Today the request is published and *then* acquisition is attempted, so a failed acquisition still delivers the message to the broker before reporting KO. After the change, acquisition and registration precede the send, so a failed acquisition reports the same KO without publishing anything. The Gatling-visible result is identical — same KO, same message text, same response-time span — but the broker and the system under test see one fewer record. | FR-001 requires a defined order between registering a request and its reply becoming possible. A reply becomes possible the moment the record is handed to the producer, so registration must precede the send. Once registration precedes the send, acquisition must too, since registration needs the channel. There is no ordering that both registers first and still publishes on acquisition failure without publishing a request whose reply can never be received — which is the state #143 exists to prevent from the other direction. | *Keep send-then-acquire and buffer unmatched replies briefly*: narrows the window instead of closing it, gives no defined order (FR-001), and is the "retain unmatched replies" shape FR-002 forbids. *Keep send-then-acquire and accept the loss*: that is the defect. *Publish on acquisition failure anyway, before reporting KO*: publishes a request nothing can ever receive the answer to, and makes the failure path deliver load the success path would not — a worse measurement artefact than the one being fixed. |

**Approval needed from the maintainer before the #191 PR merges**, per Principle I. It is a
behaviour change inside a bug fix, so it carries no `!:` marker and does not force a major version,
but the PR description must state it explicitly and the README Migration Guide should note it under
the v1.1.0 entry.

## Phase Outputs

- **Phase 0** → [research.md](research.md) — R1 (#143 guard placement), R2 (#166 stopping a Gatling
  actor that has no stop), R3 (#191 why mailbox ordering suffices), R4 (#191 keeping the ack clock),
  R5 (#191 rejected alternatives incl. #193's correlation map), R6 (#196 responder shape and topic
  layout), R7 (making the initialization wait testable without a signature break).
- **Phase 1** → [data-model.md](data-model.md), [contracts/internal-api.md](contracts/internal-api.md),
  [quickstart.md](quickstart.md).
- **Phase 2** → `tasks.md`, generated by `/speckit-tasks`. Not created by this command.
