# Behaviour Contract: Measurement Truth in Request-Reply

**Feature**: `008-measurement-truth` | **Date**: 2026-08-24

The observable contract this feature commits to. Each clause is a statement a test can fail.
`C1`…`C13` are referenced by [tasks.md](../tasks.md).

Notation: `⟨name⟩` is the request name a simulation declared; `⟨name⟩ [rejected: k]` is the derived
name for rejection kind `k`.

---

## Group A — What a run reports for a rejection

### C1 — A request whose matcher yields no correlation id is reported under a derived name

```
Given  a request-reply protocol whose matcher returns nothing for this request
       (the default key matching with no key; value matching with no payload;
        a custom extractor returning null)
When   the request is issued
Then   exactly one outcome is reported
And    its request name is  ⟨name⟩ [rejected: no correlation id]
And    its status is KO
And    its message is unchanged from today — it names the matcher and the remedy
And    no record reaches the broker
And    the virtual user is continued, marked as failed
```

### C2 — A request whose reply channel cannot be acquired is reported under a derived name

```
Given  a request-reply whose reply channel cannot be established
When   acquisition fails
Then   its request name is  ⟨name⟩ [rejected: no reply channel]
And    its status is KO
And    its message is the underlying failure's message, unchanged
And    the interval it reports is the real wait — the same span reported today
And    no record reaches the broker
```

### C3 — Rejections stay counted as failed requests

```
Given  a run containing R rejections and F failures of requests that were published
Then   global.failedRequests.count == R + F
And    global.allRequests.count is unchanged from the same run before this feature
And    no rejection appears only in the errors table
```

**Why it is stated separately**: the mechanism that would clean the aggregate — reporting rejections
as run-level errors — removes them from these counts. This clause forbids it.

### C4 — The declared name reports only requests that were sent

```
Given  a run mixing rejected and completing requests under one declared name
Then   details("⟨name⟩").responseTime.* equals the same statistic from a run
       containing only the completing requests
And    details("⟨name⟩").failedRequests counts only the published failures
And    details("⟨name⟩ [rejected: …]").failedRequests counts the rejections
```

### C5 — The run-wide aggregate is not claimed to be fixed

```
Given  a run containing rejections
Then   global.responseTime still includes their samples
And    the Migration Guide states this
And    the Migration Guide names details("⟨name⟩").responseTime and failedRequests
       as what to read instead
```

A documentation clause, and a real one: the feature must not imply a guarantee it cannot keep.

---

## Group B — What no longer reaches a run at all

### C6 — A request-reply with no consumer settings fails before the run starts

```
Given  a scenario containing a request-reply
And    a Kafka protocol with producer settings and no consumeSettings
When   the simulation is started
Then   it fails during setup, before any virtual user is injected
And    the failure names the missing consumeSettings and what it causes
And    zero request rows are produced
And    the same holds for the Scala DSL, the Java facade, and Kotlin, which share one builder
```

### C13 — A produce-only simulation with no consumer configuration is untouched

```
Given  a scenario that only publishes — kafka(name).topic(t).send(...)
And    a protocol built with properties(...), which carries no consumer settings
When   the simulation is started
Then   it builds, starts and runs exactly as it does today
And    its request count, success rate and reported figures are unchanged
And    the refusal in C6 never reaches it
```

Absent consumer configuration is the produce-only shape the DSL documents, not a defect. The guard
exists because C6's refusal must be scoped to the request-reply builder; a check applied where
publishing is built would break three published examples and four simulations in this repository.

**Also covered**: a simulation attaching a produce-only protocol at the `setUp` level *and*
containing one request-reply scenario is refused as a whole — including its produce-only scenarios.
Correct, and wider than the per-request failures it replaces, so C5's Migration Guide entry must
name it.

### C7 — A published request reports exactly what it reports today

```
Given  a request-reply that reaches the broker
When   it is matched, fails a check, times out, fails delivery, or is failed by a
       consumer failure or a channel stop
Then   its request name is ⟨name⟩ — never derived
And    its status, measured interval and failure text are unchanged from the
       previous release
```

The regression clause. Every scenario in `KafkaGatlingTest`, `KafkaConcurrencyLoadTest`,
`KafkaJavaapiMethodsGatlingTest` and all nine example simulations must pass unmodified.

---

## Group C — A reply that arrived is not reported as one that never came

### C8 — An uncorrelatable reply is counted on its channel

```
Given  a reply channel for (reply topic, matcher)
When   a record arrives whose correlation id the matcher cannot derive
Then   the channel's uncorrelatable-reply count increases by one
And    the record is dropped, as today
And    no pending request is failed or completed because of it
```

### C9 — A timeout on a channel that saw uncorrelatable replies says so

```
Given  a pending request that reaches its reply timeout
When   its channel's uncorrelatable-reply count is non-zero
Then   the reported failure states the timeout AND names how many replies arrived
       on that channel which the configured matcher could not correlate
When   the count is zero
Then   the reported failure is exactly "Reply timeout after <N> ms", unchanged
```

### C10 — Only the timeout path gains the clause

```
Given  a request failed by delivery failure, consumer failure, channel stop, a
       failed check, or a match-id reuse
Then   its message is unchanged, whatever the uncorrelatable-reply count is
```

A request that failed for a known reason must not be given a second, speculative one.

### C11 — A correlatable reply that matches nothing is not counted

```
Given  a record whose correlation id the matcher derives successfully
And    no pending request holds that id
Then   the uncorrelatable-reply count does not change
And    nothing is reported
```

Duplicates, late replies for requests already reported, and another scenario's traffic on a shared
channel are ordinary. Counting them would put a diagnosis on healthy runs.

---

## Group D — Tombstone-answering services have a supported shape

### C12 — Header correlation matches a tombstone reply end to end

```
Given  a service that answers with a tombstone, carrying a correlation header
And    a protocol correlating with matchByMessage over that header
When   the scenario runs against a real broker
Then   100% of tombstone replies are correlated to their request
And    the request's checks run against the reply
And    a body check against the absent payload fails cleanly, as v1.2.0 established,
       rather than the request timing out
```

Proved against a broker, not asserted in prose. The Migration Guide entry for #228 must name this
shape as the required one for such services, beside the existing v1.2.0 note on absent payloads.

---

## Compatibility ledger

| Surface | Status |
|---|---|
| Scala DSL signatures | Unchanged |
| `javaapi` signatures | Unchanged |
| `KafkaMatcher`, `KafkaProtocolMessage` | Unchanged |
| `KafkaProtocol` settings and defaults | Unchanged |
| Serialized wire formats | Unchanged |
| Reported request names for **published** requests | Unchanged (C7) |
| Reported request names for **rejections** | **Changed** (C1, C2) — Migration Guide entry required |
| Failure text on the reply-timeout path | **Changed when replies were uncorrelatable** (C9) |
| When a missing `consumeSettings` is reported, for a **request-reply** | **Changed** (C6) — build time, not per request |
| Produce-only protocols carrying no consumer settings | Unchanged (C13) |

No `!:` marker. Minor release. The breaking case for a downstream user is an assertion of the form
`details("⟨name⟩").failedRequests` over a name whose failures were rejections; the Migration Guide
must lead with it.
