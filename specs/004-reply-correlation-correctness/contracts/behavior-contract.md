# Observable Behaviour Contract: Reply Correlation Correctness

**Feature**: `004-reply-correlation-correctness` | **Date**: 2026-08-07

This plugin's external interface is the published Scala DSL, the `javaapi` facade, and the observable
behaviour of a run. There is no HTTP or CLI surface, so the contract that matters here is **behavioural**:
what a simulation author writes, and what the report says afterwards.

**Signature contract: unchanged.** No public type, method, or default moves. Everything below is a
behaviour delta, and each one is the correction of a defect.

---

## C1 — Request-reply with no key, correlating on the key

**Signature**: unchanged.

```scala
kafka("req").requestReply
  .requestTopic("in").replyTopic("out")
  .send[String]("payload-with-no-key")   // no key overload
// protocol: .matchByKey  (the default)
```

| | Before | After |
|---|---|---|
| On the wire | key = `Array.emptyByteArray` | request is never sent |
| Correlation | every such request shares one bucket | none — rejected before registration |
| Report | OK or KO, possibly from another user's reply | **KO at issue time** |
| Message | — | names the missing identity for the configured matcher |
| Timing | after the reply timeout, or on a wrong match | immediate |

**Breaking for**: simulations that today appear to work this way. They were reporting wrong results —
the change surfaces that rather than causing it.

**Migration**: set a key, or correlate on something the request carries:

```scala
.send[String, String]("#{correlationId}", "payload")   // give it a key
// — or —
.matchByValue                                           // correlate on the payload
.matchByMessage(msg => msg.headers…)                    // correlate on a header
```

---

## C2 — Request-reply with no key, correlating on a non-key field

**Signature**: unchanged. **Report: unchanged in shape, correct in content.**

```scala
// protocol: .matchByValue  or  .matchByMessage(...)
```

| | Before | After |
|---|---|---|
| On the wire | key = `Array.emptyByteArray` | key = `null` |
| Correlation | on value/extractor (already worked) | unchanged |
| Partition placement | single partition (`murmur2` of empty is constant) | broker's keyless placement — see C4 |

**Breaking for**: nothing in the report. **Changes**: which partition a keyless message lands on — see C4.

---

## C3 — A reply whose payload is absent

**Signature**: unchanged. Applies to `bodyString`, `substring`, `bodyBytes`, `jsonPath`, `jmesPath`.

```scala
kafka("req").requestReply
  .requestTopic("in").replyTopic("out")
  .send[String, String]("k", "v")
  .check(bodyString.is("expected"))
```

| Reply payload | Before | After |
|---|---|---|
| `null` (tombstone) | NPE out of `Check.check`; **virtual user stalls** — no OK, no KO, no next action | **KO** naming the absent payload; user continues |
| empty (`length == 0`) | `""` / empty bytes, check evaluated normally | **unchanged** |
| present | parsed | **unchanged** |

**Breaking for**: runs against compacted topics or services that answer with deletion markers. Those
runs were losing virtual users silently; they now report failures instead. The user count at the end of
a run will match the count at the start, which it did not before.

**Note**: `bodyString.is("")` does **not** pass on a tombstone. Absent and empty stay distinct — the
plugin already draws that distinction in its logging (`"null"` vs `"bytes(len=0)"`) and now draws it in
checks too.

**Additional guarantee (FR-008)**: independent of the preparers, *any* check that throws now yields a
KO and continues the user. No check can strand a virtual user.

---

## C4 — Key absence on the wire for keyless messages

**Signature**: unchanged. Applies to **all** sends, request-reply and fire-and-forget alike.

| | Before | After |
|---|---|---|
| Message with a key | key on the wire; partition = `hash(key) % n` | **unchanged** — per-key ordering preserved |
| Message with no key | empty byte array — a *present* key | absent key (`null`) |
| Placement of a keyless message | one partition, always (`murmur2` of empty is constant) | whatever the broker does for keyless records |
| Keyless send to a compacted topic | accepted (empty key satisfies the key requirement) | **rejected** with `INVALID_RECORD` |

**Breaking for**: scenarios publishing keyless records to a log-compacted topic — those go from passing
to failing every request. Kafka requires a key there, and `hasKey()` is `key != null`, so an empty key
passed the check and an absent one does not. The records were never valid on that topic.

**Changes**: measured throughput and consumer-group behaviour of keyless scenarios, because the previous
numbers described a single-partition workload the author did not ask for.

**Deliberately not specified**: how records are distributed once the key is absent. That is the broker's
decision and it changed in Kafka 3.3 (keyless records are batched stickily rather than round-robin), so
asserting spread would pin behaviour the plugin does not own.

---

## C5 — Reply channel readiness

**Signature**: unchanged; internal to `DynamicKafkaConsumer`, but the guarantee is observable.

| | Before | After |
|---|---|---|
| "Ready" means | the broker has assigned partitions | assigned **and** fetch position resolved |
| Reply published just after ready | may be skipped under `auto.offset.reset=latest` | delivered |
| Reported effect | reply timeout against a system that answered | correct success |
| Position unresolvable | n/a | that topic's readiness fails; other topics and the pool are unaffected |

**Breaking for**: nothing. Strictly removes false timeouts.

**Default unchanged**: `auto.offset.reset` stays `latest` (`KafkaProtocolBuilder.withDefaultAutoReset`).
The fix makes `latest` safe rather than changing it.

---

## C6 — Failure messages (report content)

Messages appear in the report's KO reason and are what an engineer acts on. Each case gets the message
that fits it — the point of separating absent from reused.

| Situation | Message |
|---|---|
| No identity for the configured matcher | names the missing identity and how to supply one — **new** |
| Identity reused while in flight | "Match id reused while a request was still in flight…" — existing, unchanged |
| Reply payload absent | names the absent payload as the cause — **new** |
| Check threw | terminal KO carrying the failure — **new** |
| Reply timeout | existing, unchanged — and now means the system genuinely did not answer |

---

## Compatibility summary

| Contract | Signature | Behaviour | Version impact |
|---|---|---|---|
| C1 keyless + key matching | unchanged | **breaking** (was wrong) | `feat` + Migration Guide |
| C2 keyless + other matching | unchanged | corrected | `feat` |
| C3 absent payload | unchanged | **breaking** (was a stall) | `fix` + Migration Guide |
| C4 key absence on the wire | unchanged | **breaking** (compacted topics) | `fix` + Migration Guide |
| C5 readiness | unchanged | strictly better | `feat` |
| C6 messages | unchanged | additive | — |

**Version**: minor (`1.2.0`). No `!:` / `BREAKING CHANGE` marker — no consumer's code stops compiling,
and `ExampleSmokeValidation` must stay green as the gate on that claim. The behaviour changes are real
and require a `README.md` Migration Guide entry in the same PR (FR-017), covering C1, C3 and C4 with
the remediation shown for each above.
