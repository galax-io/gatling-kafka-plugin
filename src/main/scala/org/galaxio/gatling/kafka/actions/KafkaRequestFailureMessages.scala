package org.galaxio.gatling.kafka.actions

import org.galaxio.gatling.kafka.protocol.KafkaProtocol.{KafkaKeyMatcher, KafkaMatcher, KafkaMessageMatcher, KafkaValueMatcher}

/** Visible to the whole plugin, not just `actions`: [[KafkaRequestFailureMessages.failureCause]] is the one rule for turning a
  * `Throwable` into what a report shows, and the tracker and the tracker pool build failure messages too. Scoped to `actions`
  * it could only ever be applied at the three call sites that happen to live there.
  */
private[kafka] object KafkaRequestFailureMessages {

  /** A request-reply outcome in which no record reached the broker.
    *
    * The two share the one property that matters and differ in nothing else that a report can show: neither measured the system
    * under test. One is decided instantly, the other after the whole consumer-assignment wait, and both are relocated off the
    * declared request name for the same reason.
    */
  sealed abstract class RejectionKind(val label: String)

  object RejectionKind {

    /** The configured matcher returned nothing for this request (issue #167). Near-zero interval. */
    case object NoCorrelationId extends RejectionKind("no correlation id")

    /** The reply channel could not be established. Carries the real wait, which is the one that inflates a percentile. */
    case object NoReplyChannel extends RejectionKind("no reply channel")

    /** The producer never delivered the record, or it was registered with no id to correlate on.
      *
      * The largest of the three by sample size, and the one most easily missed: with Kafka's default `delivery.timeout.ms` of
      * two minutes, a broker outage puts a ~120 000 ms sample on every in-flight request. Those requests meet this type's
      * definition exactly — no record reached the broker — so leaving them on the declared name would have left the biggest
      * instance of issue #227 in place while the guide claimed the figure was true.
      */
    case object NotDelivered extends RejectionKind("not delivered")
  }

  /** The name a rejection is reported under: the declared name, marked with what stopped it.
    *
    * The request name is the only field a plugin controls that segregates response-time samples. Gatling keys a digest on
    * `(name, group, status)` and on `(name, group, None)`, and its assertion API resolves `responseTime` against the blended
    * bucket with no successful-only scope — so a rejection left on the declared name contributes a sample the system under test
    * never produced to the percentile a simulation asserts on (issue #227). The response-code slot cannot carry this instead,
    * for the same reason [[failureCause]] puts the failure kind in the message: it is discarded before a run's data is written.
    *
    * A suffix rather than a wholly different name, so the two rows sort together and a reader can see which request they belong
    * to. Precedent is Gatling's own redirect naming, `s"$requestName Redirect $n"`, which segregates a different kind of
    * interaction the same way.
    *
    * The bracketed marker cannot be proven collision-free — request names are free-form strings, and Gatling's redirect
    * strategy carries the identical exposure. It is documented as reserved in the Migration Guide, and that is the whole
    * mitigation.
    */
  def rejectedRequestName(declaredName: String, kind: RejectionKind): String =
    s"$declaredName [rejected: ${kind.label}]"

  /** Reported when a scenario contains a request-reply and its protocol carries no consumer configuration.
    *
    * A precondition, not a per-request failure. Whether the protocol has consumer settings depends on no session data, so the
    * answer is the same for every request the scenario will ever issue — and it used to be given once per request, as a KO
    * carrying a near-zero interval, for a misconfiguration no run could recover from (issue #227).
    *
    * Absent consumer configuration is not itself an error: it is the produce-only shape `KafkaProtocolBuilder.properties(...)`
    * documents, and publishing never asks for a reply channel. What cannot work is a request-reply that needs one and has none,
    * which is why this is checked where request-reply actions are built and nowhere else.
    *
    * Names `bootstrap.servers`, not the DSL call, because that is what the gate actually reads: the tracker pool is absent iff
    * the consumer properties lack that key, so `consumeSettings("group.id" -> …)` — a perfectly compilable protocol — reaches
    * this too. Telling that user to "add consumeSettings" would be advice they had already followed, which is the defect
    * [[remedyFor]] exists to prevent one layer over. The producer side words it the same way: "Producer settings don't set the
    * required 'bootstrap.servers' parameter".
    */
  val consumerSettingsRequired: String =
    "Request-reply requires consumer settings with a 'bootstrap.servers' entry: without one there is no reply channel, so " +
      "no request-reply in this scenario could ever be answered. Call consumeSettings(...) on the protocol and give it " +
      "'bootstrap.servers' — if you already call consumeSettings, that entry is what is missing. Or publish without waiting " +
      "for a reply: kafka(name).topic(...).send(...) needs no consumer."

  def sendFailure(error: String): String =
    s"Failed to send request to Kafka broker: ${Option(error).getOrElse("unknown error")}"

  def sendFailure(exception: Throwable): String = sendFailure(failureCause(exception))

  /** A failure described by the kind of thing it is as well as by its text: `TimeoutException: Expiring 1 record(s)`.
    *
    * The kind is what separates "the broker rejected this record" from "the client was misconfigured" when reading a run
    * afterwards, and the message is the only place it can go. Gatling takes a response code alongside the message on
    * `logResponse` and then discards it: its file serializer writes groups, name, timestamps, status and message and nothing
    * else, its console writer keys the error histogram by message, and the record the report reads back has no field for it at
    * all — so a kind kept out of the message reaches no report (issue #254).
    *
    * The cause is appended when there is one, because the failures that most need naming arrive wrapped: the tracker pool
    * reports every consumer fault as `IllegalStateException("Kafka consumer failed; …", cause)`, and Kafka wraps its own as
    * `KafkaException("Failed to construct kafka producer", cause)`. Reading only the outer message would report one constant
    * string for every broker outage, ACL rejection and SASL misconfiguration alike — exactly the distinction this exists to
    * preserve.
    */
  def failureCause(exception: Throwable): String = {
    val head  = describe(exception)
    val cause = exception.getCause
    if (cause == null || (cause eq exception)) head else s"$head (caused by ${describe(cause)})"
  }

  /** One `Throwable`, named and described.
    *
    * `getSimpleName` has two JVM quirks and both reach a report: a Scala `object` reports a trailing `$` — the same hazard
    * `KafkaRequestReplyAction.matcherName` strips — and an anonymous subclass reports the empty string, which would open the
    * message with a bare `": "`. A null or blank message leaves the kind standing alone rather than reporting
    * `TimeoutException: null`; `isBlank` rather than `trim.nonEmpty` because the latter treats a non-breaking space as text.
    */
  private def describe(error: Throwable): String = {
    val simple  = error.getClass.getSimpleName.stripSuffix("$")
    val kind    = if (simple.nonEmpty) simple else error.getClass.getName
    val message = error.getMessage
    if (message == null || message.isBlank) kind else s"$kind: $message"
  }

  /** How the configured matcher is named in a failure message.
    *
    * Two hazards, the same two [[failureCause]] handles for exceptions: a Scala `object` reports a trailing `$` from
    * `getSimpleName`, which would print `KafkaKeyMatcher$`, and an anonymous `new KafkaMatcher { … }` — reachable, since the
    * trait and `KafkaProtocol` are both public — reports the empty string.
    *
    * Here rather than in the action because the tracker names the matcher too, and a rule stated twice is a rule that drifts.
    */
  def matcherName(matcher: KafkaMatcher): String = {
    val simple = matcher.getClass.getSimpleName.stripSuffix("$")
    if (simple.nonEmpty) simple else matcher.getClass.getName
  }

  /** Reported when a request-reply supplies nothing the configured matcher can correlate a reply on — in practice a request
    * with no key under the default `matchByKey`.
    *
    * Such a request used to be tracked under an empty correlation id, which every other keyless request shared, so a reply
    * resolved whichever one happened to occupy that slot: one virtual user was credited with another's answer and the real
    * owner timed out (issue #167). There is no correct correlation to perform here, so the request is failed before it is sent
    * rather than sent and mismatched.
    *
    * Names the remedy as well as the cause: the fix is a scenario change, and a message that only states the problem leaves the
    * reader to guess which of the three matchers they should be using.
    */
  def missingCorrelationId(matcherName: String, remedy: String): String =
    s"Cannot correlate a reply: this request supplies no value for the configured message matcher ($matcherName). $remedy"

  /** The remedy half of [[missingCorrelationId]], chosen from what the matcher reads.
    *
    * Parameterising the diagnosis but hardcoding the fix produced advice that contradicted itself: a `matchByValue` user whose
    * payload was null was told to "correlate with matchByValue", which they already were. What to do about an absent id depends
    * on what the matcher looks at, so it is derived from the same place the name is.
    */
  def remedyFor(matcher: KafkaMatcher): String = matcher match {
    case KafkaKeyMatcher        =>
      "Set a key on the request, or correlate on something it already carries with matchByValue/matchByMessage."
    case KafkaValueMatcher      =>
      "matchByValue correlates on the payload, so the payload cannot be null — give this request a body, or " +
        "correlate on a key or header instead."
    case _: KafkaMessageMatcher =>
      "matchByMessage correlates on whatever your extractor returns, and it returned nothing for this request. " +
        "Return a value that is unique per request — and note that returning an empty array instead of null is not a fix: " +
        "every request that does so shares one correlation id."
    case _                      =>
      "The configured matcher returned nothing for this request. Correlate on a value that is present and unique per request."
  }
}
