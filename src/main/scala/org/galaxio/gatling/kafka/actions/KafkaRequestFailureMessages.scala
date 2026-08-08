package org.galaxio.gatling.kafka.actions

import org.galaxio.gatling.kafka.protocol.KafkaProtocol.{KafkaKeyMatcher, KafkaMatcher, KafkaMessageMatcher, KafkaValueMatcher}

private[actions] object KafkaRequestFailureMessages {
  def buildFailure(error: String): String =
    s"Failed to build request: ${Option(error).getOrElse("unknown error")}"

  def sendFailure(error: String): String =
    s"Failed to send request to Kafka broker: ${Option(error).getOrElse("unknown error")}"

  def sendFailure(exception: Throwable): String =
    sendFailure(Option(exception.getMessage).getOrElse(exception.getClass.getSimpleName))

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
