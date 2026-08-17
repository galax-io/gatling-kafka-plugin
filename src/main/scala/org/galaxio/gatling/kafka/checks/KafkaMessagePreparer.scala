package org.galaxio.gatling.kafka.checks

import com.fasterxml.jackson.databind.JsonNode
import io.gatling.commons.validation._
import io.gatling.core.check.Preparer
import io.gatling.core.check.xpath.XmlParsers
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.json.JsonParsers
import net.sf.saxon.s9api.XdmNode
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.io.ByteArrayInputStream
import java.nio.charset.{Charset, StandardCharsets}
import scala.util.Try

trait KafkaMessagePreparer[P] extends Preparer[KafkaProtocolMessage, P]

object KafkaMessagePreparer {

  private def messageCharset(cfg: GatlingConfiguration, msg: KafkaProtocolMessage): Validation[Charset] =
    msg.headers
      .flatMap(headers => Option(headers.lastHeader("content_encoding")))
      .map(header => Try(Charset.forName(new String(header.value(), StandardCharsets.UTF_8))).toValidation)
      .getOrElse(cfg.core.charset.success)

  /** Reported when a check is applied to a reply that carries no payload at all.
    *
    * Distinct from an empty payload on purpose. An empty value is something the system under test sent; `null` is the absence
    * of a value — a tombstone, which is ordinary traffic on a compacted topic. Collapsing the two would let `bodyString.is("")`
    * pass on a record that says "this key is deleted", which is a different finding from "the service replied with an empty
    * string" (issue #168).
    */
  private val NoPayload = "the reply carries no payload, so there is nothing to check against"

  // XML-specific, and deliberately not applied to the string/JSON preparers: labelling a malformed-JSON failure
  // "Could not parse response into a DOM Document" sends the reader looking for XML that was never involved. Those
  // paths already return a Validation of their own (`Try(...).toValidation`, `jsonParsers.safeParse`), so they need
  // no mapper at all.
  private val XmlErrorMapper = "Could not parse response into a DOM Document: " + _

  /** Absent payload short-circuits to a failure; everything else keeps its existing behaviour.
    *
    * The `msg.value.length` reads below all NPE on a tombstone, and that exception escapes `Check.check` into the tracker,
    * whose `try`/`finally` has no `catch` — so the virtual user is never continued and simply stops, with nothing in the report
    * (issue #168).
    */
  private[checks] def withPayload[T](msg: KafkaProtocolMessage)(f: => Validation[T]): Validation[T] =
    if (msg.value == null) NoPayload.failure else f

  def stringBodyPreparer(configuration: GatlingConfiguration): KafkaMessagePreparer[String] =
    msg =>
      withPayload(msg) {
        messageCharset(configuration, msg)
          .map(cs => if (msg.value.length > 0) new String(msg.value, cs) else "")
      }

  val bytesBodyPreparer: KafkaMessagePreparer[Array[Byte]] = msg =>
    withPayload(msg) {
      (if (msg.value.length > 0) msg.value else Array.emptyByteArray).success
    }

  private val CharsParsingThreshold = 200 * 1000

  def jsonPathPreparer(
      jsonParsers: JsonParsers,
      configuration: GatlingConfiguration,
  ): Preparer[KafkaProtocolMessage, JsonNode] =
    msg =>
      withPayload(msg) {
        messageCharset(configuration, msg)
          .flatMap(bodyCharset =>
            if (msg.value.length > CharsParsingThreshold)
              jsonParsers.safeParse(new ByteArrayInputStream(msg.value))
            else
              jsonParsers.safeParse(new String(msg.value, bodyCharset)),
          )
      }

  // These two never threw — `safely` already caught it — but they reported the absent payload as a
  // parse error, which sends the reader looking at their document instead of at the reply. Same guard,
  // so all five preparers give one answer to one condition (FR-009).
  def xmlPreparer(configuration: GatlingConfiguration): KafkaMessagePreparer[XdmNode] =
    msg =>
      withPayload(msg) {
        safely(XmlErrorMapper) {
          messageCharset(configuration, msg).map(cs => XmlParsers.parse(new ByteArrayInputStream(msg.value), cs))
        }
      }

}
