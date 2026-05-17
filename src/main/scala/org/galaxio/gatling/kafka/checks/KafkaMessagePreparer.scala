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
import java.nio.charset.Charset
import scala.util.Try

trait KafkaMessagePreparer[P] extends Preparer[KafkaProtocolMessage, P]

object KafkaMessagePreparer {

  private def messageCharset(cfg: GatlingConfiguration, msg: KafkaProtocolMessage): Validation[Charset] =
    msg.headers
      .flatMap(h => Option(h.lastHeader("content_encoding")))
      .map(header => Try(Charset.forName(new String(header.value()))).toValidation)
      .getOrElse(cfg.core.charset.success)

  def stringBodyPreparer(configuration: GatlingConfiguration): KafkaMessagePreparer[String] =
    msg =>
      messageCharset(configuration, msg)
        .map(cs => if (msg.value.length > 0) new String(msg.value, cs) else "")

  val bytesBodyPreparer: KafkaMessagePreparer[Array[Byte]] = msg =>
    (if (msg.value.length > 0) msg.value else Array.emptyByteArray).success

  private val CharsParsingThreshold = 200 * 1000

  def jsonPathPreparer(
      jsonParsers: JsonParsers,
      configuration: GatlingConfiguration,
  ): Preparer[KafkaProtocolMessage, JsonNode] =
    msg =>
      messageCharset(configuration, msg)
        .flatMap(bodyCharset =>
          if (msg.value.length > CharsParsingThreshold)
            jsonParsers.safeParse(new ByteArrayInputStream(msg.value))
          else
            jsonParsers.safeParse(new String(msg.value, bodyCharset)),
        )

  def xmlPreparer(configuration: GatlingConfiguration): KafkaMessagePreparer[XdmNode] =
    msg =>
      safely("Could not parse response into a DOM Document: " + _) {
        messageCharset(configuration, msg).map(cs => XmlParsers.parse(new ByteArrayInputStream(msg.value), cs))
      }

  def avroPreparer[T <: GenericRecord: Serde](topic: String): KafkaMessagePreparer[T] = msg =>
    safely("Could not deserialize Avro message: " + _) {
      implicitly[Serde[T]].deserializer().deserialize(topic, msg.value).success
    }
}
