package org.galaxio.gatling

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.check.Check
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction, StandardCharsets}

package object kafka {
  type KafkaCheck = Check[KafkaProtocolMessage]

  trait KafkaLogging extends StrictLogging {
    private val Utf8                = StandardCharsets.UTF_8
    private val BinaryPreviewLength = 24

    private def decodeUtf8(bytes: Array[Byte]): Option[String] = {
      val decoder = Utf8.newDecoder()
      decoder.onMalformedInput(CodingErrorAction.REPORT)
      decoder.onUnmappableCharacter(CodingErrorAction.REPORT)
      try Some(decoder.decode(ByteBuffer.wrap(bytes)).toString)
      catch {
        case _: CharacterCodingException => None
      }
    }

    private def isPrintable(text: String): Boolean =
      text.forall(ch => !Character.isISOControl(ch) || ch == '\n' || ch == '\r' || ch == '\t')

    private def escape(text: String): String =
      text.flatMap {
        case '\\' => "\\\\"
        case '"'  => "\\\""
        case '\n' => "\\n"
        case '\r' => "\\r"
        case '\t' => "\\t"
        case ch   => ch.toString
      }

    private def hexPreview(bytes: Array[Byte]): String =
      bytes
        .take(BinaryPreviewLength)
        .map(b => f"${b & 0xff}%02x")
        .mkString

    protected def describeBytes(bytes: Array[Byte]): String =
      Option(bytes) match {
        case None                         => "null"
        case Some(value) if value.isEmpty => "bytes(len=0)"
        case Some(value)                  =>
          decodeUtf8(value).filter(isPrintable) match {
            case Some(text) =>
              s"""text(utf-8,len=${value.length},"${escape(text)}")"""
            case None       =>
              val suffix = if (value.length > BinaryPreviewLength) "..." else ""
              s"bytes(len=${value.length},hex=${hexPreview(value)}$suffix)"
          }
      }

    protected def describeMessage(msg: KafkaProtocolMessage): String = {
      val headerCount = msg.headers.fold(0)(_.toArray.length)
      s"KafkaProtocolMessage(producerTopic=${msg.producerTopic}, consumerTopic=${msg.consumerTopic}, key=${describeBytes(msg.key)}, value=${describeBytes(msg.value)}, headers=$headerCount, responseCode=${msg.responseCode.getOrElse("none")})"
    }

    def logMessage(text: => String, msg: KafkaProtocolMessage): Unit = {
      logger.debug(text)
      logger.trace(describeMessage(msg))
    }
  }
}
