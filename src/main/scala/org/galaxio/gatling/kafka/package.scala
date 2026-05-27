package org.galaxio.gatling

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.check.Check
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.Base64

package object kafka {
  type KafkaCheck = Check[KafkaProtocolMessage]

  object KafkaLogFormatter {
    private val PreviewBytes = 16

    def summarizeIdentifier(bytes: Array[Byte]): String =
      Option(bytes).map { value =>
        s"base64=${Base64.getEncoder.encodeToString(value)} bytes=${value.length}"
      }
        .getOrElse("null")

    def summarizePayload(bytes: Array[Byte]): String =
      Option(bytes).map { value =>
        val preview = Base64.getEncoder.encodeToString(value.take(PreviewBytes))
        val suffix  = if (value.length > PreviewBytes) "..." else ""
        s"bytes=${value.length} previewBase64=${preview}${suffix}"
      }
        .getOrElse("null")

    def summarizeMessage(msg: KafkaProtocolMessage): String =
      s"KafkaProtocolMessage(inputTopic=${msg.inputTopic}, outputTopic=${msg.outputTopic}, key=${summarizeIdentifier(msg.key)}, value=${summarizePayload(msg.value)}, headers=${msg.headers.fold(0)(_.toArray.length)}, responseCode=${msg.responseCode.getOrElse("none")})"
  }

  trait KafkaLogging extends StrictLogging {
    def logMessage(text: => String, msg: KafkaProtocolMessage): Unit = {
      logger.debug(text)
      logger.trace(KafkaLogFormatter.summarizeMessage(msg))
    }
  }
}
