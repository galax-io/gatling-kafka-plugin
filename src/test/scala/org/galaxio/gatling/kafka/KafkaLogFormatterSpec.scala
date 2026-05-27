package org.galaxio.gatling.kafka

import io.gatling.commons.validation.Success
import org.apache.kafka.common.header.internals.RecordHeaders
import org.galaxio.gatling.kafka.checks.KafkaMessagePreparer
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets

class KafkaLogFormatterSpec extends AnyFunSuite {

  test("summarizeIdentifier renders deterministic base64 metadata") {
    val bytes = "key".getBytes(StandardCharsets.UTF_8)

    val summary = KafkaLogFormatter.summarizeIdentifier(bytes)

    assert(summary == "base64=a2V5 bytes=3")
  }

  test("summarizePayload avoids raw binary strings and truncates preview") {
    val bytes = Array.tabulate[Byte](32)(_.toByte)

    val summary = KafkaLogFormatter.summarizePayload(bytes)

    assert(summary.startsWith("bytes=32 previewBase64="))
    assert(summary.endsWith("..."))
  }

  test("summarizeMessage reports metadata instead of full raw payloads") {
    val headers = new RecordHeaders().add("trace-id", "123".getBytes(StandardCharsets.UTF_8))
    val message = KafkaProtocolMessage(
      key = "request-key".getBytes(StandardCharsets.UTF_8),
      value = Array[Byte](1, 2, 3, 4),
      inputTopic = "requests",
      outputTopic = "replies",
      headers = Some(headers),
    )

    val summary = KafkaLogFormatter.summarizeMessage(message)

    assert(summary.contains("inputTopic=requests"))
    assert(summary.contains("outputTopic=replies"))
    assert(summary.contains("key=base64=cmVxdWVzdC1rZXk= bytes=11"))
    assert(summary.contains("value=bytes=4 previewBase64=AQIDBA=="))
    assert(summary.contains("headers=1"))
  }

  test("contentEncodingCharset decodes charset names with utf-8") {
    val headers = new RecordHeaders().add("content_encoding", "UTF-16LE".getBytes(StandardCharsets.UTF_8))

    val charset = KafkaMessagePreparer.contentEncodingCharset(headers)

    assert(charset.contains(Success(StandardCharsets.UTF_16LE)))
  }
}
