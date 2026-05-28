package org.galaxio.gatling.kafka

import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

class KafkaLoggingSpec extends munit.FunSuite {

  private object TestLogging extends KafkaLogging {
    def bytes(bytes: Array[Byte]): String          = describeBytes(bytes)
    def message(msg: KafkaProtocolMessage): String = describeMessage(msg)
  }

  test("describeBytes renders printable UTF-8 text deterministically") {
    val value = "payload".getBytes(java.nio.charset.StandardCharsets.UTF_8)

    assertEquals(TestLogging.bytes(value), """text(utf-8,len=7,"payload")""")
  }

  test("describeBytes falls back to hex preview for binary payloads") {
    val value = Array[Byte](0x00, 0x01, 0x7f, 0x0a, 0x2a)

    assertEquals(TestLogging.bytes(value), "bytes(len=5,hex=00017f0a2a)")
  }

  test("describeMessage summarises payload metadata without raw arrays") {
    val msg = KafkaProtocolMessage(
      key = "key".getBytes(java.nio.charset.StandardCharsets.UTF_8),
      value = Array[Byte](0x00, 0x7f),
      producerTopic = "producer-topic",
      consumerTopic = "consumer-topic",
    )

    assertEquals(
      TestLogging.message(msg),
      """KafkaProtocolMessage(producerTopic=producer-topic, consumerTopic=consumer-topic, key=text(utf-8,len=3,"key"), value=bytes(len=2,hex=007f), headers=0, responseCode=none)""",
    )
  }
}
