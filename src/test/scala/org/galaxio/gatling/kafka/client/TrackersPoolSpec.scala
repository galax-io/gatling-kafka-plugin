package org.galaxio.gatling.kafka.client

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.header.internals.RecordHeaders
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.scalatest.funsuite.AnyFunSuite

class TrackersPoolSpec extends AnyFunSuite {

  private val protocolMessage = KafkaProtocolMessage(
    key = "request-id".getBytes(),
    value = "reply-id".getBytes(),
    inputTopic = "requests",
    outputTopic = "replies",
  )

  test("responseMatchIdOrError returns response match id independently from request match id") {
    val matcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.key
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }

    val result = TrackersPool.responseMatchIdOrError(protocolMessage, matcher)

    assert(result.exists(_.sameElements("reply-id".getBytes())))
  }

  test("responseMatchIdOrError rejects null response match id") {
    val matcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.key
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = null
    }

    val result = TrackersPool.responseMatchIdOrError(protocolMessage, matcher)

    assert(result == Left("response matcher returned null match id"))
  }

  test("tracker cache key isolates same topic across different matchers") {
    val requestMatcher  = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.key
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.key
    }
    val responseMatcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.value
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }

    val left  = TrackersPool.TrackerCacheKey("requests", "replies", requestMatcher, None)
    val right = TrackersPool.TrackerCacheKey("requests", "replies", responseMatcher, None)

    assert(left != right)
  }

  test("consumerProperties derives a unique group id per tracker key") {
    val firstMatcher  = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.key
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.key
    }
    val secondMatcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.value
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }

    val baseSettings = Map[String, AnyRef](ConsumerConfig.GROUP_ID_CONFIG -> "gatling-test-base")
    val firstProps   = TrackersPool.consumerProperties(
      baseSettings,
      TrackersPool.TrackerCacheKey("requests", "replies", firstMatcher, None),
    )
    val secondProps  = TrackersPool.consumerProperties(
      baseSettings,
      TrackersPool.TrackerCacheKey("requests", "replies", secondMatcher, None),
    )

    assert(firstProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG).startsWith("gatling-test-base-"))
    assert(secondProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG).startsWith("gatling-test-base-"))
    assert(
      firstProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG) != secondProps.getProperty(
        ConsumerConfig.GROUP_ID_CONFIG,
      ),
    )
  }

  test("consumedMessage keeps Kafka headers for header-based matching") {
    val headers = new RecordHeaders()
    headers.add("correlation-id", "corr-123".getBytes())

    val message = TrackersPool.consumedMessage(
      key = "key".getBytes(),
      value = "value".getBytes(),
      inputTopic = "requests",
      outputTopic = "replies",
      headers = headers,
    )

    assert(
      message.headers
        .flatMap(h => Option(h.lastHeader("correlation-id")))
        .map(_.value())
        .exists(_.sameElements("corr-123".getBytes())),
    )
  }
}
