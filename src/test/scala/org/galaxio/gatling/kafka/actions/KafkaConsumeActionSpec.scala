package org.galaxio.gatling.kafka.actions

import io.gatling.commons.validation.Success
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaConsumeAttributes
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class KafkaConsumeActionSpec extends AnyFunSuite {

  private def attributes(
      consumeSettingsOverride: Option[Map[String, AnyRef]] = None,
      responseMatchExtractor: Option[KafkaProtocolMessage => Array[Byte]] = None,
  ): KafkaConsumeAttributes =
    KafkaConsumeAttributes(
      requestName = _ => Success("consume"),
      topic = _ => Success("events"),
      expectedMatchId = _ => Success("match-id".getBytes()),
      checks = Nil,
      silent = None,
      consumeSettingsOverride = consumeSettingsOverride,
      responseMatchExtractor = responseMatchExtractor,
      replyExtractions = Nil,
    )

  private val protocolMessage = KafkaProtocolMessage(
    key = "message-key".getBytes(),
    value = "message-value".getBytes(),
    inputTopic = "events",
    outputTopic = "events",
  )

  test("effectiveMessageMatcher keeps protocol matcher when no consume override is configured") {
    val protocolMatcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = "protocol-request".getBytes()
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = "protocol-response".getBytes()
    }

    val effective = KafkaConsumeAction.effectiveMessageMatcher(protocolMatcher, attributes())

    assert(effective eq protocolMatcher)
  }

  test("effectiveMessageMatcher uses action-level response extractor when configured") {
    val protocolMatcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = "protocol-request".getBytes()
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = "protocol-response".getBytes()
    }

    val effective = KafkaConsumeAction.effectiveMessageMatcher(
      protocolMatcher,
      attributes(responseMatchExtractor = Some(_.key)),
    )

    assert(effective.requestMatch(protocolMessage).sameElements("protocol-request".getBytes()))
    assert(effective.responseMatch(protocolMessage).sameElements("message-key".getBytes()))
  }

  test("effectiveConsumeSettings merges action override over protocol settings") {
    val effective = KafkaConsumeAction.effectiveConsumeSettings(
      org.galaxio.gatling.kafka.protocol.KafkaComponents(
        coreComponents = null,
        kafkaProtocol = KafkaProtocol(
          "topic",
          Map("bootstrap.servers" -> "protocol:9092"),
          Map("bootstrap.servers" -> "protocol:9092", "application.id" -> "base-app"),
          1.second,
          KafkaProtocol.KafkaKeyMatcher,
        ),
        trackersPoolFactory = null,
        senderProvider = null,
      ),
      attributes(consumeSettingsOverride = Some(Map("bootstrap.servers" -> "action:9092"))),
    )

    assert(effective == Map("bootstrap.servers" -> "action:9092", "application.id" -> "base-app"))
  }

  test("expectedMatchIdOrError rejects null tracking ids") {
    val result = KafkaConsumeAction.expectedMatchIdOrError(null)

    assert(result == Left("consume-only matcher returned null expected match id"))
  }
}
