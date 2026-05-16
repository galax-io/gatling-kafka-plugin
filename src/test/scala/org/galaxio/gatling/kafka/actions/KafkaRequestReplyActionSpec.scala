package org.galaxio.gatling.kafka.actions

import io.gatling.commons.validation._
import io.gatling.core.session.Session
import org.galaxio.gatling.kafka.client.{KafkaMessageTracker, KafkaTrackerProvider}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.KafkaRequestReplyAttributes
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

class KafkaRequestReplyActionSpec extends AnyFunSuite {

  private val protocolMessage = KafkaProtocolMessage(
    key = "request-key".getBytes(),
    value = "response-value".getBytes(),
    inputTopic = "requests",
    outputTopic = "replies",
  )

  private def attributes(
      producerSettingsOverride: Option[Map[String, AnyRef]] = None,
      consumeSettingsOverride: Option[Map[String, AnyRef]] = None,
      requestMatchExtractor: Option[KafkaProtocolMessage => Array[Byte]] = None,
      responseMatchExtractor: Option[KafkaProtocolMessage => Array[Byte]] = None,
  ): KafkaRequestReplyAttributes[String, String] =
    KafkaRequestReplyAttributes(
      requestName = _ => Success("request"),
      inputTopic = _ => Success("requests"),
      outputTopic = _ => Success("replies"),
      key = _ => Success("key"),
      value = _ => Success("value"),
      headers = None,
      keySerializer = org.apache.kafka.common.serialization.Serdes.String.serializer(),
      valueSerializer = org.apache.kafka.common.serialization.Serdes.String.serializer(),
      checks = Nil,
      silent = None,
      producerSettingsOverride = producerSettingsOverride,
      consumeSettingsOverride = consumeSettingsOverride,
      requestMatchExtractor = requestMatchExtractor,
      responseMatchExtractor = responseMatchExtractor,
      replyExtractions = Nil,
    )

  test("requestMatchIdOrError returns request match id when matcher is valid") {
    val matcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.key
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }

    val result = KafkaRequestReplyAction.requestMatchIdOrError(protocolMessage, matcher)

    assert(result.exists(_.sameElements("request-key".getBytes())))
  }

  test("requestMatchIdOrError rejects null request match id") {
    val matcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = null
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }

    val result = KafkaRequestReplyAction.requestMatchIdOrError(protocolMessage, matcher)

    assert(result == Left("request matcher returned null match id"))
  }

  test("prepareTrackerOrError prepares tracker after request match extraction") {
    val events   = ArrayBuffer.empty[String]
    val matcher  = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte] = {
        events += "match"
        msg.key
      }

      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }
    val trackers = new KafkaTrackerProvider {
      override def tracker(
          inputTopic: String,
          outputTopic: String,
          messageMatcher: KafkaProtocol.KafkaMatcher,
          responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
      ): KafkaMessageTracker = {
        events += s"tracker:$inputTopic->$outputTopic"
        null.asInstanceOf[KafkaMessageTracker]
      }
    }

    val result = KafkaRequestReplyAction.prepareTrackerOrError(protocolMessage, trackers, matcher)

    assert(result.exists(_.matchId.sameElements("request-key".getBytes())))
    assert(events.toList == List("match", "tracker:requests->replies"))
  }

  test("prepareTrackerOrError does not create tracker when request match id is null") {
    val events   = ArrayBuffer.empty[String]
    val matcher  = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte] = {
        events += "match"
        null
      }

      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }
    val trackers = new KafkaTrackerProvider {
      override def tracker(
          inputTopic: String,
          outputTopic: String,
          messageMatcher: KafkaProtocol.KafkaMatcher,
          responseTransformer: Option[KafkaProtocolMessage => KafkaProtocolMessage],
      ): KafkaMessageTracker = {
        events += "tracker"
        null.asInstanceOf[KafkaMessageTracker]
      }
    }

    val result = KafkaRequestReplyAction.prepareTrackerOrError(protocolMessage, trackers, matcher)

    assert(result == Left("request matcher returned null match id"))
    assert(events.toList == List("match"))
  }

  test("effectiveMessageMatcher keeps protocol defaults for missing sides and action overrides for configured sides") {
    val protocolMatcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = "protocol-request".getBytes()
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = "protocol-response".getBytes()
    }

    val effective = KafkaRequestReplyAction.effectiveMessageMatcher(
      protocolMatcher,
      attributes(
        requestMatchExtractor = Some(_ => "action-request".getBytes()),
      ),
    )

    assert(effective.requestMatch(protocolMessage).sameElements("action-request".getBytes()))
    assert(effective.responseMatch(protocolMessage).sameElements("protocol-response".getBytes()))
  }

  test("effectiveProducerSettings merges action override over protocol settings") {
    val effective = KafkaRequestReplyAction.effectiveProducerSettings(
      org.galaxio.gatling.kafka.protocol.KafkaComponents(
        coreComponents = null,
        kafkaProtocol = KafkaProtocol(
          "topic",
          Map("bootstrap.servers" -> "protocol:9092", "acks" -> "all"),
          Map("bootstrap.servers" -> "protocol:9092"),
          scala.concurrent.duration.DurationInt(1).second,
          KafkaProtocol.KafkaKeyMatcher,
        ),
        trackersPoolFactory = null,
        senderProvider = null,
      ),
      attributes(producerSettingsOverride = Some(Map("bootstrap.servers" -> "action:9092"))),
    )

    assert(effective == Map("bootstrap.servers" -> "action:9092", "acks" -> "all"))
  }

  test("effectiveConsumeSettings merges action override over protocol settings") {
    val effective = KafkaRequestReplyAction.effectiveConsumeSettings(
      org.galaxio.gatling.kafka.protocol.KafkaComponents(
        coreComponents = null,
        kafkaProtocol = KafkaProtocol(
          "topic",
          Map("bootstrap.servers" -> "protocol:9092"),
          Map("bootstrap.servers" -> "protocol:9092", "application.id" -> "base-app"),
          scala.concurrent.duration.DurationInt(1).second,
          KafkaProtocol.KafkaKeyMatcher,
        ),
        trackersPoolFactory = null,
        senderProvider = null,
      ),
      attributes(consumeSettingsOverride = Some(Map("bootstrap.servers" -> "action:9092"))),
    )

    assert(effective == Map("bootstrap.servers" -> "action:9092", "application.id" -> "base-app"))
  }
}
