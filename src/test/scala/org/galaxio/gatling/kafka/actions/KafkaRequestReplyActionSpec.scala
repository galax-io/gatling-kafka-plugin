package org.galaxio.gatling.kafka.actions

import org.galaxio.gatling.kafka.client.{KafkaMessageTracker, KafkaTrackerProvider}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

class KafkaRequestReplyActionSpec extends AnyFunSuite {

  private val protocolMessage = KafkaProtocolMessage(
    key = "request-key".getBytes(),
    value = "response-value".getBytes(),
    inputTopic = "requests",
    outputTopic = "replies",
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
}
