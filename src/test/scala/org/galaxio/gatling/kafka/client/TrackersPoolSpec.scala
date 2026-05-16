package org.galaxio.gatling.kafka.client

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.atomic.AtomicReference

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

  test("awaitRunning starts the consumer and waits for RUNNING state") {
    val currentState = new AtomicReference(KafkaStreams.State.CREATED)
    val started      = new AtomicReference(false)
    val listenerRef  = new AtomicReference[KafkaStreams.StateListener]()

    TrackersPool.awaitRunning(
      () => currentState.get(),
      listener => listenerRef.set(listener),
      () => {
        started.set(true)
        currentState.set(KafkaStreams.State.RUNNING)
        Option(listenerRef.get()).foreach(_.onChange(KafkaStreams.State.RUNNING, KafkaStreams.State.REBALANCING))
      },
      timeoutMillis = 100,
    )

    assert(started.get())
    assert(listenerRef.get() != null)
  }

  test("awaitRunning fails when the consumer never reaches RUNNING") {
    val currentState = new AtomicReference(KafkaStreams.State.CREATED)

    val error = intercept[IllegalStateException] {
      TrackersPool.awaitRunning(
        () => currentState.get(),
        _ => (),
        () => currentState.set(KafkaStreams.State.REBALANCING),
        timeoutMillis = 10,
      )
    }

    assert(error.getMessage.contains("did not reach RUNNING state"))
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

  test("trackerProperties derives a unique application id per tracker key") {
    val firstMatcher  = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.key
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.key
    }
    val secondMatcher = new KafkaProtocol.KafkaMatcher {
      override def requestMatch(msg: KafkaProtocolMessage): Array[Byte]  = msg.value
      override def responseMatch(msg: KafkaProtocolMessage): Array[Byte] = msg.value
    }

    val baseSettings = Map[String, AnyRef](StreamsConfig.APPLICATION_ID_CONFIG -> "gatling-test-base")
    val firstProps   = TrackersPool.trackerProperties(
      baseSettings,
      TrackersPool.TrackerCacheKey("requests", "replies", firstMatcher, None),
    )
    val secondProps  = TrackersPool.trackerProperties(
      baseSettings,
      TrackersPool.TrackerCacheKey("requests", "replies", secondMatcher, None),
    )

    assert(firstProps.getProperty(StreamsConfig.APPLICATION_ID_CONFIG).startsWith("gatling-test-base-"))
    assert(secondProps.getProperty(StreamsConfig.APPLICATION_ID_CONFIG).startsWith("gatling-test-base-"))
    assert(
      firstProps.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) != secondProps.getProperty(
        StreamsConfig.APPLICATION_ID_CONFIG,
      ),
    )
  }
}
