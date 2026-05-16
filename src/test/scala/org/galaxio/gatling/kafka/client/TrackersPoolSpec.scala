package org.galaxio.gatling.kafka.client

import org.apache.kafka.streams.KafkaStreams
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.atomic.AtomicReference

class TrackersPoolSpec extends AnyFunSuite {

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
}
