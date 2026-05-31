package org.galaxio.gatling.kafka.client

import io.gatling.commons.util.Clock
import io.gatling.core.actor.ActorSystem
import io.gatling.core.stats.RecordingStatsEngine
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt

class KafkaMessageTrackerPoolSpec extends munit.FunSuite {

  test("tracker fails fast after consumer failure and does not reuse cached trackers") {
    val actorSystem = new ActorSystem()
    try {
      val pool = new KafkaMessageTrackerPool(
        Map(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> "localhost:0",
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getName,
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
          "key.serializer"                               -> classOf[ByteArraySerializer].getName,
          "value.serializer"                             -> classOf[ByteArraySerializer].getName,
        ),
        actorSystem,
        new RecordingStatsEngine,
        new StubClock(0L),
      )

      val field = classOf[KafkaMessageTrackerPool].getDeclaredField("consumerFailure")
      field.setAccessible(true)
      field.get(pool).asInstanceOf[AtomicReference[Exception]].set(new RuntimeException("boom"))

      val thrown = intercept[IllegalStateException] {
          pool.tracker(
          producerTopic = "producer-topic",
          consumerTopic = "consumer-topic",
          messageMatcher = KafkaKeyMatcher,
          responseTransformer = None,
          timeout = 1.second,
        )
      }

      assert(thrown.getCause != null)
      assertEquals(thrown.getCause.getMessage, "boom")
    } finally {
      actorSystem.close()
    }
  }

  private final class StubClock(now: Long) extends Clock {
    override def nowMillis: Long = now
  }
}
