package org.galaxio.gatling.kafka.integration

import com.dimafeng.testcontainers.ConfluentKafkaContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import io.gatling.commons.util.Clock
import io.gatling.core.actor.ActorSystem
import io.gatling.core.stats.RecordingStatsEngine
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.galaxio.gatling.kafka.client.KafkaMessageTrackerPool
import org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

/** Startup coverage for issue #143 — the shared reply-receiving machinery must not fail merely because no reply topic was
  * requested for a while after it was built.
  *
  * The pool is built by `ProtocolKey.newComponents` for any protocol carrying `bootstrap.servers` in its consume settings —
  * before a single virtual user runs. So its consumer starts long before the first reply topic is requested, and any ramp,
  * warm-up or `nothingFor` leaves it running with nothing subscribed. It used to reach `poll()` in that state, which threw,
  * `markConsumerFailed` latched it, and every later `acquireTracker` failed with "Kafka consumer failed; tracker pool can no
  * longer be used" for the rest of the run.
  *
  * These tests need no shortened timeout to reach that state, because there is no longer a timed initialization phase to wait
  * out: the consumer waits for a subscription request and is woken when one arrives.
  */
class ConsumerStartupSpec extends munit.FunSuite with TestContainerForAll {

  override val containerDef: ConfluentKafkaContainer.Def = ConfluentKafkaContainer.Def()

  /** How long the pool is left idle before the tests act. Many loop turns, all of which used to be one throw. */
  private val IdleBeforeFirstUse: FiniteDuration = 3.seconds

  private def consumerSettings(bootstrap: String): Map[String, AnyRef] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> bootstrap,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "earliest",
  )

  private def createTopic(bootstrap: String, name: String): Unit = {
    val admin = AdminClient.create(
      Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap).asJava,
    )
    try admin.createTopics(List(new NewTopic(name, 1, 1.toShort)).asJava).all().get(30, TimeUnit.SECONDS)
    finally admin.close()
  }

  private final class SystemClock extends Clock {
    override def nowMillis: Long = System.currentTimeMillis()
  }

  private def withPool(bootstrap: String)(body: KafkaMessageTrackerPool => Unit): Unit = {
    val actorSystem = new ActorSystem()
    try {
      val pool = new KafkaMessageTrackerPool(
        consumerSettings(bootstrap),
        actorSystem,
        new RecordingStatsEngine,
        new SystemClock,
      )
      body(pool)
    } finally actorSystem.close()
  }

  test("a reply topic requested long after the pool was built is still served") {
    withContainers { kafka =>
      val bootstrap  = kafka.bootstrapServers
      val replyTopic = "startup-late-reply"
      createTopic(bootstrap, replyTopic)

      withPool(bootstrap) { pool =>
        // Nothing is requested while the wait runs — the shape of a ramp, a warm-up, or a produce-only
        // phase ahead of the request-reply one.
        Thread.sleep(IdleBeforeFirstUse.toMillis)

        val ready   = new CountDownLatch(1)
        val failure = new AtomicReference[Throwable]()

        pool.acquireTracker(
          producerTopic = "startup-late-request",
          consumerTopic = replyTopic,
          messageMatcher = KafkaKeyMatcher,
          responseTransformer = None,
          timeout = 60.seconds,
        )(
          _ => ready.countDown(),
          error => { failure.set(error); ready.countDown() },
        )

        assert(
          ready.await(90, TimeUnit.SECONDS),
          "acquisition long after the pool was built never reported an outcome",
        )
        assertEquals(
          Option(failure.get()).map(_.getMessage),
          None,
          "the first reply topic requested long after startup must be served, not refused",
        )
      }
    }
  }
}
