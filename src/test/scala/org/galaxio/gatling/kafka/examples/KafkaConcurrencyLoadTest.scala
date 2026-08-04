package org.galaxio.gatling.kafka.examples

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.client.{DynamicKafkaConsumer, KafkaSender}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt

/** Ad hoc load check for #163: sustained, meaningfully concurrent request-reply traffic through the real Gatling DSL/action
  * pipeline (not a hand-rolled harness like TrackerAcquisitionIsolationSpec). Every request is echoed back by a dedicated
  * responder, so — unlike KafkaGatlingTest's `scnRRwo` — no scenario here is designed to fail; the residual failures the
  * assertion still tolerates are tracked defects, see `KnownReplyLossBudgetPercent`.
  *
  * Not wired into CI; run manually against the docker-compose.kafka.yml stack: sbt "Gatling / testOnly
  * org.galaxio.gatling.kafka.examples.KafkaConcurrencyLoadTest"
  */
class KafkaConcurrencyLoadTest extends Simulation with StrictLogging {

  private val bootstrap     = "localhost:9093"
  private val requestTopic  = "load.request"
  private val replyTopic    = "load.reply"
  private val concurrency   = 30
  private val rampDuration  = 10.seconds
  private val sustainFor    = 100.seconds
  private val scenarioLoops = rampDuration + sustainFor + 15.seconds // outlasts the injection window
  private val probeMarker   = "_probe"

  /** Reply-loss budget: **zero**, now that every known source of loss is closed.
    *
    * The last one was [[https://github.com/galax-io/gatling-kafka-plugin/issues/191 #191]]: the pending record was created from
    * the producer's acknowledgement callback, i.e. after the request was already answerable, so a fast responder's reply could
    * be delivered before the request it answers had registered and be discarded silently. Registering before the send removed
    * it structurally — a reply cannot exist before the send, and the mailbox preserves enqueue order across threads.
    *
    * The history this budget tracked, for context. Before [[https://github.com/galax-io/gatling-kafka-plugin/issues/163 #163]]:
    * ~1.4% (38 KO of 2,724, of which 7 were assignment timeouts #163 removed outright). Before
    * [[https://github.com/galax-io/gatling-kafka-plugin/issues/165 #165]]: 14–16 KO of ~6,500, because the tracker entry was
    * dropped at refcount zero after every reply and each re-registration re-armed the next loss. After #165 and before #191:
    * 0–2 KO of ~6,760 across five runs. After #191: zero, and anything above it is a regression rather than a known defect
    * being absorbed.
    */
  private val KnownReplyLossBudget: Long = 0

  // Stand-in for the external service under test: echoes every request straight back on the reply
  // topic with the same key and value, so the default key-based matcher (KafkaKeyMatcher) pairs each
  // reply with its own request regardless of how many are in flight concurrently.
  private val responderSender = KafkaSender(
    Map(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ),
  )

  private val responderReady: CountDownLatch = new CountDownLatch(1)

  private val responder       = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
    Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> bootstrap,
      ConsumerConfig.GROUP_ID_CONFIG                 -> "load-test-responder",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "earliest",
    ),
    Set(requestTopic),
    record => {
      responderReady.countDown()
      if (new String(record.value()) != probeMarker) {
        responderSender.send(
          KafkaProtocolMessage(record.key(), record.value(), replyTopic, replyTopic),
        )(
          _ => (),
          // Never silent: a responder that stops echoing looks exactly like the plugin losing
          // replies, and the run would fail its budget with nothing pointing at the real cause.
          error => logger.error(s"[load-test responder] failed to echo ${new String(record.key())}", error),
        )
      }
    },
    error => logger.error("[load-test responder] consumer failed, replies stop here", error),
  )
  private val responderThread = new Thread(responder, "load-test-responder")

  private def awaitResponderReady(timeoutSeconds: Int = 30): Unit = {
    val probe    = KafkaProtocolMessage(probeMarker.getBytes, probeMarker.getBytes, requestTopic, requestTopic)
    val deadline = System.currentTimeMillis() + timeoutSeconds * 1000L
    while (responderReady.getCount > 0 && System.currentTimeMillis() < deadline) {
      val sent = new CountDownLatch(1)
      responderSender.send(probe)(_ => sent.countDown(), _ => sent.countDown())
      sent.await(2, java.util.concurrent.TimeUnit.SECONDS)
      responderReady.await(500, java.util.concurrent.TimeUnit.MILLISECONDS)
    }
    require(responderReady.getCount == 0, s"Responder not ready within ${timeoutSeconds}s")
  }

  before {
    responderThread.setDaemon(true)
    responderThread.start()
    // Gatling does not run `after` when `before` throws, and a responder left in the group blocks
    // the next run until the coordinator times its stale member out.
    try awaitResponderReady()
    catch {
      case error: Throwable =>
        responder.close()
        responderSender.close()
        throw error
    }
  }

  after {
    responder.close()
    responderSender.close()
  }

  private val protocol: KafkaProtocol = kafka
    .producerSettings(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    )
    .consumeSettings(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
    )
    .timeout(10.seconds)

  private val keySeq: AtomicReference[Long] = new AtomicReference(0L)
  private val feeder: Feeder[String]        = Iterator.continually {
    val n = keySeq.updateAndGet(_ + 1)
    Map("k" -> s"load-$n-${java.util.UUID.randomUUID()}")
  }

  private val scn: ScenarioBuilder = scenario("Concurrent Request-Reply Load")
    .during(scenarioLoops) {
      feed(feeder)
        .exec(
          kafka("Load RR").requestReply
            .requestTopic(requestTopic)
            .replyTopic(replyTopic)
            .send[String, String]("#{k}", "#{k}")
            .check(bodyString.is("#{k}")),
        )
        .pause(300.millis, 800.millis)
    }

  setUp(
    scn.inject(
      rampConcurrentUsers(1).to(concurrency).during(rampDuration),
      constantConcurrentUsers(concurrency).during(sustainFor),
    ),
  ).protocols(protocol)
    .assertions(
      global.failedRequests.count.lte(KnownReplyLossBudget),
    )
    .maxDuration(3.minutes)
}
