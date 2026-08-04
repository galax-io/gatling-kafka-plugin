package org.galaxio.gatling.kafka.examples

import com.sksamuel.avro4s._
import com.typesafe.scalalogging.StrictLogging
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.avro4s._
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, Deserializer, Serde, Serializer}
import org.galaxio.gatling.kafka.client.{DynamicKafkaConsumer, KafkaSender}
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import scala.concurrent.duration.DurationInt

class KafkaGatlingTest extends Simulation with StrictLogging {

  case class Ingredient(name: String, sugar: Double, fat: Double)

  private val bootstrap = "localhost:9093"

  /** Request topic → reply topic for the scenarios the responder serves.
    *
    * `myTopic4` is deliberately absent: `scnRRwo` publishes there and must never be answered, or the simulation loses its only
    * reply-timeout coverage (issue #196).
    */
  private val echoRoutes: Map[String, String] = Map(
    "myTopic1" -> "test.t1",
    "myTopic2" -> "test.t2",
  )

  /** Header carrying when the responder answered. Round-trip metadata has to ride here rather than in the key or the value:
    * `scnRR` matches by key and checks `jsonPath` on the value, `scnRR2` matches by value and checks `bodyBytes`. Any rewrite
    * of either breaks correlation, a check, or both.
    */
  private val RespondedAtHeader = "x-responded-at"

  private val probeMarker = "_probe"

  // Stands in for the service under test. Before this existed the request-reply scenarios were "answered"
  // by sibling fire-and-forget scenarios that happened to publish a matching key or value at a fixed
  // delay — so the simulation exercised the matching code without ever exercising a round trip, and was
  // green because of timing rather than because the plugin correlated anything (issue #196).
  private val responderSender = KafkaSender(
    Map(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ),
  )

  /** Request topics the responder has demonstrably answered on — a *successful* echo send, not merely a record received.
    *
    * Readiness used to be one latch counted down on the first record, before the probe short-circuit and without ever sending
    * anything. That proved the responder's consumer was alive and nothing else: a wrong route map, a dead producer or a
    * consumer that died right after would all have passed it, and the simulation would then have gone green anyway because a
    * sibling scenario answered instead.
    */
  private val echoedRoutes: java.util.Set[String] = ConcurrentHashMap.newKeySet[String]()

  private val responder = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
    Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> bootstrap,
      ConsumerConfig.GROUP_ID_CONFIG                 -> "kafka-gatling-test-responder",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "earliest",
    ),
    echoRoutes.keySet,
    record => {
      // Probes are echoed like anything else. Skipping them was what made readiness vacuous: the send
      // path — the half that actually matters — was never exercised before the simulation started.
      // Their key and value are the probe marker, which no scenario correlates on, so the echo lands on
      // the reply topic and is discarded.
      echoRoutes.get(record.topic()).foreach { replyTopic =>
        val headers = new RecordHeaders()
        headers.add(RespondedAtHeader, System.currentTimeMillis().toString.getBytes)
        responderSender.send(
          KafkaProtocolMessage(record.key(), record.value(), replyTopic, replyTopic, Some(headers)),
        )(
          _ => { echoedRoutes.add(record.topic()); () },
          // Never silent: a responder that stops echoing looks exactly like the plugin losing replies,
          // and the run would fail its assertion with nothing pointing at the real cause.
          error => logger.error(s"[gatling-test responder] failed to echo onto ", error),
        )
      }
    },
    error => logger.error("[gatling-test responder] consumer failed, replies stop here", error),
  )

  private val responderThread = new Thread(responder, "kafka-gatling-test-responder")

  /** Probes every route and waits until each has been echoed successfully.
    *
    * Every route, not just one: with a route map there is no reason to believe the second entry works because the first does,
    * and a typo in either would otherwise surface as a mysterious reply timeout mid-run.
    */
  private def awaitResponderReady(timeoutSeconds: Int = 30): Unit = {
    val deadline = System.currentTimeMillis() + timeoutSeconds * 1000L
    while (echoedRoutes.size < echoRoutes.size && System.currentTimeMillis() < deadline) {
      echoRoutes.keys.foreach { requestTopic =>
        val probe = KafkaProtocolMessage(probeMarker.getBytes, probeMarker.getBytes, requestTopic, requestTopic)
        val sent  = new CountDownLatch(1)
        responderSender.send(probe)(_ => sent.countDown(), _ => sent.countDown())
        sent.await(2, TimeUnit.SECONDS)
      }
      Thread.sleep(250)
    }
    require(
      echoedRoutes.size == echoRoutes.size,
      s"Responder did not echo on every route within ${timeoutSeconds}s: " +
        s"echoed ${echoedRoutes.toArray.mkString("[", ", ", "]")}, expected ${echoRoutes.keys.mkString("[", ", ", "]")}",
    )
  }

  before {
    responderThread.setDaemon(true)
    responderThread.start()
    // Gatling does not run `after` when `before` throws, and a responder left in the group blocks the
    // next run until the coordinator times its stale member out.
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

  val kafkaConf: KafkaProtocol = kafka
    .properties(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
      ),
    )

  val kafkaConfwoKey: KafkaProtocol = kafka
    .properties(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
      ),
    )

  val kafkaConfBytes: KafkaProtocol = kafka
    .properties(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.ByteArraySerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ),
    )

  val kafkaProtocolRRString: KafkaProtocol = kafka
    .producerSettings(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    )
    .consumeSettings(
      "bootstrap.servers" -> "localhost:9093",
    )
    .withDefaultTimeout

  val kafkaProtocolRRBytes: KafkaProtocol = kafka
    .producerSettings(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
    )
    .consumeSettings(
      "bootstrap.servers" -> "localhost:9093",
    )
    .timeout(5.seconds)
    .matchByValue

  // The reply timeout doubles as the acquisition timeout, so it has to outlast establishing the reply
  // channel. At the previous 1 second this scenario failed with "Timed out waiting for consumer
  // assignment to topic 'test.t2'" — a KO for the right count but the wrong reason, and a timing-dependent
  // one at that. It is supposed to prove that a request nobody answers times out waiting for its reply,
  // which is what it now does: myTopic4 has no responder, so nothing ever publishes tstBytesWO.
  val kafkaProtocolRRBytes2: KafkaProtocol = kafka
    .producerSettings(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
    )
    .consumeSettings(
      "bootstrap.servers" -> "localhost:9093",
    )
    .timeout(5.seconds)
    .matchByValue

  val kafkaAvro4sConf: KafkaProtocol = kafka
    .properties(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
        "value.subject.name.strategy"                -> "io.confluent.kafka.serializers.subject.RecordNameStrategy",
        "schema.registry.url"                        -> "http://localhost:9094",
      ),
    )

  def matchByOwnVal(message: KafkaProtocolMessage): Array[Byte] = {
    message.key
  }

  val kafkaProtocolRRAvro: KafkaProtocol = kafka
    .producerSettings(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
      "value.subject.name.strategy"                -> "io.confluent.kafka.serializers.subject.RecordNameStrategy",
      "schema.registry.url"                        -> "http://localhost:9094",
    )
    .consumeSettings(
      "bootstrap.servers" -> "localhost:9093",
    )
    .timeout(7.seconds)
    .matchByMessage(matchByOwnVal)

  val scnRR: ScenarioBuilder = scenario("RequestReply String")
    .exec(
      kafka("Request Reply String").requestReply
        .requestTopic("myTopic1")
        .replyTopic("test.t1")
        .send[String, String]("testCheckJson", """{ "m": "dkf" }""")
        .check(jsonPath("$.m").is("dkf")),
    )

  val scnwokey: ScenarioBuilder = scenario("Request String without key")
    .exec(
      kafka("Request String")
        .topic("myTopic3")
        .send[String]("foo"),
    )
    .exec(
      kafka("Request String With null key")
        .topic("myTopic3")
        .send[Int, String](null, "nullkey"),
    )

  // These publish to myTopic3, not to the reply topics, and that is the point of #196.
  //
  // `scn` used to publish key `testCheckJson` and body `{ "m": "dkf" }` to test.t1 — which is scnRR's
  // reply topic, matched by key, checked with that exact jsonPath. `scn2` used to publish value
  // `tstBytes` to test.t2, which is scnRR2's reply topic, matched by value. They *were* the answers.
  // Adding a responder made it answer first, but left both able to answer, so the simulation still went
  // green with the responder completely dead — the coincidence the issue exists to remove.
  //
  // myTopic3 is a plain produce target nothing correlates on and the responder does not consume.
  val scn: ScenarioBuilder = scenario("Request String")
    .exec(kafka("Request String 2").topic("myTopic3").send[String, String]("testCheckJson", """{ "m": "dkf" }"""))

  val scn2: ScenarioBuilder = scenario("Request Byte")
    .exec(
      kafka("Request Byte")
        .topic("myTopic3")
        .send[Array[Byte], Array[Byte]]("key".getBytes(), "tstBytes".getBytes()),
    )

  val scnRR2: ScenarioBuilder = scenario("RequestReply Bytes")
    .exec(
      kafka("Request Reply Bytes").requestReply
        .requestTopic("myTopic2")
        .replyTopic("test.t2")
        .send[Array[Byte], Array[Byte]]("test".getBytes(), "tstBytes".getBytes())
        .check(bodyBytes.is("tstBytes".getBytes()).saveAs("bodyInfo")),
    )

  val scnAvro4s: ScenarioBuilder = scenario("Request Avro4s")
    .exec(
      kafka("Request Simple Avro4s")
        .topic("test.t3")
        .send[Ingredient](Ingredient("Cheese", 1d, 50d)),
    )
    .exec(
      kafka("Request Avro4s")
        .topic("test.t3")
        .send[String, Ingredient]("key4s", Ingredient("Cheese", 0d, 70d)),
    )

  // myTopic4 rather than myTopic2: the responder serves myTopic2, so sharing it would answer this
  // request too and destroy the simulation's only reply-timeout coverage (issue #196). The reply topic
  // stays test.t2, which the responder does publish to — so this scenario also exercises a held reply
  // channel receiving traffic that matches none of its pending requests, and discarding it silently.
  val scnRRwo: ScenarioBuilder = scenario("RequestReply w/o answer")
    .exec(
      kafka("Request Reply Bytes wo").requestReply
        .requestTopic("myTopic4")
        .replyTopic("test.t2")
        .send[Array[Byte], Array[Byte]]("testWO".getBytes(), "tstBytesWO".getBytes()),
    )

  setUp(
    scnRR.inject(atOnceUsers(1)).protocols(kafkaProtocolRRString),
    scn.inject(nothingFor(1), atOnceUsers(1)).protocols(kafkaConf),
    scnRR2.inject(atOnceUsers(1)).protocols(kafkaProtocolRRBytes),
    scn2.inject(nothingFor(2), atOnceUsers(1)).protocols(kafkaConfBytes),
    scnAvro4s.inject(atOnceUsers(1)).protocols(kafkaAvro4sConf),
    scnRRwo.inject(atOnceUsers(1)).protocols(kafkaProtocolRRBytes2),
    scnwokey.inject(nothingFor(1), atOnceUsers(1)).protocols(kafkaConfwoKey),
  ).assertions(
    // Exactly one failure is expected, and it is by design: scnRRwo ("RequestReply w/o answer") sends to
    // myTopic4, which the echo responder does not consume, so it always KOs on its reply timeout.
    //
    // `is(1)` rather than `lte(1)`: the previous bound passed when the by-design timeout silently stopped
    // failing, so a broken reply-timeout would have dropped the count to 0 and still gone green. Pinning
    // the count and naming the request that must fail closes the gate in both directions — a second,
    // real failure fails the run, and so does the expected one starting to pass.
    global.failedRequests.count.is(1),
    details("Request Reply Bytes wo").failedRequests.count.is(1),
  ).maxDuration(120.seconds)

}
