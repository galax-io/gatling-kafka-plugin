package org.galaxio.gatling.kafka.examples

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.avro4s._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import scala.concurrent.duration.DurationInt

/** The round trips that must work: produce-only, request-reply across three serializations, and keyless correlation by value.
  *
  * Every request here is expected to succeed, and the run asserts exactly that — so a green run prints no error block at all.
  * The failure modes the plugin also has to get right (no correlation id, tombstone reply, reply timeout) live in
  * [[KafkaFailureModesGatlingTest]]. They used to be scenarios in this file, which meant a completely healthy run ended with
  * six errors on the console and no way to tell them apart from a regression at a glance.
  */
class KafkaGatlingTest extends Simulation {

  case class Ingredient(name: String, sugar: Double, fat: Double)

  private val bootstrap = "localhost:9093"

  /** Request topic → reply topic for the scenarios the responder serves.
    *
    * `myTopic4` and `myTopic6` are deliberately absent: they belong to the reply-timeout and tombstone scenarios, which are not
    * in this simulation and must not be answered by it.
    */
  private val echoRoutes: Map[String, String] = Map(
    "myTopic1" -> "test.t1",
    "myTopic2" -> "test.t2",
    // Serves the keyless request-reply scenario (issue #167). Its own route rather than a shared one:
    // it runs several virtual users at once, and sharing a reply topic with a single-user scenario would
    // make a cross-attribution failure look like that scenario's problem instead.
    "myTopic5" -> "test.t5",
  )

  /** Concurrent users for the keyless-by-value scenario (issue #167). More than one is the whole point — a reply reaching the
    * wrong virtual user is unobservable with a single user in flight.
    */
  private val KeylessValueUsers = 5

  /** Every request this simulation issues: one each from `scnRR`, `scn`, `scnRR2` and `scn2`, two each from `scnAvro4s` and
    * `scnwokey`, and one per keyless-by-value user.
    *
    * Pinned because `failedRequests.count.is(0)` is satisfied by a scenario that never ran. Asserting zero failures alone would
    * turn a whole scenario silently disappearing — a build error in a feeder, a protocol that fails to start — into a green
    * run.
    */
  private val ExpectedRequests = 1 + 1 + 1 + 1 + 2 + 2 + KeylessValueUsers

  private val responder = new EchoResponder(bootstrap, "kafka-gatling-test-responder", echoRoutes)

  before(responder.start())
  after(responder.close())

  val kafkaConf: KafkaProtocol = kafka
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
    .timeout(15.seconds)
    .matchByValue

  /** String serializers plus value matching, for the keyless scenario (issue #167). String rather than bytes so the payload can
    * come from a feeder through EL, which `KafkaAction.serializeValue` only applies to `String` values.
    */
  val kafkaProtocolRRKeylessValue: KafkaProtocol = kafka
    .producerSettings(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    )
    .consumeSettings(
      "bootstrap.servers" -> "localhost:9093",
    )
    .timeout(10.seconds)
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

  /** Distinct payload per virtual user. A feeder rather than an EL expression on the value: EL is only applied when the value
    * type is `String` (see `KafkaAction.serializeValue`), so a templated `Array[Byte]` would be sent with its placeholder
    * unresolved — identical for every user, which would make value correlation collide for a reason that has nothing to do with
    * what is under test.
    */
  private val keylessPayloads: Feeder[String] =
    Iterator.from(1).map(i => Map("payload" -> s"keyless-$i"))

  /** Keyless, correlating on the value each request carries (issue #167). Several users at once, each with a distinct payload:
    * correlation has something real to work with, so every one must be answered.
    *
    * `Option(null)` is `None`, so a null key expression is how the DSL expresses "this request carries no key" — the same idiom
    * `scnwokey` already uses for produce-only. Together with `KafkaFailureModesGatlingTest`'s keyless-by-key scenario, these
    * are the only places that exercise `KafkaAction`'s key handling itself, which is where the empty-array substitution lived.
    * `KeylessCorrelationSpec` builds its `KafkaProtocolMessage` directly and so bypasses exactly that code.
    */
  val scnRRKeylessValue: ScenarioBuilder = scenario("RequestReply keyless by value")
    .feed(keylessPayloads)
    .exec(
      kafka("Request Reply Keyless Value").requestReply
        .requestTopic("myTopic5")
        .replyTopic("test.t5")
        .send[String, String](null, "#{payload}"),
    )

  setUp(
    scnRR.inject(atOnceUsers(1)).protocols(kafkaProtocolRRString),
    scn.inject(nothingFor(1), atOnceUsers(1)).protocols(kafkaConf),
    scnRR2.inject(atOnceUsers(1)).protocols(kafkaProtocolRRBytes),
    scn2.inject(nothingFor(2), atOnceUsers(1)).protocols(kafkaConfBytes),
    scnAvro4s.inject(atOnceUsers(1)).protocols(kafkaAvro4sConf),
    scnwokey.inject(nothingFor(1), atOnceUsers(1)).protocols(kafkaConf),
    scnRRKeylessValue.inject(atOnceUsers(KeylessValueUsers)).protocols(kafkaProtocolRRKeylessValue),
  ).assertions(
    // Nothing here is allowed to fail, and the count is pinned alongside it so that "nothing failed"
    // cannot be achieved by nothing running. Both halves are needed: the failure count catches a
    // regression in a request, the request count catches a scenario that disappeared.
    global.failedRequests.count.is(0),
    global.allRequests.count.is(ExpectedRequests),
    // The other half of #167: keyless requests that *can* be correlated must all succeed, concurrently.
    // Named explicitly rather than left to the global count, so a cross-attribution failure names the
    // scenario it belongs to.
    details("Request Reply Keyless Value").successfulRequests.count.is(KeylessValueUsers),
  ).maxDuration(120.seconds)

}
