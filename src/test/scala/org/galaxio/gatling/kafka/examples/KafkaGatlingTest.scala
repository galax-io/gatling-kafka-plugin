package org.galaxio.gatling.kafka.examples

import com.sksamuel.avro4s._
import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import scala.concurrent.duration.DurationInt

class KafkaGatlingTest extends Simulation {

  case class Ingredient(name: String, sugar: Double, fat: Double)

  implicit val ingridientToRecord: ToRecord[Ingredient]     = ToRecord.apply
  implicit val ingridientFromRecord: FromRecord[Ingredient] = FromRecord.apply
  implicit val ingridientSchemaFor: SchemaFor[Ingredient]   = SchemaFor.apply
  implicit val ingridientFormat: RecordFormat[Ingredient]   = RecordFormat.apply
  implicit val ingredientHeaders: Headers                   = new RecordHeaders()

  val kafkaConf: KafkaProtocol = kafka
    .topic("test.t1")
    .properties(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
      ),
    )

  val kafkaConfwoKey: KafkaProtocol = kafka
    .topic("myTopic3")
    .properties(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
      ),
    )

  val kafkaConfBytes: KafkaProtocol = kafka
    .topic("test.t2")
    .properties(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.ByteArraySerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ),
    )

  val kafkaProtocolRRString: KafkaProtocol = kafka.requestReply
    .producerSettings(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
      ),
    )
    .consumeSettings(
      Map(
        "bootstrap.servers" -> "localhost:9093",
      ),
    )
    .withDefaultTimeout

  val kafkaProtocolRRBytes: KafkaProtocol = kafka.requestReply
    .producerSettings(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.ByteArraySerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ),
    )
    .consumeSettings(
      Map(
        "bootstrap.servers" -> "localhost:9093",
      ),
    )
    .timeout(5.seconds)
    .matchByValue

  val kafkaProtocolRRBytes2: KafkaProtocol = kafka.requestReply
    .producerSettings(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.ByteArraySerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ),
    )
    .consumeSettings(
      Map(
        "bootstrap.servers" -> "localhost:9093",
      ),
    )
    .timeout(1.seconds)
    .matchByValue

  val kafkaAvro4sConf: KafkaProtocol = kafka
    .topic("test.t3")
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

  val kafkaProtocolRRAvro: KafkaProtocol = kafka.requestReply
    .producerSettings(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
        "value.subject.name.strategy"                -> "io.confluent.kafka.serializers.subject.RecordNameStrategy",
        "schema.registry.url"                        -> "http://localhost:9094",
      ),
    )
    .consumeSettings(
      Map(
        "bootstrap.servers" -> "localhost:9093",
      ),
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
        .send[String]("foo"),
    )
    .exec(
      kafka("Request Stirng With null key")
        .send[Any, String](null, "nullkey"),
    )
    .exec(
      kafka("Request String SILENT")
        .send[String]("foo")
        .silent,
    )

  val scn: ScenarioBuilder = scenario("Request String")
    .exec(kafka("Request String 2").send[String, String]("testCheckJson", """{ "m": "dkf" }"""))
    .exec(kafka("Request String 2 SILENT").send[String, String]("testCheckJson", """{ "m": "dkf" }""").silent)

  val scn2: ScenarioBuilder = scenario("Request Byte")
    .exec(
      kafka("Request Byte")
        .send[Array[Byte], Array[Byte]]("key".getBytes(), "tstBytes".getBytes()),
    )
    .exec(
      kafka("Request Byte SILENT")
        .send[Array[Byte], Array[Byte]]("key".getBytes(), "tstBytes".getBytes())
        .silent,
    )

  val scnRR2: ScenarioBuilder = scenario("RequestReply Bytes")
    .exec(
      kafka("Request Reply Bytes").requestReply
        .requestTopic("myTopic2")
        .replyTopic("test.t2")
        .send[Array[Byte], Array[Byte]]("test".getBytes(), "tstBytes".getBytes())
        .check(bodyBytes.is("tstBytes".getBytes()).saveAs("bodyInfo")),
    )
    .exec(
      kafka("Request Reply Bytes SILENT").requestReply
        .requestTopic("myTopic2")
        .replyTopic("test.t2")
        .send[Array[Byte], Array[Byte]]("test".getBytes(), "tstBytes".getBytes())
        .silent
        .check(bodyBytes.is("tstBytes".getBytes()).saveAs("bodyInfo")),
    )

  val scnAvro4s: ScenarioBuilder = scenario("Request Avro4s")
    .exec(
      kafka("Request Simple Avro4s")
        .send[Ingredient](Ingredient("Cheese", 1d, 50d)),
    )
    .exec(
      kafka("Request Avro4s")
        .send[String, Ingredient]("key4s", Ingredient("Cheese", 0d, 70d)),
    )
    .exec(
      kafka("Request Simple Avro4s SILENT")
        .send[Ingredient](Ingredient("Cheese", 1d, 50d))
        .silent,
    )

  val scnRRwo: ScenarioBuilder = scenario("RequestReply w/o answer")
    .exec(
      kafka("Request Reply Bytes wo").requestReply
        .requestTopic("myTopic2")
        .replyTopic("test.t2")
        .send[Array[Byte], Array[Byte]]("testWO".getBytes(), "tstBytesWO".getBytes()),
    )
    .exec(
      kafka("Request Reply Bytes wo SILENT").requestReply
        .requestTopic("myTopic2")
        .replyTopic("test.t2")
        .send[Array[Byte], Array[Byte]]("testWO".getBytes(), "tstBytesWO".getBytes())
        .silent,
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
    global.failedRequests.percent.lt(17.0),
  )

}
