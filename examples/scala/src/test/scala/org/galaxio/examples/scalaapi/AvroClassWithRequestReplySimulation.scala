package org.galaxio.examples.scalaapi

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import io.gatling.core.Predef._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.actions.KafkaRequestReplyActionBuilder
import org.galaxio.gatling.kafka.protocol.KafkaProtocol

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class AvroClassWithRequestReplySimulation extends Simulation {

  // default serde for unknown classes is avro serde
  // schemaRegUrl must be specified if custom avro scheme is used, when the send method requires implicit
  implicit val schemaRegUrl: String = "http://localhost:9094"

  // Your own domain type, with your own serde for it. The plugin already provides an implicit
  // Serde[GenericRecord], so a custom serde is only worth writing — and only unambiguous — for a type
  // of your own.
  case class MyAvroClass(name: String, sugar: Double, fat: Double)

  val schema: Schema = new Schema.Parser().parse(
    """|{"type":"record","name":"MyAvroClass","namespace":"org.galaxio.gatling.kafka.examples",
       | "fields":[{"name":"name","type":"string"},
       |           {"name":"sugar","type":"double"},
       |           {"name":"fat","type":"double"}]}""".stripMargin,
  )

  // example if you want to use your own or custom serde.
  // Pass the config as well as the client: constructed with a client alone, KafkaAvroSerializer never
  // registers a schema, it only looks one up — and fails with "Error retrieving Avro schema" the first
  // time it meets a type the registry has not seen.
  val serdeConfig: java.util.Map[String, Any] =
    Map[String, Any]("schema.registry.url" -> schemaRegUrl, "auto.register.schemas" -> true).asJava

  val ser = new KafkaAvroSerializer(new CachedSchemaRegistryClient(schemaRegUrl.split(',').toList.asJava, 16), serdeConfig)
  val de  = new KafkaAvroDeserializer(new CachedSchemaRegistryClient(schemaRegUrl.split(',').toList.asJava, 16), serdeConfig)

  implicit val serdeClass: Serde[MyAvroClass] = new Serde[MyAvroClass] {
    override def serializer(): Serializer[MyAvroClass] = (topic: String, data: MyAvroClass) => {
      val record = new GenericData.Record(schema)
      record.put("name", data.name)
      record.put("sugar", data.sugar)
      record.put("fat", data.fat)
      ser.serialize(topic, record)
    }

    override def deserializer(): Deserializer[MyAvroClass] = (topic: String, bytes: Array[Byte]) =>
      de.deserialize(topic, bytes) match {
        case null             => null
        case r: GenericRecord =>
          MyAvroClass(
            r.get("name").toString,
            r.get("sugar").asInstanceOf[Double],
            r.get("fat").asInstanceOf[Double],
          )
        case other            => sys.error(s"unexpected payload type ${other.getClass.getName}")
      }
  }

  // protocol
  val kafkaProtocolRRAvro: KafkaProtocol = kafka
    .producerSettings(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
        // schema registry url is required for KafkaAvroSerializer and KafkaAvroDeserializer
        "schema.registry.url"                        -> schemaRegUrl,
      ),
    )
    .consumeSettings(
      Map(
        "bootstrap.servers" -> "localhost:9093",
      ),
    )
    .timeout(10.seconds)

  // message
  val kafkaMessage: KafkaRequestReplyActionBuilder[String, MyAvroClass] = kafka("RequestReply").requestReply
    .requestTopic("ex.scala.avrorr.t")
    .replyTopic("ex.scala.avrorr.t")
    .send[String, MyAvroClass]("key", MyAvroClass("Cheese", 0d, 70d))

  // simulation
  setUp(scenario("Kafka RequestReply Avro").exec(kafkaMessage).inject(atOnceUsers(1)))
    .protocols(kafkaProtocolRRAvro)
    .assertions(global.allRequests.count.is(1), global.successfulRequests.percent.is(100))
}
