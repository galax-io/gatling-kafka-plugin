package org.galaxio.gatling.kafka.examples

import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.Predef._

import org.galaxio.gatling.kafka.avro4s._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol

class Avro4sSimulation extends Simulation {

  val kafkaAclConf: KafkaProtocol = kafka
    .properties(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
      "value.subject.name.strategy"                -> "io.confluent.kafka.serializers.subject.RecordNameStrategy",
      "schema.registry.url"                        -> "http://localhost:9094",
    )

  case class Ingredient(name: String, sugar: Double, fat: Double)

  val scn: ScenarioBuilder = scenario("Kafka Test")
    .exec(
      kafka("Simple Avro4s Request")
        // message to send
        .topic("my.acl.topic")
        .send[Ingredient](Ingredient("Cheese", 0d, 70d)),
    )
    .exec(
      kafka("Simple Avro4s Request with Key")
        // message to send
        .topic("my.acl.topic")
        .send[String, Ingredient]("Key", Ingredient("Cheese", 0d, 70d)),
    )

  setUp(scn.inject(atOnceUsers(1))).protocols(kafkaAclConf)
}
