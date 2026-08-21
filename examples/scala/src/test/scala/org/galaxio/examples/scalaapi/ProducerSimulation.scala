package org.galaxio.examples.scalaapi

import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol

class ProducerSimulation extends Simulation {

  val kafkaConsumerConf: KafkaProtocol =
    kafka
      .properties(
        Map(
          ProducerConfig.ACKS_CONFIG                   -> "1",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.DoubleSerializer",
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> "localhost:9093",
        ),
      )

  val scn: ScenarioBuilder = scenario("Basic")
    .exec(
      kafka("BasicRequest")
        .topic("ex.scala.producer.t")
        .send[Double](1.16423),
    )
    .exec(kafka("BasicRequestWithKey").topic("ex.scala.producer.t").send[String, Double]("true", 12.0))

  setUp(scn.inject(atOnceUsers(5)))
    .protocols(kafkaConsumerConf)
    .assertions(global.allRequests.count.is(10), global.successfulRequests.percent.is(100))

}
