package org.galaxio.examples.scalaapi

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

class MatchSimulation extends Simulation {

  val kafkaProtocolMatchByValue: KafkaProtocol = kafka
    .producerSettings(
      Map(
        ProducerConfig.ACKS_CONFIG              -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9093",
      ),
    )
    .consumeSettings(
      Map(
        "bootstrap.servers" -> "localhost:9093",
      ),
    )
    .timeout(10.seconds)
    // for match by message value
    .matchByValue

  def matchByOwnVal(message: KafkaProtocolMessage): Array[Byte] = {
    // do something with the message and extract the values your are interested in
    // method is called:
    // - for each message which will be sent out
    // - for each message which has been received
    "Custom Message".getBytes // just returning something
  }

  val kafkaProtocolMatchByMessage: KafkaProtocol = kafka
    .producerSettings(
      Map(
        ProducerConfig.ACKS_CONFIG              -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9093",
      ),
    )
    .consumeSettings(
      Map(
        "bootstrap.servers" -> "localhost:9093",
      ),
    )
    .timeout(10.seconds)
    .matchByMessage(matchByOwnVal)

  val c                   = new AtomicInteger(0)
  val feeder: Feeder[Int] = Iterator.continually(Map("kekey" -> c.incrementAndGet()))

  val scn: ScenarioBuilder = scenario("Basic")
    .feed(feeder)
    .exec(
      kafka("ReqRep").requestReply
        .requestTopic("ex.scala.match.t")
        .replyTopic("ex.scala.match.t")
        .send[String, String]("#{kekey}", """{ "m": "dkf" }"""),
    )

  // Deliberately one user in flight: matchByOwnVal returns the same bytes for every message, so any
  // reply matches any request. The assertion is written to that bound, not above it.
  setUp(scn.inject(atOnceUsers(1)))
    .protocols(kafkaProtocolMatchByMessage)
    .maxDuration(120.seconds)
    .assertions(global.allRequests.count.is(1), global.successfulRequests.percent.is(100))
}
