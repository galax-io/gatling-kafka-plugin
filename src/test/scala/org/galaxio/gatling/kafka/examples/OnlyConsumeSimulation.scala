package org.galaxio.gatling.kafka.examples

import io.gatling.core.Predef._
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.Predef._

import scala.concurrent.duration.DurationInt

class OnlyConsumeSimulation extends Simulation {

  private val protocol = kafka.requestReply
    .producerSettings(
      Map(
        ProducerConfig.ACKS_CONFIG              -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ),
    )
    .consumeSettings(
      Map(
        "bootstrap.servers" -> "localhost:9092",
      ),
    )
    .timeout(5.seconds)

  private val scn = scenario("ConsumeOnly")
    .exec(session => session.set("correlationId", "corr-42"))
    .exec(
      kafka("Consume only")
        .receiveFrom("events")
        .headerForTracking("correlation-id", "#{correlationId}")
        .saveAs("replyValue")(message => new String(message.value)),
    )

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol)
}
