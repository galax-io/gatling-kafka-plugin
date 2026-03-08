package org.galaxio.gatling.kafka.examples

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.DurationInt

class KafkaThreadLoadSimulation extends Simulation {

  private val bootstrapServers = sys.props.getOrElse("gatling.kafka.bootstrapServers", "localhost:9093")
  private val topic            = sys.props.getOrElse("gatling.kafka.topic", "test.t2")
  private val concurrentUsers  = sys.props.get("gatling.kafka.threadLoad.concurrentUsers").flatMap(_.toIntOption).getOrElse(200)
  private val durationSeconds  = sys.props.get("gatling.kafka.threadLoad.durationSeconds").flatMap(_.toIntOption).getOrElse(300)
  private val pauseMillis      = sys.props.get("gatling.kafka.threadLoad.pauseMillis").flatMap(_.toIntOption).getOrElse(25)

  private val keyCounter                = new AtomicLong(0L)
  private val keyFeeder: Feeder[String] =
    Iterator.continually(Map("key" -> s"thread-check-${keyCounter.incrementAndGet()}"))

  private val kafkaProtocolLoad: KafkaProtocol = kafka
    .topic(topic)
    .properties(
      Map(
        ProducerConfig.ACKS_CONFIG                   -> "1",
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
      ),
    )

  private val scn: ScenarioBuilder = scenario("Kafka Thread Load")
    .feed(keyFeeder)
    .forever(
      pace(pauseMillis.millis)
        .exec(
          kafka("Thread Load Send")
            .send[String, String]("#{key}", """{ \"type\": \"thread-load-check\" }"""),
        ),
    )

  setUp(
    scn.inject(constantConcurrentUsers(concurrentUsers) during durationSeconds.seconds),
  ).protocols(kafkaProtocolLoad)
}
