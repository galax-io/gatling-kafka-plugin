package org.galaxio.gatling.kafka.examples

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.KeyValueMapper
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.protocol.KafkaProtocol
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

class BasicSimulation extends Simulation {

//  val kafkaConf: KafkaProtocol = kafka
//    .topic("test.topic")
//    .properties(Map(ProducerConfig.ACKS_CONFIG -> "1"))

  def getHeader(headerKey: String): KafkaProtocolMessage => Array[Byte] =
    _.headers
      .flatMap(hs => Option(hs.lastHeader(headerKey)).map(_.value()))
      .getOrElse(Array.emptyByteArray)

  def kafkaProtocolC: KafkaProtocol = kafka
    .producerSettings(
      ProducerConfig.ACKS_CONFIG              -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9093",
    )
    .consumeSettings(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9093",
    )
    .timeout(5.seconds)
//    .matchByMessage(getHeader("test-header"))

  val c                              = new AtomicInteger(1)
  val feeder: Feeder[java.util.UUID] = Iterator.continually(Map("kekey" -> java.util.UUID.randomUUID()))
  val hFeeder: Feeder[Array[Byte]]   = Iterator.continually(Map("headerId" -> java.util.UUID.randomUUID().toString.getBytes))

  val headers: Expression[Headers] =
    _("headerId").validate[Array[Byte]].map(bytes => new RecordHeaders().add("test-header", bytes))

  def scn(id: String): ScenarioBuilder = scenario(s"Basic$id")
    .feed(feeder)
    .feed(hFeeder)
    .exec(
      kafka("ReqRep").requestReply
        .requestTopic("test.t")
        .replyTopic("test.t")
        .send[String, String]("#{kekey}", """{ "m": "dkf" }""", headers)
        .check(jsonPath("$.m").is("dkf")),
    )
    .exec(
      kafka("ReqRep2").requestReply
        .requestTopic("myTopic2")
        .replyTopic("test.t1")
        .send[String, String]("#{kekey}", """{ "m": "dkf" }""", headers)
        .check(jsonPath("$.M").is("DKF")),
    )

  setUp(
    scn("A").inject(atOnceUsers(50)),
    scn("B").inject(atOnceUsers(1)),
    scn("C").inject(atOnceUsers(1)),
    scn("D").inject(atOnceUsers(1)),
    scn("E").inject(atOnceUsers(1)),
  ).protocols(kafkaProtocolC).maxDuration(120.seconds)

}
