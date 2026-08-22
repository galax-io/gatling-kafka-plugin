package org.galaxio.examples.kotlinapi

import io.gatling.javaapi.core.CoreDsl.atOnceUsers
import io.gatling.javaapi.core.CoreDsl.global
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.Simulation
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.javaapi.KafkaDsl.kafka
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class MatchSimulation : Simulation() {

    @Suppress("unused")
    private val kafkaProtocolMatchByValue = kafka()
        .producerSettings(
            mapOf<String, Any>(
                ProducerConfig.ACKS_CONFIG to "1",
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9093",
            )
        )
        .consumeSettings(mapOf<String, Any>("bootstrap.servers" to "localhost:9093"))
        .timeout(Duration.ofSeconds(10))
        .matchByValue()

    private fun matchByOwnVal(message: KafkaProtocolMessage): ByteArray = "Custom Message".toByteArray()

    private val kafkaProtocolMatchByMessage = kafka()
        .producerSettings(
            mapOf<String, Any>(
                ProducerConfig.ACKS_CONFIG to "1",
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9093",
            )
        )
        .consumeSettings(mapOf<String, Any>("bootstrap.servers" to "localhost:9093"))
        .timeout(Duration.ofSeconds(10))
        .matchByMessage { message: KafkaProtocolMessage -> matchByOwnVal(message) }

    private val c = AtomicInteger(0)

    private val feeder = generateSequence { mapOf<String, Any>("kekey" to c.incrementAndGet()) }.iterator()

    private val scn = scenario("Basic")
        .feed(feeder)
        .exec(
            kafka("ReqRep").requestReply()
                .requestTopic("ex.kotlin.match.t")
                .replyTopic("ex.kotlin.match.t")
                .send("#{kekey}", """{ "m": "dkf" }""")
        )

    init {
        setUp(scn.injectOpen(atOnceUsers(1)))
            .protocols(kafkaProtocolMatchByMessage)
            .maxDuration(Duration.ofSeconds(120))
            .assertions(
                global().allRequests().count().`is`(1L),
                global().successfulRequests().percent().`is`(100.0),
            )
    }
}
