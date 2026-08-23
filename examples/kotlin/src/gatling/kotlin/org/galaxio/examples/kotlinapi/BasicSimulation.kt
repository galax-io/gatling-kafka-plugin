package org.galaxio.examples.kotlinapi

import io.gatling.javaapi.core.CoreDsl.atOnceUsers
import io.gatling.javaapi.core.CoreDsl.global
import io.gatling.javaapi.core.CoreDsl.jsonPath
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.Simulation
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.galaxio.gatling.kafka.javaapi.KafkaDsl.kafka
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class BasicSimulation : Simulation() {

    private val kafkaProtocol = kafka()
        .producerSettings(
            mapOf<String, Any>(
                ProducerConfig.ACKS_CONFIG to "1",
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9093",
            )
        )
        .consumeSettings(mapOf<String, Any>("bootstrap.servers" to "localhost:9093"))
        .timeout(Duration.ofSeconds(10))

    private val c = AtomicInteger(0)

    private val feeder = generateSequence { mapOf<String, Any>("kekey" to c.incrementAndGet()) }.iterator()

    private val headers: Headers = RecordHeaders().add("test-header", "test_value".toByteArray())

    private val scn = scenario("Basic")
        .feed(feeder)
        .exec(
            kafka("ReqRep").requestReply()
                .requestTopic("ex.kotlin.basic.t")
                .replyTopic("ex.kotlin.basic.t")
                .send("#{kekey}", """{ "m": "dkf" }""", headers, String::class.java, String::class.java)
                .check(jsonPath("\$.m").`is`("dkf"))
        )

    init {
        setUp(scn.injectOpen(atOnceUsers(5)))
            .protocols(kafkaProtocol)
            .maxDuration(Duration.ofSeconds(120))
            .assertions(
                global().allRequests().count().`is`(5L),
                global().successfulRequests().percent().`is`(100.0),
            )
    }
}
