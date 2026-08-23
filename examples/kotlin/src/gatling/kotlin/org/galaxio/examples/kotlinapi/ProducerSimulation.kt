package org.galaxio.examples.kotlinapi

import io.gatling.javaapi.core.CoreDsl.atOnceUsers
import io.gatling.javaapi.core.CoreDsl.feed
import io.gatling.javaapi.core.CoreDsl.global
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.Session
import io.gatling.javaapi.core.Simulation
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.galaxio.gatling.kafka.javaapi.KafkaDsl.kafka
import org.galaxio.gatling.kafka.javaapi.request.expressions.JExpression
import java.nio.charset.StandardCharsets
import java.util.UUID

class ProducerSimulation : Simulation() {

    private val kafkaProducerConf = kafka()
        .properties(
            mapOf<String, Any>(
                ProducerConfig.ACKS_CONFIG to "1",
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9093",
            )
        )

    // Fed, not defaulted: without a feeder `getString("UUID")` is always null and the header this
    // example exists to demonstrate would ship empty on every record — passing its assertions while
    // proving nothing about the mechanism.
    private val uuidFeeder = generateSequence { mapOf<String, Any>("UUID" to UUID.randomUUID().toString()) }.iterator()

    private val header = JExpression<Headers> { session: Session ->
        val uuid = session.getString("UUID") ?: ""
        RecordHeaders().add("uuid-header", uuid.toByteArray(StandardCharsets.UTF_8))
    }

    private val scn = scenario("Basic")
        .feed(uuidFeeder)
        .exec(kafka("BasicRequest").topic("ex.kotlin.producer.t").send("foo"))
        .exec(kafka("dld").topic("ex.kotlin.producer.t").send("true", 12.0))
        .exec(kafka("Msg1").topic("ex.kotlin.producer.t").send("key", "val", header))

    init {
        setUp(scn.injectOpen(atOnceUsers(1)))
            .protocols(kafkaProducerConf)
            .assertions(
                global().allRequests().count().`is`(3L),
                global().successfulRequests().percent().`is`(100.0),
            )
    }
}
