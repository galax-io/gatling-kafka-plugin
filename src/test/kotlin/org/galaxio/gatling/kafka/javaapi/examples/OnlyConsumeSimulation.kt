package org.galaxio.gatling.kafka.javaapi.examples

import io.gatling.javaapi.core.CoreDsl.*
import io.gatling.javaapi.core.Simulation
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.javaapi.KafkaDsl
import org.galaxio.gatling.kafka.javaapi.KafkaDsl.byteArrayExp
import java.time.Duration

class OnlyConsumeSimulation : Simulation() {

    private val protocol = KafkaDsl.kafka().requestReply()
        .producerSettings(
            mapOf<String, Any>(
                ProducerConfig.ACKS_CONFIG to "1",
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092"
            )
        )
        .consumeSettings(mapOf("bootstrap.servers" to "localhost:9092"))
        .timeout(Duration.ofSeconds(5))

    private val scn = scenario("ConsumeOnly")
        .exec { session -> session.set("matchId", "corr-42".toByteArray()) }
        .exec(
            KafkaDsl.kafka("Consume only")
                .receiveFrom("events")
                .matchIdForTracking(byteArrayExp { session -> session.get("matchId") as ByteArray })
                .replyMatchBy { message -> message.value }
                .saveAs("replyValue") { message -> String(message.value) }
        )
        .exec(
            KafkaDsl.kafka("Consume any")
                .consumeAny("events")
                .saveAs("firstPayload") { message -> String(message.value) }
        )

    init {
        setUp(scn.injectOpen(atOnceUsers(1))).protocols(protocol)
    }
}
