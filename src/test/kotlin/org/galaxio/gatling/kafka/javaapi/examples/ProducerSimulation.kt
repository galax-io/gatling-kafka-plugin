package org.galaxio.gatling.kafka.javaapi.examples

import io.gatling.javaapi.core.CoreDsl.*
import io.gatling.javaapi.core.Simulation
import org.apache.kafka.clients.producer.ProducerConfig
import org.galaxio.gatling.kafka.javaapi.KafkaDsl.*
import java.time.Duration

class ProducerSimulation : Simulation() {

    // example of using custom serde
    private val ser = KafkaAvroSerializer(CachedSchemaRegistryClient("schRegUrl".split(','), 16),) as Serializer<MyAvroClass>
    private val de = KafkaAvroDeserializer(CachedSchemaRegistryClient("schRegUrl".split(','), 16),) as Deserializer<MyAvroClass>

    private val kafkaConsumerConf = kafka().topic("test.topic")
        .properties(mapOf<String, Any>(ProducerConfig.ACKS_CONFIG to "1",
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringSerializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringSerializer"
        ))

    private val scn = scenario("Basic")
        .exec(kafka("BasicRequest").send("foo"))
        .exec(kafka("dld").send("true", "12.0"))
        .exec(kafka("avro_serde").send("#{sessionIdKey}", avro(
            { session: Session -> session.get("event") },
                ser,
                de
            )
        ) { session: Session -> session.get("headers") }

    init {
        setUp(
            scn.injectOpen(atOnceUsers(1))
        )
            .protocols(kafkaConsumerConf)
            .maxDuration(Duration.ofSeconds(120))
    }

}