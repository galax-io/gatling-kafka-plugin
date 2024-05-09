package org.galaxio.gatling.kafka.javaapi.examples;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.galaxio.gatling.kafka.javaapi.protocol.KafkaProtocolBuilder;
import org.galaxio.gatling.kafka.javaapi.KafkaDsl;

import java.util.Map;

import static io.gatling.javaapi.core.CoreDsl.*;
import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.kafka;

public class ProducerSimulation extends Simulation {

    private final KafkaProtocolBuilder kafkaConsumerConf =
            KafkaDsl.kafka().topic("test.topic")
                    .properties(Map.of(ProducerConfig.ACKS_CONFIG, "1"));

    private final ScenarioBuilder scn = scenario("Basic")
            .exec(KafkaDsl.kafka("BasicRequest").send("foo"))
            .exec(KafkaDsl.kafka("BasicRequest SILENT").send("foo")
                    .silent())
            .exec(KafkaDsl.kafka("dld").send("true", 12.0));

}
