package org.galaxio.gatling.kafka.javaapi.examples;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Session;
import io.gatling.javaapi.core.Simulation;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.galaxio.gatling.kafka.javaapi.protocol.KafkaProtocolBuilder;
import org.galaxio.gatling.kafka.javaapi.KafkaDsl;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;

import static io.gatling.javaapi.core.CoreDsl.*;
import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.kafka;

public class ProducerSimulation extends Simulation {

    private final KafkaProtocolBuilder kafkaConsumerConf =
            KafkaDsl.kafka().topic("test.topic")
                    .properties(Map.of(ProducerConfig.ACKS_CONFIG, "1"));

    private Headers header(Session session) {
        var uuid = Optional.ofNullable(session.getString("UUID")).orElse("");
        return new RecordHeaders().add("uuid-header", uuid.getBytes(Charset.defaultCharset()));
    }

    private final ScenarioBuilder scn = scenario("Basic")
            .exec(kafka("BasicRequest").send("foo"))
            .exec(kafka("dld").send("true", 12.0))
            .exec(kafka("Msg1").send("key", "val", this::header));

}
