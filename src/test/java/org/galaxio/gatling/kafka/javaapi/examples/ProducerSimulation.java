package org.galaxio.gatling.kafka.javaapi.examples;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Session;
import io.gatling.javaapi.core.Simulation;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.galaxio.gatling.kafka.javaapi.KafkaDsl;
import org.galaxio.gatling.kafka.javaapi.protocol.KafkaProtocolBuilder;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;

import static io.gatling.javaapi.core.CoreDsl.scenario;
import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.kafka;

public class ProducerSimulation extends Simulation {

    private final KafkaProtocolBuilder kafkaConsumerConf =
            KafkaDsl.kafka()
                    .properties(Map.of(ProducerConfig.ACKS_CONFIG, "1"));

    private Headers header(Session session) {
        var uuid = Optional.ofNullable(session.getString("UUID")).orElse("");
        return new RecordHeaders().add("uuid-header", uuid.getBytes(Charset.defaultCharset()));
    }

    private final ScenarioBuilder scn = scenario("Basic")
            .exec(kafka("BasicRequest").topic("test.topic").send("foo"))
            .exec(kafka("dld").topic("test.topic").send("true", 12.0))
            .exec(kafka("Msg1").topic("test.topic").send("key", "val", this::header));

}
