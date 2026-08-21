package org.galaxio.examples.javaapi;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Session;
import io.gatling.javaapi.core.Simulation;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.galaxio.gatling.kafka.javaapi.KafkaDsl;
import org.galaxio.gatling.kafka.javaapi.protocol.KafkaProtocolBuilder;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.gatling.javaapi.core.CoreDsl.atOnceUsers;
import static io.gatling.javaapi.core.CoreDsl.global;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static org.galaxio.gatling.kafka.javaapi.KafkaDsl.kafka;

public class ProducerSimulation extends Simulation {

    private final KafkaProtocolBuilder kafkaProducerConf =
            KafkaDsl.kafka()
                    .properties(
                            Map.of(
                                    ProducerConfig.ACKS_CONFIG, "1",
                                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093"
                            )
                    );

    private Headers header(Session session) {
        var uuid = Optional.ofNullable(session.getString("UUID")).orElse("");
        return new RecordHeaders().add("uuid-header", uuid.getBytes(Charset.defaultCharset()));
    }

    // Fed, not defaulted: without a feeder session.getString("UUID") is always null and the header
    // this example exists to demonstrate would ship empty on every record — passing its assertions
    // while proving nothing about the mechanism.
    private final Iterator<Map<String, Object>> uuidFeeder =
            Stream.generate((Supplier<Map<String, Object>>) () -> Map.of("UUID", UUID.randomUUID().toString()))
                    .iterator();

    private final ScenarioBuilder scn = scenario("Basic")
            .feed(uuidFeeder)
            .exec(kafka("BasicRequest").topic("ex.java.producer.t").send("foo"))
            .exec(kafka("dld").topic("ex.java.producer.t").send("true", 12.0))
            .exec(kafka("Msg1").topic("ex.java.producer.t").send("key", "val", this::header));

    {
        setUp(scn.injectOpen(atOnceUsers(1)))
                .protocols(kafkaProducerConf)
                .assertions(
                        global().allRequests().count().is(3L),
                        global().successfulRequests().percent().is(100.0)
                );
    }

}
