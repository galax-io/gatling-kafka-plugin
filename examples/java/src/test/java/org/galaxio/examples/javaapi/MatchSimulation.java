package org.galaxio.examples.javaapi;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.galaxio.gatling.kafka.javaapi.KafkaDsl;
import org.galaxio.gatling.kafka.javaapi.protocol.KafkaProtocolBuilder;
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.gatling.javaapi.core.CoreDsl.atOnceUsers;
import static io.gatling.javaapi.core.CoreDsl.global;
import static io.gatling.javaapi.core.CoreDsl.scenario;

public class MatchSimulation extends Simulation {

    private final KafkaProtocolBuilder kafkaProtocolMatchByValue = KafkaDsl.kafka()
            .producerSettings(
                    Map.of(
                            ProducerConfig.ACKS_CONFIG, "1",
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093"
                    )
            )
            .consumeSettings(
                    Map.of("bootstrap.servers", "localhost:9093")
            )
            .timeout(Duration.ofSeconds(10))
            // for match by message value
            .matchByValue();

    private byte[] matchByOwnVal(KafkaProtocolMessage message) {
        // do something with the message and extract the values you are interested in
        // method is called:
        // - for each message which will be sent out
        // - for each message which has been received
        return "Custom Message".getBytes(); // just returning something
    }

    private final KafkaProtocolBuilder kafkaProtocolMatchByMessage = KafkaDsl.kafka()
            .producerSettings(
                    Map.of(
                            ProducerConfig.ACKS_CONFIG, "1",
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093"
                    )
            )
            .consumeSettings(
                    Map.of(
                            "bootstrap.servers", "localhost:9093"
                    )
            )
            .timeout(Duration.ofSeconds(10))
            .matchByMessage(this::matchByOwnVal);

    private final AtomicInteger c = new AtomicInteger(0);
    private final Iterator<Map<String, Object>> feeder =
            Stream.generate((Supplier<Map<String, Object>>) () -> Collections.singletonMap("kekey", c.incrementAndGet())
            ).iterator();

    private final ScenarioBuilder scn = scenario("Basic")
            .feed(feeder)
            .exec(
                    KafkaDsl.kafka("ReqRep").requestReply()
                            .requestTopic("ex.java.match.t")
                            .replyTopic("ex.java.match.t")
                            .send("#{kekey}", """
                                    { "m": "dkf" }
                                    """));

    {
        // Deliberately one user in flight: matchByOwnVal returns the same bytes for every message,
        // so any reply matches any request. The assertion is written to that bound, not above it.
        setUp(
                scn.injectOpen(atOnceUsers(1)))
                .protocols(kafkaProtocolMatchByMessage)
                .maxDuration(Duration.ofSeconds(120))
                .assertions(
                        global().allRequests().count().is(1L),
                        global().successfulRequests().percent().is(100.0)
                );
    }

}
