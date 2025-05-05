package org.galaxio.gatling.kafka.javaapi.protocol;

import scala.jdk.javaapi.DurationConverters;

import java.time.Duration;
import java.util.Map;

import static scala.jdk.javaapi.CollectionConverters.asScala;

public class KPConsumeSettingsStep {

    private final Map<String, Object> producerSettings;
    private final Map<String, Object> consumerSettings;

    public KPConsumeSettingsStep(Map<String, Object> producerSettings, Map<String, Object> consumerSettings) {
        this.producerSettings = producerSettings;
        this.consumerSettings = consumerSettings;
    }

    public KafkaProtocolBuilder timeout(Duration timeout) {
        scala.collection.immutable.Map<String, Object> ps = scala.collection.immutable.Map.from(asScala(this.producerSettings));
        scala.collection.immutable.Map<String, Object> cs = scala.collection.immutable.Map.from(asScala(this.consumerSettings));
        return new KafkaProtocolBuilder(org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilder.apply(ps, cs, DurationConverters.toScala(timeout), org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher$.MODULE$));
    }

    public KafkaProtocolBuilder withDefaultTimeout() {
        scala.collection.immutable.Map<String, Object> ps = scala.collection.immutable.Map.from(asScala(this.producerSettings));
        scala.collection.immutable.Map<String, Object> cs = scala.collection.immutable.Map.from(asScala(this.consumerSettings));
        return new KafkaProtocolBuilder(org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilder.apply(ps, cs, DurationConverters.toScala(Duration.ofSeconds(60)), org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher$.MODULE$));
    }
}
