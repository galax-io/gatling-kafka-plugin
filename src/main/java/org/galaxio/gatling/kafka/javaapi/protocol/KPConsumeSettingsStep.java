package org.galaxio.gatling.kafka.javaapi.protocol;

import java.time.Duration;
import java.util.Map;

import static scala.jdk.javaapi.CollectionConverters.asScala;
import scala.jdk.javaapi.DurationConverters;

public class KPConsumeSettingsStep {

    private final Map<String, Object> producerSettings;
    private final Map<String, Object> consumerSettings;

    public KPConsumeSettingsStep(Map<String, Object> producerSettings, Map<String, Object> consumerSettings) {
        this.producerSettings = producerSettings;
        this.consumerSettings = consumerSettings;
    }

    public KafkaProtocolBuilderNew timeout(Duration timeout) {
        scala.collection.immutable.Map<String, Object> ps = scala.collection.immutable.Map.from(asScala(this.producerSettings));
        scala.collection.immutable.Map<String, Object> cs = scala.collection.immutable.Map.from(asScala(this.consumerSettings));
        return new KafkaProtocolBuilderNew(org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilderNew.apply(ps, cs, DurationConverters.toScala(timeout), org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher$.MODULE$));
    }

    public KafkaProtocolBuilderNew withDefaultTimeout() {
        scala.collection.immutable.Map<String, Object> ps = scala.collection.immutable.Map.from(asScala(this.producerSettings));
        scala.collection.immutable.Map<String, Object> cs = scala.collection.immutable.Map.from(asScala(this.consumerSettings));
        return new KafkaProtocolBuilderNew(org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilderNew.apply(ps, cs, DurationConverters.toScala(Duration.ofSeconds(60)), org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher$.MODULE$));
    }
}
