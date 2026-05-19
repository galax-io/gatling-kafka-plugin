package org.galaxio.gatling.kafka.javaapi.protocol;


import scala.jdk.javaapi.DurationConverters;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static scala.jdk.javaapi.CollectionConverters.asScala;

public class KafkaProtocolBuilderBase {

    /**
     * Start definition of the protocol for send only pattern
     *
     * @param name - topic for sending messages
     * @return KafkaProtocolBuilderPropertiesStep
     * @deprecated use topic definition in kafka request builders
     */
    @Deprecated(since = "1.0.0", forRemoval = true)
    public KafkaProtocolBuilderPropertiesStep topic(String name) {
        return new KafkaProtocolBuilderPropertiesStep(name, Collections.emptyMap());
    }

    /**
     * Start definition of the protocol for requestReply pattern
     *
     * @deprecated separate definition of the protocol for the requestReply scheme is no longer required; use producerSettings right away"
     */
    @Deprecated(since = "1.0.0", forRemoval = true)
    public KafkaProtocolBuilderNewBase requestReply() {
        return new KafkaProtocolBuilderNewBase();
    }

    public KPProducerSettingsStep producerSettings(Map<String, Object> ps) {
        return new KafkaProtocolBuilderNewBase().producerSettings(ps);
    }

    public KafkaProtocolBuilder properties(Map<String, Object> producerSettings) {
        scala.collection.immutable.Map<String, Object> ps = scala.collection.immutable.Map.from(asScala(producerSettings));
        scala.collection.immutable.Map<String, Object> cs = scala.collection.immutable.Map.from(asScala(Map.of()));
        return new KafkaProtocolBuilder(
                org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilder.apply(ps, cs, DurationConverters.toScala(Duration.ofSeconds(60)), org.galaxio.gatling.kafka.protocol.KafkaProtocol.KafkaKeyMatcher$.MODULE$)
        );
    }

}
