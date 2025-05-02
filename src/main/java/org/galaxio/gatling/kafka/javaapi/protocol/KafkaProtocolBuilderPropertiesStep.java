package org.galaxio.gatling.kafka.javaapi.protocol;

import java.util.Map;

import static scala.jdk.javaapi.CollectionConverters.asScala;

public class KafkaProtocolBuilderPropertiesStep {

    private final String topic;
    private Map<String, Object> props;

    public KafkaProtocolBuilderPropertiesStep(String topic, Map<String, Object> props) {
        this.topic = topic;
        this.props = props;
    }

    public KafkaProtocolBuilderBackwardCompatible properties(Map<String, Object> props) {
        this.props = props;
        scala.collection.immutable.Map<String, Object> scalaMap = scala.collection.immutable.Map.from(asScala(this.props));
        return new KafkaProtocolBuilderBackwardCompatible(
                new org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilder.KafkaProtocolBuilderPropertiesStep(this.topic)
                        .properties(scalaMap)
        );
    }


}
