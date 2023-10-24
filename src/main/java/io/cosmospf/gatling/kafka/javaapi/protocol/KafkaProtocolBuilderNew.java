package io.cosmospf.gatling.kafka.javaapi.protocol;

import io.gatling.core.protocol.Protocol;
import io.gatling.javaapi.core.ProtocolBuilder;
import io.cosmospf.gatling.kafka.request.KafkaProtocolMessage;
import scala.Function1;

public class KafkaProtocolBuilderNew implements ProtocolBuilder {

    private io.cosmospf.gatling.kafka.protocol.KafkaProtocolBuilderNew wrapped;

    public KafkaProtocolBuilderNew(io.cosmospf.gatling.kafka.protocol.KafkaProtocolBuilderNew wrapped) {
        this.wrapped = wrapped;
    }

    public KafkaProtocolBuilderNew matchByValue() {
        this.wrapped = wrapped.matchByValue();
        return this;
    }

    public KafkaProtocolBuilderNew matchByMessage(Function1<KafkaProtocolMessage, byte[]> keyExtractor) {
        this.wrapped = wrapped.matchByMessage(keyExtractor);
        return this;
    }

    @Override
    public Protocol protocol() {
        return wrapped.build();
    }

}


