package org.galaxio.gatling.kafka.javaapi.protocol;

import io.gatling.core.protocol.Protocol;
import io.gatling.javaapi.core.ProtocolBuilder;
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage;
import scala.Function1;

public class KafkaProtocolBuilder implements ProtocolBuilder {

    private org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilder wrapped;

    public KafkaProtocolBuilder(org.galaxio.gatling.kafka.protocol.KafkaProtocolBuilder wrapped) {
        this.wrapped = wrapped;
    }

    public KafkaProtocolBuilder matchByValue() {
        this.wrapped = wrapped.matchByValue();
        return this;
    }

    public KafkaProtocolBuilder matchByMessage(Function1<KafkaProtocolMessage, byte[]> keyExtractor) {
        this.wrapped = wrapped.matchByMessage(keyExtractor);
        return this;
    }

    @Override
    public Protocol protocol() {
        return wrapped.build();
    }

}
