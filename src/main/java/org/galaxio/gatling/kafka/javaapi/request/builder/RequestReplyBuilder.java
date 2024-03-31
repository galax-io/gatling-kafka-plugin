package org.galaxio.gatling.kafka.javaapi.request.builder;

import io.gatling.javaapi.core.ActionBuilder;
import org.galaxio.gatling.kafka.javaapi.checks.KafkaChecks;
import org.galaxio.gatling.kafka.actions.KafkaRequestReplyActionBuilder;

import java.util.Arrays;
import java.util.List;

public class RequestReplyBuilder<K, V> implements ActionBuilder {

    private KafkaRequestReplyActionBuilder<K, V> wrapped;

    public RequestReplyBuilder(KafkaRequestReplyActionBuilder<K,V> wrapped) {
        this.wrapped = wrapped;
    }

    public RequestReplyBuilder<K, V> check(Object... checks) {
        return check(Arrays.asList(checks));
    }

    public RequestReplyBuilder<K, V> check(List<Object> checks) {
        this.wrapped = wrapped.check(KafkaChecks.toScalaChecks(checks));
        return this;
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return wrapped;
    }
}