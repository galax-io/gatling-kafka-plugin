package io.cosmospf.gatling.kafka.javaapi.request.builder;

import io.gatling.javaapi.core.ActionBuilder;
import io.cosmospf.gatling.kafka.javaapi.checks.KafkaChecks;

import java.util.Arrays;
import java.util.List;

public class RequestReplyBuilder<K, V> implements ActionBuilder {

    private io.cosmospf.gatling.kafka.actions.KafkaRequestReplyActionBuilder<K, V> wrapped;

    public RequestReplyBuilder(io.cosmospf.gatling.kafka.actions.KafkaRequestReplyActionBuilder<K,V> wrapped) {
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