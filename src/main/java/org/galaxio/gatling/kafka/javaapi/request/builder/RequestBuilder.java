package org.galaxio.gatling.kafka.javaapi.request.builder;

import io.gatling.javaapi.core.ActionBuilder;

public class RequestBuilder<K, V> implements ActionBuilder {

    private final org.galaxio.gatling.kafka.request.builder.RequestBuilder<K, V> wrapped;

    public RequestBuilder(org.galaxio.gatling.kafka.request.builder.RequestBuilder<K,V> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return wrapped.build();
    }
}
