package org.galaxio.gatling.kafka.javaapi.request.builder;

import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.javaapi.core.CheckBuilder;
import org.galaxio.gatling.kafka.javaapi.checks.KafkaChecks;
import org.galaxio.gatling.kafka.protocol.KafkaProtocol;
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.FunctionConverters;

public class RequestReplyBuilder<K, V> implements ActionBuilder {

    private org.galaxio.gatling.kafka.actions.KafkaRequestReplyActionBuilder<K, V> wrapped;

    public RequestReplyBuilder(org.galaxio.gatling.kafka.actions.KafkaRequestReplyActionBuilder<K,V> wrapped) {
        this.wrapped = wrapped;
    }

    public RequestReplyBuilder<K, V> check(CheckBuilder... checks) {
        return check(Arrays.asList(checks));
    }

    public RequestReplyBuilder<K, V> check(List<CheckBuilder> checks) {
        this.wrapped = wrapped.check(KafkaChecks.toScalaChecks(checks));
        return this;
    }

    public RequestReplyBuilder<K, V> producerSettings(Map<String, Object> settings) {
        this.wrapped = wrapped.producerSettings(scalaMap(settings));
        return this;
    }

    public RequestReplyBuilder<K, V> consumeSettings(Map<String, Object> settings) {
        this.wrapped = wrapped.consumeSettings(scalaMap(settings));
        return this;
    }

    public RequestReplyBuilder<K, V> requestMatchBy(Function<KafkaProtocolMessage, byte[]> extractor) {
        this.wrapped = wrapped.requestMatchBy(FunctionConverters.asScalaFromFunction(extractor));
        return this;
    }

    public RequestReplyBuilder<K, V> replyMatchBy(Function<KafkaProtocolMessage, byte[]> extractor) {
        this.wrapped = wrapped.replyMatchBy(FunctionConverters.asScalaFromFunction(extractor));
        return this;
    }

    public RequestReplyBuilder<K, V> matchByMessage(Function<KafkaProtocolMessage, byte[]> extractor) {
        this.wrapped = wrapped.matchByMessage(FunctionConverters.asScalaFromFunction(extractor));
        return this;
    }

    public RequestReplyBuilder<K, V> matchByKafkaMatcher(KafkaProtocol.KafkaMatcher matcher) {
        this.wrapped = wrapped.matchByKafkaMatcher(matcher);
        return this;
    }

    public <T> RequestReplyBuilder<K, V> saveReplyAs(String sessionKey, Function<KafkaProtocolMessage, T> extractor) {
        this.wrapped = wrapped.saveReplyAs(
                sessionKey,
                FunctionConverters.asScalaFromFunction((Function<KafkaProtocolMessage, Object>) extractor::apply)
        );
        return this;
    }

    private scala.collection.immutable.Map<String, Object> scalaMap(Map<String, Object> settings) {
        return scala.collection.immutable.Map.from(CollectionConverters.asScala(settings));
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return wrapped;
    }
}
