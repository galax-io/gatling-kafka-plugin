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

public class ConsumeActionBuilder implements ActionBuilder {

    private org.galaxio.gatling.kafka.actions.KafkaConsumeActionBuilder wrapped;

    public ConsumeActionBuilder(org.galaxio.gatling.kafka.actions.KafkaConsumeActionBuilder wrapped) {
        this.wrapped = wrapped;
    }

    public ConsumeActionBuilder check(CheckBuilder... checks) {
        return check(Arrays.asList(checks));
    }

    public ConsumeActionBuilder check(List<CheckBuilder> checks) {
        this.wrapped = wrapped.check(KafkaChecks.toScalaChecks(checks));
        return this;
    }

    public ConsumeActionBuilder consumeSettings(Map<String, Object> settings) {
        this.wrapped = wrapped.consumeSettings(scalaMap(settings));
        return this;
    }

    public ConsumeActionBuilder replyMatchBy(Function<KafkaProtocolMessage, byte[]> extractor) {
        this.wrapped = wrapped.replyMatchBy(FunctionConverters.asScalaFromFunction(extractor));
        return this;
    }

    public ConsumeActionBuilder matchByMessage(Function<KafkaProtocolMessage, byte[]> extractor) {
        this.wrapped = wrapped.matchByMessage(FunctionConverters.asScalaFromFunction(extractor));
        return this;
    }

    public ConsumeActionBuilder matchByKafkaMatcher(KafkaProtocol.KafkaMatcher matcher) {
        this.wrapped = wrapped.matchByKafkaMatcher(matcher);
        return this;
    }

    public <T> ConsumeActionBuilder saveAs(String sessionKey, Function<KafkaProtocolMessage, T> extractor) {
        this.wrapped = wrapped.saveAs(
                sessionKey,
                FunctionConverters.asScalaFromFunction((Function<KafkaProtocolMessage, Object>) extractor::apply)
        );
        return this;
    }

    public ConsumeActionBuilder silent() {
        this.wrapped = wrapped.silent();
        return this;
    }

    public ConsumeActionBuilder notSilent() {
        this.wrapped = wrapped.notSilent();
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
