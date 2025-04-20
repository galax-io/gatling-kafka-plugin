package org.galaxio.gatling.kafka.javaapi.request.builder;


import io.gatling.commons.validation.Validation;
import io.gatling.core.session.Session;
import io.gatling.javaapi.core.internal.Expressions;

import java.util.function.Function;

public class RRInTopicStep {

    private final scala.Function1<Session, Validation<String>> inputTopic;
    private final String requestName;

    public RRInTopicStep(scala.Function1<Session, Validation<String>> inputTopic, String requestName) {
        this.inputTopic = inputTopic;
        this.requestName = requestName;
    }

    public RROutTopicStep replyTopic(String outputTopic) {
        return new RROutTopicStep(this.inputTopic,  Expressions.toStringExpression(outputTopic), this.requestName);
    }

    public RROutTopicStep replyTopic(Function<io.gatling.javaapi.core.Session, String> outputTopic) {
        return new RROutTopicStep(this.inputTopic, Expressions.javaFunctionToExpression(outputTopic), this.requestName);
    }
}
