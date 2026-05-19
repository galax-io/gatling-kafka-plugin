package org.galaxio.gatling.kafka.javaapi.request.builder;

import io.gatling.javaapi.core.internal.Expressions;
import org.galaxio.gatling.kafka.javaapi.request.expressions.JExpression;

public class ReqRepBase {

    private final String requestName;

    public ReqRepBase(String requestName) {
        this.requestName = requestName;
    }

    public RRInTopicStep requestTopic(String inputTopic) {
        return new RRInTopicStep(Expressions.toStringExpression(inputTopic), this.requestName);
    }

    public RRInTopicStep requestTopic(JExpression<String> inputTopic) {
        return new RRInTopicStep(Expressions.javaFunctionToExpression(inputTopic), this.requestName);
    }
}
