package org.galaxio.gatling.kafka.javaapi.request.builder;

import io.gatling.commons.validation.Validation;
import io.gatling.core.session.Session;
import io.gatling.javaapi.core.internal.Expressions;
import org.galaxio.gatling.kafka.actions.KafkaConsumeActionBuilder;
import org.galaxio.gatling.kafka.javaapi.request.expressions.ExpressionBuilder;

public class ReceiveFromStep {

    private final scala.Function1<Session, Validation<String>> topicExpression;
    private final scala.Function1<Session, Validation<String>> requestNameExpression;

    public ReceiveFromStep(
            scala.Function1<Session, Validation<String>> topicExpression,
            scala.Function1<Session, Validation<String>> requestNameExpression
    ) {
        this.topicExpression = topicExpression;
        this.requestNameExpression = requestNameExpression;
    }

    public ConsumeActionBuilder matchIdForTracking(byte[] expectedMatchId) {
        return new ConsumeActionBuilder(
                KafkaConsumeActionBuilder.create(
                        requestNameExpression,
                        topicExpression,
                        Expressions.toStaticValueExpression(expectedMatchId)
                )
        );
    }

    public ConsumeActionBuilder matchIdForTracking(ExpressionBuilder<byte[]> expectedMatchId) {
        return new ConsumeActionBuilder(
                KafkaConsumeActionBuilder.create(
                        requestNameExpression,
                        topicExpression,
                        expectedMatchId.gatlingExpression()
                )
        );
    }

    public ConsumeActionBuilder expectMatchId(byte[] expectedMatchId) {
        return matchIdForTracking(expectedMatchId);
    }

    public ConsumeActionBuilder expectMatchId(ExpressionBuilder<byte[]> expectedMatchId) {
        return matchIdForTracking(expectedMatchId);
    }
}
