package org.galaxio.gatling.kafka.javaapi.request.expressions;

import io.gatling.commons.validation.Validation;
import io.gatling.javaapi.core.internal.Expressions;
import org.apache.kafka.common.serialization.Serde;

public abstract class ExpressionBuilder<V> {
    private final JExpression<V> javaExpression;
    private final Class<V> type;
    private final Serde<V> serde;

    protected ExpressionBuilder(JExpression<V> javaExpression, Class<V> type, Serde<V> serde) {
        this.javaExpression = javaExpression;
        this.type = type;
        this.serde = serde;
    }

    scala.Function1<io.gatling.core.session.Session, Validation<byte[]>> bytes(String topic){
        return Expressions.javaFunctionToExpression(javaExpression.andThen(v -> serde.serializer().serialize(topic, v)));
    }

    public scala.Function1<io.gatling.core.session.Session, Validation<V>> gatlingExpression(){
        return Expressions.javaFunctionToExpression(javaExpression);
    }

    public Class<V> getType(){
        return type;
    }

    public Serde<V> getSerde() {
        return serde;
    }
}
