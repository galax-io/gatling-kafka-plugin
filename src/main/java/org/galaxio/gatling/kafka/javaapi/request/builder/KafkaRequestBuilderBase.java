package org.galaxio.gatling.kafka.javaapi.request.builder;

import static io.gatling.javaapi.core.internal.Expressions.toStaticValueExpression;

/**
 * Entry point for a Kafka request in the Java DSL.
 *
 * <p>A request names its producer topic before it names its payload:
 * {@code kafka(name).topic(t).send(...)} for produce-only, and
 * {@code kafka(name).requestReply().requestTopic(rt).replyTopic(ct).send(...)} for request-reply.
 *
 * <p>The {@code send(...)} and {@code sendWithClass(...)} overloads that used to sit here were removed in 2.0.0.
 * They built actions carrying no producer topic, so every scenario reaching one failed at send time; the
 * headers-taking {@code sendWithClass} additionally threw {@code IllegalArgumentException} from
 * {@code Serdes.serdeFrom(Object.class)} before a scenario could even be constructed.
 */
public class KafkaRequestBuilderBase {

    private final org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilderBase wrapped;
    private final String requestName;

    public KafkaRequestBuilderBase(org.galaxio.gatling.kafka.request.builder.KafkaRequestBuilderBase wrapped, String requestName) {
        this.wrapped = wrapped;
        this.requestName = requestName;
    }

    public ReqRepBase requestReply() {
        return new ReqRepBase(requestName);
    }

    // Delegates to the wrapped Scala builder rather than rebuilding the step from `requestName`: one
    // construction path, and the request name cannot drift between the two representations.
    public OnlyPublishStep topic(String producerTopic) {
        return new OnlyPublishStep(wrapped.topic(toStaticValueExpression(producerTopic)));
    }
}
