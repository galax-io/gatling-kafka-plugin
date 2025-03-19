package org.galaxio.gatling.kafka.javaapi.request.expressions;

@FunctionalInterface
public interface JExpression<T> extends java.util.function.Function<io.gatling.javaapi.core.Session, T>{}