package io.cosmospf.gatling.kafka.request.builder

import io.gatling.core.action.builder.ActionBuilder
import io.cosmospf.gatling.kafka.actions.KafkaRequestActionBuilder

case class KafkaRequestBuilder[K, V](attr: KafkaAttributes[K, V]) extends RequestBuilder[K, V] {

  def build: ActionBuilder = new KafkaRequestActionBuilder(attr)

}
