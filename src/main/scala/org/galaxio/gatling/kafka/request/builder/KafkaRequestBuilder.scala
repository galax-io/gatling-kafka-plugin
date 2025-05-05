package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.action.builder.ActionBuilder
import org.galaxio.gatling.kafka.actions.KafkaRequestActionBuilder

import scala.reflect.ClassTag

case class KafkaRequestBuilder[+K: ClassTag, +V: ClassTag](attributes: KafkaAttributes[K, V]) extends RequestBuilder[K, V] {

  def build: ActionBuilder = new KafkaRequestActionBuilder(attributes)

}
