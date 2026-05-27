package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.action.builder.ActionBuilder
import org.galaxio.gatling.kafka.actions.KafkaRequestAvro4sActionBuilder

import scala.reflect.ClassTag

case class KafkaAvro4sRequestBuilder[K: ClassTag, V: ClassTag](attr: Avro4sAttributes[K, V]) extends RequestBuilder[K, V] {

  def build: ActionBuilder = new KafkaRequestAvro4sActionBuilder(attr)

}
