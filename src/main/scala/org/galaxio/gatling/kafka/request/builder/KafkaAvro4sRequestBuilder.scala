package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.action.builder.ActionBuilder
import io.gatling.internal.quicklens._
import org.galaxio.gatling.kafka.actions.KafkaRequestAvro4sActionBuilder

case class KafkaAvro4sRequestBuilder[K, V](attributes: Avro4sAttributes[K, V]) extends RequestBuilder[K, V] {

  def silent: KafkaAvro4sRequestBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(true))

  def notSilent: KafkaAvro4sRequestBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(false))

  def build: ActionBuilder = new KafkaRequestAvro4sActionBuilder(attributes)

}
