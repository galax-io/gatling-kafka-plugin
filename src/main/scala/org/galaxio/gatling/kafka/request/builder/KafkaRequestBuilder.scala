package org.galaxio.gatling.kafka.request.builder

import com.softwaremill.quicklens.ModifyPimp
import io.gatling.core.action.builder.ActionBuilder
import org.galaxio.gatling.kafka.actions.KafkaRequestActionBuilder

case class KafkaRequestBuilder[K, V](attributes: KafkaAttributes[K, V]) extends RequestBuilder[K, V] {

  def silent: KafkaRequestBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(true))

  def notSilent: KafkaRequestBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(false))

  def build: ActionBuilder = KafkaRequestActionBuilder(attributes)

}
