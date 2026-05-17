package org.galaxio.gatling.kafka.request.builder

import com.softwaremill.quicklens.ModifyPimp
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session._
import org.galaxio.gatling.kafka.actions.KafkaRequestActionBuilder

case class KafkaRequestBuilder[K, V](attributes: KafkaAttributes[K, V]) extends RequestBuilder[K, V] {

  def silent: KafkaRequestBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(true))

  def notSilent: KafkaRequestBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(false))

  def partition(p: Int): KafkaRequestBuilder[K, V] =
    this.modify(_.attributes.partition).setTo(Some(Integer.valueOf(p).expressionSuccess))

  def partition(p: Expression[java.lang.Integer]): KafkaRequestBuilder[K, V] =
    this.modify(_.attributes.partition).setTo(Some(p))

  def timestamp(ts: Long): KafkaRequestBuilder[K, V] =
    this.modify(_.attributes.timestamp).setTo(Some(java.lang.Long.valueOf(ts).expressionSuccess))

  def timestamp(ts: Expression[java.lang.Long]): KafkaRequestBuilder[K, V] =
    this.modify(_.attributes.timestamp).setTo(Some(ts))

  def build: ActionBuilder = new KafkaRequestActionBuilder(attributes)

}
