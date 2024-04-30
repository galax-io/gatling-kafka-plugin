package org.galaxio.gatling.kafka.request.builder

import com.softwaremill.quicklens.ModifyPimp
import io.gatling.core.action.builder.ActionBuilder
import org.galaxio.gatling.kafka.actions.KafkaRequestAvro4sActionBuilder

import scala.reflect.ClassTag

case class KafkaAvro4sRequestBuilder[K: ClassTag, V: ClassTag](attributes: Avro4sAttributes[K, V])
    extends RequestBuilder[K, V] {

  def build: ActionBuilder = KafkaRequestAvro4sActionBuilder(attributes)

  override def silent: RequestBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(true))

  override def notSilent: RequestBuilder[K, V] = this.modify(_.attributes.silent).setTo(Some(false))
}
