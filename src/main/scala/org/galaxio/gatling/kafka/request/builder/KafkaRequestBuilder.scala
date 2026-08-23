package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.action.builder.ActionBuilder
import org.galaxio.gatling.kafka.actions.KafkaRequestActionBuilder

import scala.reflect.ClassTag

/** What a produce-only `send(...)` returns.
  *
  * A `RequestBuilder[+K, +V]` trait sat in front of this until 2.0.0. It had one abstract member and one implementation, and
  * was public only because it was the declared return type of the documented `send` methods — an abstraction with no second
  * caller, which is what the freeze made expensive to remove rather than what made it worth keeping.
  */
case class KafkaRequestBuilder[+K: ClassTag, +V: ClassTag](attributes: KafkaAttributes[K, V]) {

  def build: ActionBuilder = new KafkaRequestActionBuilder(attributes)

}
