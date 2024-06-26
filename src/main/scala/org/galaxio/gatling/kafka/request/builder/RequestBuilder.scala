package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.action.builder.ActionBuilder

trait RequestBuilder[K, V] {

  def build: ActionBuilder

  def silent: RequestBuilder[K, V]

  def notSilent: RequestBuilder[K, V]

}
