package org.galaxio.gatling.kafka.request.builder

import io.gatling.core.action.builder.ActionBuilder

trait RequestBuilder[+K, +V] {

  def build: ActionBuilder
}
