package io.gatling.core.stats

import io.gatling.commons.stats.Status
import io.gatling.core.actor.ActorRef
import io.gatling.core.controller.Controller
import io.gatling.core.session.GroupBlock

final class NoOpStatsEngine extends StatsEngine {
  override def start(): Unit = ()

  override private[gatling] def stop(controller: ActorRef[Controller.Command], exception: Option[Exception]): Unit = ()

  override def logUserStart(scenario: String): Unit = ()

  override def logUserEnd(scenario: String): Unit = ()

  override def logResponse(
      scenario: String,
      groups: List[String],
      requestName: String,
      startTimestamp: Long,
      endTimestamp: Long,
      status: Status,
      responseCode: Option[String],
      message: Option[String],
  ): Unit = ()

  override def logGroupEnd(
      scenario: String,
      groupBlock: GroupBlock,
      exitTimestamp: Long,
  ): Unit = ()

  override def logRequestCrash(
      scenario: String,
      groups: List[String],
      requestName: String,
      error: String,
  ): Unit = ()
}
