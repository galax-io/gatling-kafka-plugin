package io.gatling.core.stats

import io.gatling.commons.stats.Status
import io.gatling.core.actor.ActorRef
import io.gatling.core.controller.Controller
import io.gatling.core.session.GroupBlock

import java.util.concurrent.atomic.AtomicReference

final case class LoggedResponse(
    requestName: String,
    startTimestamp: Long,
    endTimestamp: Long,
    status: Status,
    message: Option[String],
)

final class RecordingStatsEngine extends StatsEngine {
  val responses: AtomicReference[Vector[LoggedResponse]] = new AtomicReference(Vector.empty)

  override def start(): Unit = ()

  override def stop(controller: ActorRef[Controller.Command], exception: Option[Exception]): Unit = ()

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
  ): Unit =
    responses.updateAndGet(
      _ :+ LoggedResponse(
        requestName = requestName,
        startTimestamp = startTimestamp,
        endTimestamp = endTimestamp,
        status = status,
        message = message,
      ),
    )

  override def logGroupEnd(scenario: String, groupBlock: GroupBlock, exitTimestamp: Long): Unit = ()

  override def logRequestCrash(scenario: String, groups: List[String], requestName: String, error: String): Unit = ()
}
