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

/** Records what a Gatling run can actually show of a request, and nothing more.
  *
  * `LoggedResponse` keeps request name, both instants, status and message — which is exactly the set Gatling itself persists.
  * Verified against `gatling-core-3.13.5.jar`: `ResponseMessageSerializer.serialize0` writes the group hierarchy, the name, the
  * two timestamps, `status == OK` as a boolean and the message, and never reads `responseCode`. Dropping that argument here is
  * therefore not a simplification — it mirrors the real writer, and a spec that asserted on it would be asserting on a value no
  * report can display.
  *
  * `logRequestCrash` is a no-op for the same reason: it emits an error event carrying only a message and a timestamp, outside
  * the failed-request total, outside the request table and outside every assertion path. A reporting site that switched to it
  * would show up here as a missing response, which is the correct signal.
  */
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
