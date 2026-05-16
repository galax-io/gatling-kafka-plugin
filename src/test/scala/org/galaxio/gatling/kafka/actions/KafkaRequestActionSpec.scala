package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.OK
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import org.scalatest.funsuite.AnyFunSuite

class KafkaRequestActionSpec extends AnyFunSuite {

  private def baseSession: Session =
    Session("kafka-request", 1L, Map.empty, OK, Nil, Session.NothingOnExit, null)

  test("nextSession keeps session successful when producer callback succeeds") {
    val result = KafkaRequestAction.nextSession(baseSession, null)

    assert(!result.isFailed)
  }

  test("nextSession marks session as failed when producer callback receives exception") {
    val result = KafkaRequestAction.nextSession(baseSession, new RuntimeException("send failed"))

    assert(result.isFailed)
  }

  test("failure session can be propagated to next action") {
    @volatile var captured = Option.empty[Session]

    val nextAction = new Action {
      override def name: String = "capture-next"

      override def !(session: Session): Unit =
        execute(session)

      override def execute(session: Session): Unit =
        captured = Some(session)
    }

    nextAction ! KafkaRequestAction.nextSession(baseSession, new RuntimeException("send failed"))

    assert(captured.exists(_.isFailed))
  }
}
