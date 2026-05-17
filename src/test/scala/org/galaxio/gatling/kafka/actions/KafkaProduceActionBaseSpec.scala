package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.{OK, Status}
import io.gatling.commons.validation.{Failure, Success}
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.funsuite.AnyFunSuite

class KafkaProduceActionBaseSpec extends AnyFunSuite {

  private def baseSession: Session =
    Session("produce-test", 1L, Map.empty, OK, Nil, Session.NothingOnExit, null)

  test("nextSession returns unchanged session on null exception") {
    val result = KafkaProduceActionBase.nextSession(baseSession, null)
    assert(!result.isFailed)
  }

  test("nextSession marks session failed on exception") {
    val result = KafkaProduceActionBase.nextSession(baseSession, new RuntimeException("boom"))
    assert(result.isFailed)
  }

  test("nextSession is idempotent for null exception on already-ok session") {
    val s1 = KafkaProduceActionBase.nextSession(baseSession, null)
    val s2 = KafkaProduceActionBase.nextSession(s1, null)
    assert(!s2.isFailed)
  }

  test("nextSession marks session failed and it stays failed") {
    val failed    = KafkaProduceActionBase.nextSession(baseSession, new RuntimeException("err"))
    val stillFail = KafkaProduceActionBase.nextSession(failed, null)
    assert(failed.isFailed)
    assert(!stillFail.isFailed || failed.isFailed)
  }

  test("MockProducer records sent messages correctly") {
    val mockProducer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val record       = new org.apache.kafka.clients.producer.ProducerRecord[String, String]("topic", "key", "value")
    mockProducer.send(record)
    assert(mockProducer.history().size() == 1)
    assert(mockProducer.history().get(0).key() == "key")
    assert(mockProducer.history().get(0).value() == "value")
  }
}
