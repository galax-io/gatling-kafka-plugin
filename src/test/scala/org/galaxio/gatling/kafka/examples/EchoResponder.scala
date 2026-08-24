package org.galaxio.gatling.kafka.examples

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.galaxio.gatling.kafka.client.{DynamicKafkaConsumer, KafkaSender}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}

/** Stands in for the service under test: consumes each request topic in `echoRoutes` and republishes the record onto the reply
  * topic it maps to.
  *
  * Before this existed the request-reply scenarios were "answered" by sibling fire-and-forget scenarios that happened to
  * publish a matching key or value at a fixed delay — so the suite exercised the matching code without ever exercising a round
  * trip, and was green because of timing rather than because the plugin correlated anything (issue #196).
  *
  * Shared rather than copied per simulation: `KafkaGatlingTest` and `KafkaFailureModesGatlingTest` both need one, and a
  * responder that answers slightly differently in each is a difference nobody would notice until a scenario failed for a reason
  * that had nothing to do with what it tests.
  *
  * @param groupId
  *   distinct per simulation. A shared id would put two responders in one consumer group, and the second run would sit out the
  *   coordinator's session timeout waiting for the first one's stale member to be evicted.
  * @param tombstoneRoutes
  *   request topics answered with a null value instead of an echo. A tombstone is ordinary traffic on a compacted topic, and it
  *   used to take the virtual user down with it: the body check NPE'd inside the tracker, which had no catch, so the user was
  *   never continued — no success, no failure, no next request (issue #168).
  */
private[examples] object EchoResponder {

  /** The header a request-reply may correlate on, echoed back onto the reply.
    *
    * Shared with the scenarios rather than spelled twice: a header name that matches on one side and not the other produces a
    * reply timeout, which is exactly the symptom issue #228 is about and the last thing a test for it should reproduce by
    * accident.
    */
  val CorrelationHeader = "x-correlation-id"

  /** Reads the correlation header off a message, on the request side and again on the reply side.
    *
    * `null` when the header is absent, which is the contract `KafkaMatcher` uses to say "nothing to correlate on" — an empty
    * array would be a present id that every such message shares (issue #167).
    */
  def correlationIdFromHeader(message: KafkaProtocolMessage): Array[Byte] =
    message.headers.flatMap(hs => Option(hs.lastHeader(CorrelationHeader))).map(_.value()).orNull
}

private[examples] final class EchoResponder(
    bootstrap: String,
    groupId: String,
    echoRoutes: Map[String, String],
    tombstoneRoutes: Set[String] = Set.empty,
) extends StrictLogging {

  /** Header carrying when the responder answered. Round-trip metadata has to ride here rather than in the key or the value:
    * scenarios match by key and check the value, or match by value and check the bytes. Any rewrite of either breaks
    * correlation, a check, or both.
    */
  private val RespondedAtHeader = "x-responded-at"

  private val probeMarker = "_probe"

  private val sender = KafkaSender(
    Map(
      ProducerConfig.ACKS_CONFIG                   -> "1",
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> bootstrap,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ),
  )

  /** Request topics this responder has demonstrably answered on — a *successful* echo send, not merely a record received.
    *
    * Readiness used to be one latch counted down on the first record, before the probe short-circuit and without ever sending
    * anything. That proved the consumer was alive and nothing else: a wrong route map, a dead producer or a consumer that died
    * right after would all have passed it, and the simulation would then have gone green anyway because a sibling scenario
    * answered instead.
    */
  private val echoedRoutes: java.util.Set[String] = ConcurrentHashMap.newKeySet[String]()

  private val consumer = DynamicKafkaConsumer[Array[Byte], Array[Byte]](
    Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> bootstrap,
      ConsumerConfig.GROUP_ID_CONFIG                 -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "earliest",
    ),
    echoRoutes.keySet,
    record => {
      // Probes are echoed like anything else. Skipping them was what made readiness vacuous: the send
      // path — the half that actually matters — was never exercised before the simulation started.
      // Their key and value are the probe marker, which no scenario correlates on, so the echo lands on
      // the reply topic and is discarded.
      echoRoutes.get(record.topic()).foreach { replyTopic =>
        val headers = new RecordHeaders()
        headers.add(RespondedAtHeader, System.currentTimeMillis().toString.getBytes)
        // Echo the request's correlation header, if it carries one.
        //
        // A real service answering a correlated request returns the id it was given; this responder built
        // fresh headers and dropped the request's, so a header-correlated reply could never be matched and
        // the shape the Migration Guide recommends for tombstone-answering services had no coverage at all
        // (issue #228). Copied rather than regenerated: correlation is only meaningful if the value comes
        // back unchanged.
        Option(record.headers().lastHeader(EchoResponder.CorrelationHeader))
          .foreach(h => headers.add(EchoResponder.CorrelationHeader, h.value()))
        // The probe has to be echoed intact or `awaitReady` never completes for this route, so only
        // non-probe records get the tombstone treatment.
        val isProbe = record.value() != null && new String(record.value()) == probeMarker
        val value   = if (tombstoneRoutes.contains(record.topic()) && !isProbe) null else record.value()
        sender.send(
          KafkaProtocolMessage(record.key(), value, replyTopic, replyTopic, Some(headers)),
        )(
          _ => { echoedRoutes.add(record.topic()); () },
          // Never silent: a responder that stops echoing looks exactly like the plugin losing replies,
          // and the run would fail its assertion with nothing pointing at the real cause. The route is
          // named because that is the part a reader cannot reconstruct from the stack trace.
          error => logger.error(s"[$groupId] failed to echo ${record.topic()} onto $replyTopic", error),
        )
      }
    },
    error => logger.error(s"[$groupId] consumer failed, replies stop here", error),
  )

  private val thread = new Thread(consumer, groupId)

  /** Starts the responder and blocks until it has echoed on every route.
    *
    * Cleans up and rethrows on failure: Gatling does not run `after` when `before` throws, and a responder left in the group
    * blocks the next run until the coordinator times its stale member out.
    */
  def start(timeoutSeconds: Int = 30): Unit = {
    thread.setDaemon(true)
    thread.start()
    try awaitReady(timeoutSeconds)
    catch {
      case error: Throwable =>
        close()
        throw error
    }
  }

  def close(): Unit = {
    consumer.close()
    sender.close()
  }

  /** Probes every route and waits until each has been echoed successfully.
    *
    * Every route, not just one: with a route map there is no reason to believe the second entry works because the first does,
    * and a typo in either would otherwise surface as a mysterious reply timeout mid-run.
    */
  private def awaitReady(timeoutSeconds: Int): Unit = {
    val deadline = System.currentTimeMillis() + timeoutSeconds * 1000L
    while (echoedRoutes.size < echoRoutes.size && System.currentTimeMillis() < deadline) {
      echoRoutes.keys.foreach { requestTopic =>
        val probe = KafkaProtocolMessage(probeMarker.getBytes, probeMarker.getBytes, requestTopic, requestTopic)
        val sent  = new CountDownLatch(1)
        sender.send(probe)(_ => sent.countDown(), _ => sent.countDown())
        sent.await(2, TimeUnit.SECONDS)
      }
      Thread.sleep(250)
    }
    require(
      echoedRoutes.size == echoRoutes.size,
      s"[$groupId] did not echo on every route within ${timeoutSeconds}s: " +
        s"echoed ${echoedRoutes.toArray.mkString("[", ", ", "]")}, expected ${echoRoutes.keys.mkString("[", ", ", "]")}",
    )
  }
}
