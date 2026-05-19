package org.galaxio.gatling.kafka.actions

import io.gatling.commons.stats.{KO, OK, Status}
import io.gatling.commons.util.DefaultClock
import io.gatling.commons.validation.{Failure, Success, Validation}
import io.gatling.core.action.Action
import io.gatling.core.Predef._
import io.gatling.core.actor.ActorSystem
import io.gatling.core.session.Session
import io.gatling.core.stats.{NoOpStatsEngine => GatlingNoOpStatsEngine, StatsEngine}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringSerializer}
import org.galaxio.gatling.kafka.Predef._
import org.galaxio.gatling.kafka.client.{KafkaSenderPool, KafkaTrackersPoolFactory}
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol, KafkaProtocolBuilderNew}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.{KafkaReplyExtraction, KafkaRequestReplyAttributes}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.DockerClientFactory
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.immutable.List
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.Try

class KafkaRequestReplyActionIntegrationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private val externalBootstrap      = sys.env
    .get("GATLING_KAFKA_TEST_BOOTSTRAP")
    .orElse(sys.props.get("gatling.kafka.test.bootstrap"))
  private val kafkaContainer         = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"))
  private val actorSystem            = new ActorSystem()
  private val clock                  = new DefaultClock
  private lazy val dockerAvailable   = dockerSocketLikelyPresent && Try(DockerClientFactory.instance().client()).isSuccess
  private lazy val bootstrapServers  = externalBootstrap.getOrElse(kafkaContainer.getBootstrapServers)
  private lazy val canRunIntegration =
    externalBootstrap.isDefined || dockerAvailable

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (externalBootstrap.isEmpty && dockerAvailable) {
      kafkaContainer.start()
    }
  }

  override def afterAll(): Unit = {
    actorSystem.close()
    if (externalBootstrap.isEmpty && dockerAvailable) {
      kafkaContainer.stop()
    }
    super.afterAll()
  }

  test("action-level broker overrides and reply extraction work end-to-end with asymmetric matcher") {
    assume(
      canRunIntegration,
      "Integration tests require either GATLING_KAFKA_TEST_BOOTSTRAP or a Docker environment for Testcontainers",
    )
    val requestTopic = s"rr-request-${UUID.randomUUID()}"
    val replyTopic   = s"rr-reply-${UUID.randomUUID()}"

    createTopics(requestTopic, replyTopic)

    withResponder(requestTopic, replyTopic) { record =>
      new ProducerRecord[Array[Byte], Array[Byte]](
        replyTopic,
        "reply-key".getBytes(),
        record.key(),
      )
    } {
      val components = kafkaComponents(invalidBaseProtocol)
      val attributes = requestReplyAttributes(
        requestTopic = requestTopic,
        replyTopic = replyTopic,
        key = "corr-override",
        value = "request-payload",
        producerSettingsOverride = Some(Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers)),
        consumeSettingsOverride = Some(Map("bootstrap.servers" -> bootstrapServers)),
        requestMatchExtractor = Some(_.key),
        responseMatchExtractor = Some(_.value),
        replyExtractions = List(KafkaReplyExtraction("replyValue", msg => new String(msg.value))),
        checks = List(bodyBytes.is("corr-override".getBytes())),
      )

      val resultSession = awaitActionResult(components, attributes)

      assert(!resultSession.isFailed)
      assert(resultSession("replyValue").as[String] == "corr-override")
    }
  }

  test("different action-level matchers on the same topics do not reuse an incompatible tracker") {
    assume(
      canRunIntegration,
      "Integration tests require either GATLING_KAFKA_TEST_BOOTSTRAP or a Docker environment for Testcontainers",
    )
    val requestTopic = s"rr-shared-request-${UUID.randomUUID()}"
    val replyTopic   = s"rr-shared-reply-${UUID.randomUUID()}"

    createTopics(requestTopic, replyTopic)

    withResponder(requestTopic, replyTopic) { record =>
      new ProducerRecord[Array[Byte], Array[Byte]](
        replyTopic,
        record.key(),
        record.value(),
      )
    } {
      val components = kafkaComponents(invalidBaseProtocol)

      val firstAttributes = requestReplyAttributes(
        requestTopic = requestTopic,
        replyTopic = replyTopic,
        key = "first-key",
        value = "first-payload",
        producerSettingsOverride = Some(Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers)),
        consumeSettingsOverride = Some(Map("bootstrap.servers" -> bootstrapServers)),
        requestMatchExtractor = Some(_.key),
        responseMatchExtractor = Some(_.key),
        replyExtractions = List(KafkaReplyExtraction("firstReply", msg => new String(msg.value))),
        checks = List(bodyBytes.is("first-payload".getBytes())),
      )

      val secondAttributes = requestReplyAttributes(
        requestTopic = requestTopic,
        replyTopic = replyTopic,
        key = "second-key",
        value = "second-payload",
        producerSettingsOverride = Some(Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers)),
        consumeSettingsOverride = Some(Map("bootstrap.servers" -> bootstrapServers)),
        requestMatchExtractor = Some(_.value),
        responseMatchExtractor = Some(_.value),
        replyExtractions = List(KafkaReplyExtraction("secondReply", msg => new String(msg.value))),
        checks = List(bodyBytes.is("second-payload".getBytes())),
      )

      val firstSession  = awaitActionResult(components, firstAttributes)
      val secondSession = awaitActionResult(components, secondAttributes)

      assert(!firstSession.isFailed)
      assert(!secondSession.isFailed)
      assert(firstSession("firstReply").as[String] == "first-payload")
      assert(secondSession("secondReply").as[String] == "second-payload")
    }
  }

  private def invalidBaseProtocol: KafkaProtocol =
    KafkaProtocolBuilderNew
      .producerSettings(
        Map(
          ProducerConfig.ACKS_CONFIG              -> "1",
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "invalid-host:9099",
        ),
      )
      .consumeSettings(
        Map(
          "bootstrap.servers" -> "invalid-host:9099",
        ),
      )
      .timeout(5.seconds)
      .build

  private def kafkaComponents(protocol: KafkaProtocol): KafkaComponents =
    KafkaComponents(
      coreComponents = null,
      kafkaProtocol = protocol,
      trackersPoolFactory = new KafkaTrackersPoolFactory(actorSystem, NoOpStatsEngine, clock),
      senderProvider = new KafkaSenderPool,
    )

  private def requestReplyAttributes(
      requestTopic: String,
      replyTopic: String,
      key: String,
      value: String,
      producerSettingsOverride: Option[Map[String, AnyRef]],
      consumeSettingsOverride: Option[Map[String, AnyRef]],
      requestMatchExtractor: Option[KafkaProtocolMessage => Array[Byte]],
      responseMatchExtractor: Option[KafkaProtocolMessage => Array[Byte]],
      replyExtractions: List[KafkaReplyExtraction],
      checks: List[org.galaxio.gatling.kafka.KafkaCheck],
  ): KafkaRequestReplyAttributes[String, String] =
    KafkaRequestReplyAttributes(
      requestName = _ => Success("request-reply-integration"),
      inputTopic = _ => Success(requestTopic),
      outputTopic = _ => Success(replyTopic),
      key = _ => Success(key),
      value = _ => Success(value),
      headers = None,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer,
      checks = checks,
      silent = Some(false),
      producerSettingsOverride = producerSettingsOverride,
      consumeSettingsOverride = consumeSettingsOverride,
      requestMatchExtractor = requestMatchExtractor,
      responseMatchExtractor = responseMatchExtractor,
      replyExtractions = replyExtractions,
    )

  private def awaitActionResult(
      components: KafkaComponents,
      attributes: KafkaRequestReplyAttributes[String, String],
  ): Session = {
    val latch         = new CountDownLatch(1)
    @volatile var out = Option.empty[Session]

    val nextAction = new Action {
      override def name: String = "capture-next"

      override def !(session: Session): Unit =
        execute(session)

      override def execute(session: Session): Unit = {
        out = Some(session)
        latch.countDown()
      }
    }

    val instrumentedAction = new KafkaRequestReplyAction[String, String](
      components = components,
      attributes = attributes,
      statsEngine = NoOpStatsEngine,
      clock = clock,
      next = nextAction,
      throttler = None,
    )

    val session =
      Session("integration-scenario", 1L, scala.collection.immutable.Map.empty, OK, Nil, Session.NothingOnExit, null)

    instrumentedAction.sendRequest(session) match {
      case Success(_)       => ()
      case Failure(message) => fail(s"request-reply action failed before send: $message")
    }

    assert(latch.await(20, TimeUnit.SECONDS), "timed out waiting for request-reply action result")
    out.get
  }

  private def createTopics(topics: String*): Unit = {
    val admin = AdminClient.create(
      Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers).asJava,
    )
    try {
      admin.createTopics(topics.map(topic => new NewTopic(topic, 1, 1.toShort)).asJava).all().get(20, TimeUnit.SECONDS)
    } finally {
      admin.close(Duration.ofSeconds(5))
    }
  }

  private def withResponder(
      requestTopic: String,
      replyTopic: String,
  )(
      replyFactory: ConsumerRecord[Array[Byte], Array[Byte]] => ProducerRecord[Array[Byte], Array[Byte]],
  )(testCode: => Unit): Unit = {
    val running  = new AtomicBoolean(true)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](
      consumerProps("responder-group"),
      new ByteArrayDeserializer,
      new ByteArrayDeserializer,
    )
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps, new ByteArraySerializer, new ByteArraySerializer)

    consumer.subscribe(java.util.Collections.singletonList(requestTopic))

    val thread = new Thread(() => {
      try {
        while (running.get()) {
          val records = consumer.poll(Duration.ofMillis(200))
          records.iterator().asScala.foreach { record =>
            producer.send(replyFactory(record)).get(10, TimeUnit.SECONDS)
          }
        }
      } catch {
        case _: WakeupException if !running.get() => ()
      }
    })

    thread.setName(s"responder-$requestTopic")
    thread.setDaemon(true)
    thread.start()

    try {
      testCode
    } finally {
      running.set(false)
      consumer.wakeup()
      thread.join(5_000L)
      producer.close(Duration.ofSeconds(5))
      consumer.close(Duration.ofSeconds(5))
    }
  }

  private def producerProps: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    props
  }

  private def dockerSocketLikelyPresent: Boolean = {
    val homeSocket = Paths.get(sys.props("user.home"), ".docker", "run", "docker.sock")
    sys.env.contains("DOCKER_HOST") || Files.exists(Paths.get("/var/run/docker.sock")) || Files.exists(homeSocket)
  }

  private def consumerProps(groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"$groupId-${UUID.randomUUID()}")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props
  }

  private object NoOpAction extends Action {
    override def name: String                    = "noop"
    override def execute(session: Session): Unit = ()
  }

  private val NoOpStatsEngine: StatsEngine = new GatlingNoOpStatsEngine
}
