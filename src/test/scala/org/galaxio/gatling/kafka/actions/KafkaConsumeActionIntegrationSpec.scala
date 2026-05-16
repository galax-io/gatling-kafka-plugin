package org.galaxio.gatling.kafka.actions

import akka.actor.ActorRef
import akka.actor.ActorSystem
import io.gatling.commons.stats.{OK, Status}
import io.gatling.commons.util.DefaultClock
import io.gatling.commons.validation.{Failure, Success}
import io.gatling.core.Predef._
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.galaxio.gatling.kafka.Predef._
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.galaxio.gatling.kafka.client.KafkaTrackersPoolFactory
import org.galaxio.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol, KafkaProtocolBuilderNew}
import org.galaxio.gatling.kafka.request.KafkaProtocolMessage
import org.galaxio.gatling.kafka.request.builder.{KafkaConsumeAttributes, KafkaReplyExtraction}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.DockerClientFactory
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.Try

class KafkaConsumeActionIntegrationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private val externalBootstrap      = sys.env
    .get("GATLING_KAFKA_TEST_BOOTSTRAP")
    .orElse(sys.props.get("gatling.kafka.test.bootstrap"))
  private val kafkaContainer         = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"))
  private val actorSystem            = ActorSystem("kafka-consume-integration")
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
    actorSystem.terminate()
    if (externalBootstrap.isEmpty && dockerAvailable) {
      kafkaContainer.stop()
    }
    super.afterAll()
  }

  test("consume-only action tracks payload and stores consumed reply into session") {
    assume(
      canRunIntegration,
      "Integration tests require either GATLING_KAFKA_TEST_BOOTSTRAP or a Docker environment for Testcontainers",
    )

    val topic = s"consume-topic-${UUID.randomUUID()}"
    createTopics(topic)

    val attributes = KafkaConsumeAttributes(
      requestName = _ => Success("consume-integration"),
      topic = _ => Success(topic),
      expectedMatchId = _ => Success("tracked-payload".getBytes()),
      checks = List(bodyBytes.is("tracked-payload".getBytes())),
      silent = Some(false),
      consumeSettingsOverride = Some(
        Map(
          "bootstrap.servers" -> bootstrapServers,
          "auto.offset.reset" -> "earliest",
        ),
      ),
      responseMatchExtractor = Some(_.value),
      replyExtractions = List(
        KafkaReplyExtraction("consumedPayload", message => new String(message.value)),
        KafkaReplyExtraction("consumedKey", message => Option(message.key).map(new String(_)).orNull),
      ),
    )

    val resultSession = awaitConsumeResult(invalidBaseProtocol, attributes) {
      publish(topic, key = "event-key".getBytes(), value = "tracked-payload".getBytes())
    }

    assert(!resultSession.isFailed)
    assert(resultSession("consumedPayload").as[String] == "tracked-payload")
    assert(resultSession("consumedKey").as[String] == "event-key")
  }

  test("consume-only action can correlate by header without sending a Kafka request first") {
    assume(
      canRunIntegration,
      "Integration tests require either GATLING_KAFKA_TEST_BOOTSTRAP or a Docker environment for Testcontainers",
    )

    val topic = s"consume-headers-${UUID.randomUUID()}"
    createTopics(topic)

    val attributes = KafkaConsumeAttributes(
      requestName = _ => Success("consume-header-integration"),
      topic = _ => Success(topic),
      expectedMatchId = _ => Success("corr-123".getBytes()),
      checks = Nil,
      silent = Some(false),
      consumeSettingsOverride = Some(
        Map(
          "bootstrap.servers" -> bootstrapServers,
          "auto.offset.reset" -> "earliest",
        ),
      ),
      responseMatchExtractor = Some { message =>
        message.headers
          .flatMap(headers => Option(headers.lastHeader("correlation-id")))
          .map(_.value())
          .orNull
      },
      replyExtractions = List(KafkaReplyExtraction("headerPayload", msg => new String(msg.value))),
    )

    val resultSession = awaitConsumeResult(invalidBaseProtocol, attributes) {
      publish(
        topic,
        key = "event-key".getBytes(),
        value = "header-payload".getBytes(),
        headers = Map("correlation-id" -> "corr-123".getBytes()),
      )
    }

    assert(!resultSession.isFailed)
    assert(resultSession("headerPayload").as[String] == "header-payload")
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

  private def awaitConsumeResult(
      protocol: KafkaProtocol,
      attributes: KafkaConsumeAttributes,
  )(
      publishCode: => Unit,
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

    val action = new KafkaConsumeAction(
      components = KafkaComponents(
        coreComponents = null,
        kafkaProtocol = protocol,
        trackersPoolFactory = new KafkaTrackersPoolFactory(actorSystem, NoOpStatsEngine, clock),
        senderProvider = null,
      ),
      attributes = attributes,
      statsEngine = NoOpStatsEngine,
      clock = clock,
      next = nextAction,
      throttler = None,
    )

    val session =
      Session("consume-integration", 1L, scala.collection.immutable.Map.empty, OK, Nil, Session.NothingOnExit, null)

    action.sendRequest(session) match {
      case Success(_)       =>
        publishCode
      case Failure(message) => fail(s"consume action failed before tracking: $message")
    }

    assert(latch.await(20, TimeUnit.SECONDS), "timed out waiting for consume action result")
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

  private def publish(
      topic: String,
      key: Array[Byte],
      value: Array[Byte],
      headers: Map[String, Array[Byte]] = Map.empty,
  ): Unit = {
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps, new ByteArraySerializer, new ByteArraySerializer)
    try {
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
      headers.foreach { case (headerKey, headerValue) =>
        record.headers().add(headerKey, headerValue)
      }
      producer.send(record).get(10, TimeUnit.SECONDS)
    } finally {
      producer.close(Duration.ofSeconds(5))
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

  private object NoOpStatsEngine extends StatsEngine {
    override def start(): Unit                                                                                            = ()
    override def stop(controller: ActorRef, exception: Option[Exception]): Unit                                           = ()
    override def logUserStart(scenario: String): Unit                                                                     = ()
    override def logUserEnd(scenario: String): Unit                                                                       = ()
    override def logResponse(
        scenario: String,
        groups: List[String],
        requestName: String,
        startTimestamp: Long,
        endTimestamp: Long,
        status: Status,
        responseCode: Option[String],
        message: Option[String],
    ): Unit                                                                                                               = ()
    override def logGroupEnd(scenario: String, groupBlock: io.gatling.core.session.GroupBlock, exitTimestamp: Long): Unit = ()
    override def logRequestCrash(
        scenario: String,
        groups: List[String],
        requestName: String,
        error: String,
    ): Unit                                                                                                               = ()
  }
}
