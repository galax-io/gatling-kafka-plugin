package org.galaxio.gatling.kafka.examples

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

/** The set of simulations this project publishes as documentation, derived from the source tree rather than from a hand-written
  * list.
  *
  * The list it replaces was nine fully-qualified names maintained by hand in the gate, with nothing detecting an omission. That
  * is how four Kotlin examples came to have no coverage of any kind, and one of them stayed broken across four releases.
  * Deriving the set from disk means adding an example without covering it fails instead of passing quietly — contract C3 of
  * `specs/007-multilang-example-ci-coverage/contracts/example-coverage.md`.
  */
object ExampleInventory {

  sealed trait Language extends Product with Serializable
  object Language {
    case object Scala  extends Language
    case object Java   extends Language
    case object Kotlin extends Language
  }

  /** What CI does with an example. Every example has exactly one; there is no default (DR-5). */
  sealed trait Coverage extends Product with Serializable
  object Coverage {

    /** Run against a real broker, with assertions. */
    case object Executed extends Coverage

    /** Deliberately not run, with the reason on the record (FR-004). */
    final case class CompileOnly(reason: String) extends Coverage
  }

  final case class Example(
      language: Language,
      fqcn: String,
      source: Path,
      coverage: Coverage,
      topics: Set[String],
      unresolvedTopicCalls: Seq[String],
  ) {
    def simpleName: String = fqcn.substring(fqcn.lastIndexOf('.') + 1)
  }

  private final case class Root(language: Language, dir: String, pkg: String, extension: String)

  // One consumer project per language, each on the build tool its users actually use, each depending
  // on the published artifact. This inventory spans all three because "an example nobody covers" has
  // to be detectable wherever the example lives.
  private val roots = Seq(
    Root(
      Language.Scala,
      "examples/scala/src/test/scala/org/galaxio/examples/scalaapi",
      "org.galaxio.examples.scalaapi",
      ".scala",
    ),
    Root(Language.Java, "examples/java/src/test/java/org/galaxio/examples/javaapi", "org.galaxio.examples.javaapi", ".java"),
    Root(
      Language.Kotlin,
      "examples/kotlin/src/gatling/kotlin/org/galaxio/examples/kotlinapi",
      "org.galaxio.examples.kotlinapi",
      ".kt",
    ),
  )

  /** Sources that live in an example directory but are not examples. Closed and small on purpose: an addition here is a
    * deliberate statement that a file is not user-facing documentation, and a file wrongly left out of it fails the coverage
    * check rather than being run as an example.
    */
  // Keyed by (language, simple name), not by simple name alone: an exclusion meant for one language
  // must not silently drop a genuine example of another that happens to share the name. The list is
  // short now that the example projects hold nothing but examples — the test harnesses stayed behind
  // in the plugin's own src/test/scala, where this scan never looks.
  private val notExamples: Map[(Language, String), String] = Map(
    (Language.Scala: Language, "GatlingRunner") -> "IDE launcher for a single simulation; declares none of its own",
  )

  /** Coverage level per simple name and language. Every example is `Executed`, each by its own project's Gatling task:
    * `sbt Gatling/test` in examples/scala, `mvn verify` in examples/java, `./gradlew gatlingRun --all` in examples/kotlin.
    */
  private val coverage: Map[(Language, String), Coverage] = {
    val jvmExecuted = Seq(
      (Language.Scala, "Avro4sSimulation"),
      (Language.Scala, "AvroClassWithRequestReplySimulation"),
      (Language.Scala, "BasicSimulation"),
      (Language.Scala, "MatchSimulation"),
      (Language.Scala, "ProducerSimulation"),
      (Language.Java, "AvroClassWithRequestReplySimulation"),
      (Language.Java, "BasicSimulation"),
      (Language.Java, "MatchSimulation"),
      (Language.Java, "ProducerSimulation"),
    ).map(_ -> (Coverage.Executed: Coverage))

    val kotlinExecuted = Seq(
      "AvroClassWithRequestReplySimulation",
      "BasicSimulation",
      "MatchSimulation",
      "ProducerSimulation",
    ).map(n => (Language.Kotlin, n) -> (Coverage.Executed: Coverage))

    (jvmExecuted ++ kotlinExecuted).toMap
  }

  // Topics are read out of the source rather than declared here, so the disjointness check (C6) is
  // made against the topics an example actually uses. A declared copy would drift from the source and
  // then certify a collision it no longer describes.
  //
  // Both patterns anchor on the same call sites. The first captures a string literal; the second
  // matches the call whatever its argument is, so a topic passed as a constant or an expression is
  // *reported* rather than silently contributing nothing. A guard that returns an empty set looks
  // exactly like a guard that passed, which is worse than having no guard at all.
  private val topicLiteral = """\.(?:topic|requestTopic|replyTopic)\(\s*"([^"]+)"""".r
  private val topicCall    = """\.(?:topic|requestTopic|replyTopic)\(\s*([^)]*)""".r

  /** Strip line comments, but only outside string literals.
    *
    * Runs over the whole file rather than line by line, because the "inside a string" state has to survive a newline: every
    * language here has multi-line literals, and four of the example files use them (Java text blocks, Kotlin raw strings, Scala
    * `stripMargin`). A triple-quoted block is copied out whole for the same reason: its content is data, and the quotes inside
    * it are not delimiters, so a `//` in there must survive.
    */
  private val TripleQuote = "\"\"\""

  private def stripComments(text: String): String = {
    val out    = new StringBuilder
    var inStr  = false
    var escape = false
    var i      = 0
    while (i < text.length) {
      val c = text.charAt(i)
      if (escape) { out.append(c); escape = false; i += 1 }
      else if (!inStr && text.startsWith(TripleQuote, i)) {
        // A triple-quoted block — Java text block, Kotlin raw string, Scala stripMargin literal — is
        // copied out verbatim. Its content is data: `//` in there is ordinary text, and the quotes
        // inside it are not delimiters, so toggling on each one would leave the scanner out of step
        // with reality for the rest of the block.
        val end  = text.indexOf(TripleQuote, i + TripleQuote.length)
        val stop = if (end < 0) text.length else end + TripleQuote.length
        out.append(text, i, stop)
        i = stop
      } else if (c == '\\' && inStr) { out.append(c); escape = true; i += 1 }
      else if (c == '"') { out.append(c); inStr = !inStr; i += 1 }
      else if (!inStr && c == '/' && i + 1 < text.length && text.charAt(i + 1) == '/') {
        // Skip to the newline, which is kept so line structure survives.
        while (i < text.length && text.charAt(i) != '\n') i += 1
      } else { out.append(c); i += 1 }
    }
    out.toString
  }

  private def sourceText(source: Path): String =
    stripComments(new String(Files.readAllBytes(source), "UTF-8"))

  private def topicsIn(text: String): Set[String] = topicLiteral.findAllMatchIn(text).map(_.group(1)).toSet

  // A whole argument that is one literal, and nothing else. `startsWith("\"")` is not enough: for
  // `.topic("prefix-" + suffix)` it says "resolved" while `topicLiteral` captures the truncated
  // `prefix-` and reports a topic no message is ever sent to — the reader would then be wrong in two
  // directions at once, and silent about both.
  private val wholeLiteral = """^"[^"]*"$""".r

  /** Topic calls whose argument is not a single string literal — the ones `topicsIn` cannot read correctly. */
  private def unresolvedTopicsIn(text: String): Seq[String] =
    topicCall
      .findAllMatchIn(text)
      .map(_.group(1).trim)
      .filterNot(arg => wholeLiteral.findFirstIn(arg).isDefined)
      .toSeq

  /** Every published example, derived from the source tree. Throws if any example is unaccounted for. */
  lazy val all: Seq[Example] = {
    val found = roots.flatMap { root =>
      val dir     = Paths.get(root.dir)
      if (!Files.isDirectory(dir)) sys.error(s"example directory ${root.dir} does not exist")
      // Files.list holds an open directory handle until the stream is closed, so it has to be closed
      // explicitly — the iterator conversion does not do it.
      val listing = Files.list(dir)
      try
        listing
          .iterator()
          .asScala
          .filter(p => p.getFileName.toString.endsWith(root.extension))
          .map { p =>
            val simpleName = p.getFileName.toString.stripSuffix(root.extension)
            (root, simpleName, p)
          }
          .toSeq
          .sortBy(_._2)
      finally listing.close()
    }

    val examples = found.collect {
      case (root, simpleName, path) if !notExamples.contains((root.language, simpleName)) =>
        val level = coverage.getOrElse(
          (root.language, simpleName),
          sys.error(
            s"$path is an example with no coverage level. Give it one in ExampleInventory.coverage, " +
              s"or record it in notExamples with the reason it is not user-facing documentation.",
          ),
        )
        val text  = sourceText(path)
        Example(root.language, s"${root.pkg}.$simpleName", path, level, topicsIn(text), unresolvedTopicsIn(text))
    }

    val declared = coverage.keySet
    val present  = examples.map(e => (e.language, e.simpleName)).toSet
    val missing  = declared.diff(present)
    if (missing.nonEmpty)
      sys.error(s"coverage declared for examples that no longer exist: ${missing.toSeq.sortBy(_._2).mkString(", ")}")

    examples
  }

  def executed: Seq[Example] = all.filter(_.coverage == Coverage.Executed)

  /** The consumer projects the examples live in, one per language. */
  def projects: Seq[String] = Seq("examples/scala", "examples/java", "examples/kotlin")

  /** Two covered examples sharing a topic would let one example's leftover record satisfy the next one's reply — and
    * `MatchSimulation` matches by a constant, so it accepts anything. That failure would be attributed to the plugin's
    * correlation logic, which is the wrong diagnosis (C6).
    */
  def topicCollisions: Seq[String] =
    executed
      .flatMap(e => e.topics.map(_ -> e.fqcn))
      .groupBy(_._1)
      .collect { case (topic, users) if users.size > 1 => s"$topic is used by ${users.map(_._2).sorted.mkString(", ")}" }
      .toSeq
      .sorted

  /** A topic named by anything other than a string literal is one [[topicCollisions]] cannot see, so it is reported rather than
    * quietly contributing nothing to the disjointness check.
    */
  def unresolvedTopics: Seq[String] =
    executed
      .flatMap(e => e.unresolvedTopicCalls.map(arg => s"${e.fqcn} names a topic non-literally ($arg); C6 cannot check it"))
      .sorted

  // The two broker definitions are maintained separately by design — CI builds its broker from its own
  // service block, not from the Compose file — so a topic added to one and not the other is invisible
  // until a run depends on auto-create. That has already happened once on this feature.
  //
  // Each carries the shape of an actual creation entry, not a bare substring. `text.contains(topic)`
  // would pass `ex.java.basic` purely because `ex.java.basic.t` exists elsewhere in the file, so a
  // topic that is a prefix of another would be certified as created while nothing creates it — a false
  // negative in exactly the drift this check exists to catch.
  private val brokerDefinitions = Seq(
    (".github/workflows/ci.yml", "KAFKA_CREATE_TOPICS in the CI broker service", (t: String) => s"$t:1:1"),
    ("docker-compose.kafka.yml", "the topic-init service", (t: String) => s"--topic $t "),
  )

  /** Every topic a covered example uses must be created by both broker definitions (C6). */
  def missingBrokerTopics: Seq[String] = {
    val topics = executed.flatMap(_.topics).distinct.sorted
    brokerDefinitions.flatMap { case (file, where, entry) =>
      val path = Paths.get(file)
      if (!Files.isRegularFile(path)) Seq(s"$file does not exist, so its topic list cannot be checked")
      else {
        val text = new String(Files.readAllBytes(path), "UTF-8")
        topics
          .filterNot(topic => text.contains(entry(topic)))
          .map(topic => s"$topic is used by an example but not created by $where ($file)")
      }
    }
  }

  /** Every guarantee C6 makes, in one call so a caller cannot enforce half of it. */
  def topicProblems: Seq[String] = topicCollisions ++ unresolvedTopics ++ missingBrokerTopics

  // Each entry says how that build tool spells the dependency, so the check reads the coordinate and
  // not merely the file. Every one of these files also names the sentinel in a comment, and a bare
  // `contains` is satisfied by the comment alone — the same substring-for-token defect the topic check
  // above had, which is why both now match a shape.
  private val sentinelVersion = "0.0.0-EXAMPLES-SNAPSHOT"

  private val sentinelCoordinates = Seq(
    (".github/workflows/ci.yml", s"""version := "$sentinelVersion""""),
    ("examples/scala/build.sbt", s"""% "$sentinelVersion""""),
    ("examples/java/pom.xml", s"<gatling-kafka-plugin.version>$sentinelVersion</gatling-kafka-plugin.version>"),
    ("examples/kotlin/build.gradle.kts", s"gatling-kafka-plugin_2.13:$sentinelVersion"),
  )

  /** The version each example project consumes must be the one CI publishes. */
  def sentinelProblems: Seq[String] =
    sentinelCoordinates.flatMap { case (file, coordinate) =>
      val path = Paths.get(file)
      if (!Files.isRegularFile(path)) Seq(s"$file does not exist, so the example version cannot be checked")
      else if (new String(Files.readAllBytes(path), "UTF-8").contains(coordinate)) Nil
      else Seq(s"$file does not carry `$coordinate`; the example projects and the publish step have drifted")
    }

  // project/Dependencies.scala is this repo's declared dependency truth. Each example project restates
  // these versions for its own build tool, so a bump applied to the file the author happens to have open
  // would leave the others pinned to the old one — and CI would stay green, because each project resolves
  // independently. Nothing can share the literal across sbt, Maven and Gradle, so compare instead.
  private val declaredVersion = """val (\w+)\s*=\s*"([^"]+)"""".r

  // Which of those each project is expected to pin, and why the gaps are gaps rather than omissions:
  // examples/scala takes avro transitively through avro4s and the Confluent serde, and examples/kotlin
  // takes Gatling from the `io.gatling.gradle` plugin rather than a dependency of its own.
  private val expectedPins = Seq(
    ("examples/scala/build.sbt", Set("kafka", "gatling", "kafkaAvroSerde")),
    ("examples/java/pom.xml", Set("kafka", "gatling", "kafkaAvroSerde", "avro")),
    ("examples/kotlin/build.gradle.kts", Set("kafka", "kafkaAvroSerde", "avro")),
  )

  // A version has to appear as a whole token. `contains("3.13.5")` is satisfied by the Gradle plugin id
  // `3.13.5.4`, which pins nothing — the check would pass on a coincidence of someone else's versioning
  // scheme rather than on a pin this project made.
  private def pinsVersion(text: String, version: String): Boolean =
    s"(?<![\\d.])${java.util.regex.Pattern.quote(version)}(?![\\d.])".r.findFirstIn(text).isDefined

  /** Every example project must pin the versions `project/Dependencies.scala` declares for it. */
  def dependencyPinProblems: Seq[String] = {
    val truth = Paths.get("project/Dependencies.scala")
    if (!Files.isRegularFile(truth)) Seq("project/Dependencies.scala does not exist, so pins cannot be checked")
    else {
      val declared = declaredVersion
        .findAllMatchIn(new String(Files.readAllBytes(truth), "UTF-8"))
        .map(m => m.group(1) -> m.group(2))
        .toMap
      expectedPins.flatMap { case (file, names) =>
        val path = Paths.get(file)
        if (!Files.isRegularFile(path)) Seq(s"$file does not exist, so its pins cannot be checked")
        else {
          val text = new String(Files.readAllBytes(path), "UTF-8")
          names.toSeq.sorted.flatMap { name =>
            declared.get(name) match {
              case None                                         =>
                Seq(s"project/Dependencies.scala no longer declares `$name`, which $file is checked against")
              case Some(version) if !pinsVersion(text, version) =>
                Seq(s"$file does not pin $name $version, which project/Dependencies.scala declares")
              case _                                            => Nil
            }
          }
        }
      }
    }
  }

  // A deliberate copy, not a reference. `scalafmtConfig := ../../.scalafmt.conf` would dedupe it, but
  // README promises "Nothing in them is specific to this repository, so you can copy one and start from
  // it" — and a path pointing two levels out of the project breaks the moment someone takes it at its
  // word. The duplication stays; what does not stay is it being silent.
  private val formatPolicy = ".scalafmt.conf"

  /** The example project's formatting policy must be the repo's, byte for byte. */
  def formatPolicyProblems: Seq[String] = {
    val root = Paths.get(formatPolicy)
    val copy = Paths.get("examples/scala", formatPolicy)
    if (!Files.isRegularFile(root)) Seq(s"$formatPolicy does not exist, so the example copy cannot be checked")
    else if (!Files.isRegularFile(copy)) Seq(s"$copy does not exist; examples/scala has no formatting policy")
    else if (Files.readAllBytes(root).sameElements(Files.readAllBytes(copy))) Nil
    else Seq(s"$copy has drifted from $formatPolicy; the two builds would format the same code differently")
  }

  /** Everything checkable about the example projects without a broker. */
  def problems: Seq[String] = topicProblems ++ sentinelProblems ++ dependencyPinProblems ++ formatPolicyProblems
}
