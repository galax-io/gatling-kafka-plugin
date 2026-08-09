package org.galaxio.gatling.kafka.classpath

/** Contract E1: the default DSL entry points must be usable with only the inherited dependency set.
  *
  * The Confluent Avro artifacts are optional and not inherited by consumers, but `Predef` is imported by every simulation —
  * plain ones included. If anything reachable from an entry point *constructs* a Confluent type while that entry point
  * initialises, then a consumer who never touches Avro still fails, and moving the artifacts out of the inherited set only
  * trades a resolution-time error for a `NoClassDefFoundError` in the middle of a load test.
  *
  * This suite loads each entry point through a classloader that refuses `io.confluent.*` and forces static initialisation. It
  * is deliberately a *unit* check: the end-to-end proof — plain produce and request-reply simulations running with the
  * artifacts genuinely absent — is the consumer-resolution work still open as T012/T019/T020 in
  * `specs/005-classpath-dependency-shedding/tasks.md`. Until that exists, this suite is the only enforcement of E1, which is
  * why it carries the positive control below rather than trusting its own mechanism.
  */
final class PlainClasspathIsolationSpec extends munit.FunSuite {

  private val ConfluentAvroSerde = "io.confluent.kafka.streams.serdes.avro.GenericAvroSerde"
  private val PluginPrefix       = "org.galaxio.gatling.kafka."

  /** Refuses every `io.confluent.*` class, and loads plugin classes itself so that *their* references resolve through this
    * loader rather than through the parent. Everything else — Gatling, the Kafka client, the Scala library — is delegated, so
    * the only variable under test is Confluent's presence.
    */
  private final class ConfluentDenyingClassLoader(parent: ClassLoader) extends ClassLoader(parent) {

    private def defineFromParentResource(name: String): Option[Class[_]] = {
      val resource = name.replace('.', '/') + ".class"
      Option(parent.getResourceAsStream(resource)).map { in =>
        try {
          val bytes = in.readAllBytes()
          defineClass(name, bytes, 0, bytes.length)
        } finally in.close()
      }
    }

    override protected def loadClass(name: String, resolve: Boolean): Class[_] = synchronized {
      if (name.startsWith("io.confluent."))
        throw new ClassNotFoundException(s"denied by $getClass: $name")

      val own: Option[Class[_]] =
        if (name.startsWith(PluginPrefix)) Option(findLoadedClass(name)).orElse(defineFromParentResource(name))
        else None

      own match {
        case Some(loaded) =>
          if (resolve) resolveClass(loaded)
          loaded
        case None         =>
          super.loadClass(name, resolve)
      }
    }
  }

  private def denyingLoader: ClassLoader =
    new ConfluentDenyingClassLoader(getClass.getClassLoader)

  /** Loads and initialises `className`, and asserts the denying loader actually *defined* it.
    *
    * Without that second assertion the suite is worthless: `loadClass` falls back to `super.loadClass` whenever the class bytes
    * are unreadable as a resource, which hands back the parent's copy — whose references resolve against the parent, where
    * Confluent is present. Every assertion below would then pass with zero isolation.
    */
  private def initialiseUnderDenyingLoader(className: String, loader: ClassLoader): Class[_] = {
    val loaded = Class.forName(className, true, loader)
    assertEquals(
      loaded.getClassLoader,
      loader,
      s"$className was loaded by the parent, so nothing in this test was actually isolated",
    )
    loaded
  }

  // ---------------------------------------------------------------------------------------------
  // Guards. Without these the suite could report success for the wrong reason.
  // ---------------------------------------------------------------------------------------------

  test("guard: the Confluent Avro serde IS on the test classpath") {
    val loaded = Class.forName(ConfluentAvroSerde, false, getClass.getClassLoader)
    assertEquals(
      loaded.getName,
      ConfluentAvroSerde,
      "this suite is only meaningful when the artifact it hides is otherwise present",
    )
  }

  test("guard: the denying classloader refuses a direct load") {
    intercept[ClassNotFoundException] {
      Class.forName(ConfluentAvroSerde, false, denyingLoader)
    }: Unit
  }

  test("positive control: a plugin class that DOES construct a Confluent type fails under the denying loader") {
    // The load-bearing guard. It proves the deny-list reaches references made *from child-defined plugin classes*, which is the
    // mechanism every assertion below depends on. Initialising ConfluentSerdes$ alone is not enough — its <clinit> only assigns
    // MODULE$; the construction lives in newAvroSerde(), so the call has to be forced.
    val loader = denyingLoader
    val cls    = initialiseUnderDenyingLoader("org.galaxio.gatling.kafka.request.ConfluentSerdes$", loader)
    val module = cls.getField("MODULE$").get(null)
    val thrown = intercept[java.lang.reflect.InvocationTargetException] {
      cls.getMethod("newAvroSerde").invoke(module): Unit
    }
    assert(
      thrown.getCause.isInstanceOf[NoClassDefFoundError],
      s"expected NoClassDefFoundError from the denied Confluent construction, got ${thrown.getCause}",
    )
  }

  // ---------------------------------------------------------------------------------------------
  // Contract E1 — every entry point a simulation reaches.
  // ---------------------------------------------------------------------------------------------

  test("the Scala DSL entry point initialises with no Confluent artifact present") {
    initialiseUnderDenyingLoader("org.galaxio.gatling.kafka.Predef$", denyingLoader): Unit
  }

  test("the Java facade's DSL entry point initialises with no Confluent artifact present") {
    // Named in every Java and Kotlin simulation as `import static ...KafkaDsl.*`, and it holds SchemaRegistryClient in three
    // public signatures. Safe only as long as it has no Confluent-touching static state; this is what enforces that.
    initialiseUnderDenyingLoader("org.galaxio.gatling.kafka.javaapi.KafkaDsl", denyingLoader): Unit
  }

  test("the Java facade's checks object initialises with no Confluent artifact present") {
    initialiseUnderDenyingLoader("org.galaxio.gatling.kafka.javaapi.checks.KafkaChecks$", denyingLoader): Unit
  }

  test("the Java facade's Avro expression builder initialises with no Confluent artifact present") {
    initialiseUnderDenyingLoader(
      "org.galaxio.gatling.kafka.javaapi.request.expressions.Builders$AvroExpressionBuilder",
      denyingLoader,
    ): Unit
  }

  // ---------------------------------------------------------------------------------------------
  // The mechanism that lets `avroSerde` stay a strict val (published trait ABI) without Confluent on
  // the plain path. If this regressed, E1 would hold only by making avroSerde lazy — which silently
  // returns null for consumers compiled against an earlier release.
  // ---------------------------------------------------------------------------------------------

  test("LazyGenericAvroSerde constructs and closes without touching Confluent, and fails only on use") {
    val loader = denyingLoader
    val cls    = initialiseUnderDenyingLoader("org.galaxio.gatling.kafka.request.LazyGenericAvroSerde", loader)

    val serde = cls.getDeclaredConstructor().newInstance()
    cls.getMethod("close").invoke(serde): Unit

    val thrown = intercept[java.lang.reflect.InvocationTargetException] {
      cls.getMethod("serializer").invoke(serde): Unit
    }
    assert(
      thrown.getCause.isInstanceOf[NoClassDefFoundError],
      s"expected the delegate construction to be deferred to first use, got ${thrown.getCause}",
    )
  }
}
