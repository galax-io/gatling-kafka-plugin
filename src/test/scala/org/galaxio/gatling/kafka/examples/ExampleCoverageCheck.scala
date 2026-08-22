package org.galaxio.gatling.kafka.examples

/** Checks the contract the three example projects have to keep, without needing a broker or a network.
  *
  * It asserts two things, both of which used to be asserted by nothing:
  *
  *   - every example simulation on disk has a recorded coverage level, so adding one without covering it fails here instead of
  *     passing quietly (contract C3). That is how four Kotlin examples came to have no coverage of any kind, and one of them
  *     stayed broken across four releases;
  *   - no two examples share a topic, every topic they use is created by both broker definitions, and no example names a topic
  *     in a way this check cannot read (contract C6). A clash has to be caught before the simulations run — afterwards it
  *     surfaces as an intermittent reply mismatch that looks like a plugin correlation bug;
  *   - each project consumes the version CI publishes and pins the versions `project/Dependencies.scala` declares for it
  *     (contract C8). Nothing can share a version literal across sbt, Maven and Gradle, so the duplication is unavoidable —
  *     what this stops is the duplication being silent.
  *
  * It does not construct or run anything. The examples live in `examples/{scala,java,kotlin}`, each a consumer project on the
  * build tool its users use, and each compiles and runs them itself — running necessarily constructs. This was previously
  * called `ExampleSmokeValidation` and did try to construct them, back when the Scala examples were on this build's classpath.
  */
object ExampleCoverageCheck {

  def main(args: Array[String]): Unit = {
    val examples = ExampleInventory.all

    examples.groupBy(_.language).toSeq.sortBy(_._1.toString).foreach { case (language, found) =>
      println(s"$language: ${found.size} example(s) — ${found.map(_.simpleName).sorted.mkString(", ")}")
    }

    val problems = ExampleInventory.problems
    problems.foreach(p => println(s"  $p"))

    println(
      s"\n${examples.size} example simulation(s) across ${ExampleInventory.projects.size} project(s); " +
        s"${examples.flatMap(_.topics).distinct.size} topic(s) checked",
    )

    if (problems.nonEmpty) sys.error(s"${problems.size} example project problem(s); see contracts C3, C6 and C8")
  }
}
