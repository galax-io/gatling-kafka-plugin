package org.galaxio.gatling.kafka.examples

import io.gatling.core.scenario.Simulation

object ExampleSmokeValidation {

  private val scalaSimulationClass = classOf[Simulation]
  private val javaSimulationClass  = classOf[io.gatling.javaapi.core.Simulation]

  private val exampleSimulationClasses = Seq(
    "org.galaxio.gatling.kafka.examples.Avro4sSimulation",
    "org.galaxio.gatling.kafka.examples.AvroClassWithRequestReplySimulation",
    "org.galaxio.gatling.kafka.examples.BasicSimulation",
    "org.galaxio.gatling.kafka.examples.MatchSimulation",
    "org.galaxio.gatling.kafka.examples.ProducerSimulation",
    "org.galaxio.gatling.kafka.javaapi.examples.AvroClassWithRequestReplySimulation",
    "org.galaxio.gatling.kafka.javaapi.examples.BasicSimulation",
    "org.galaxio.gatling.kafka.javaapi.examples.MatchSimulation",
    "org.galaxio.gatling.kafka.javaapi.examples.ProducerSimulation",
  )

  def main(args: Array[String]): Unit = {
    exampleSimulationClasses.foreach { className =>
      val clazz = Class.forName(className)
      require(
        scalaSimulationClass.isAssignableFrom(clazz) || javaSimulationClass.isAssignableFrom(clazz),
        s"$className does not extend ${scalaSimulationClass.getName} or ${javaSimulationClass.getName}",
      )
      clazz.getDeclaredConstructor()
      println(s"Validated example simulation class: $className")
    }
  }
}
