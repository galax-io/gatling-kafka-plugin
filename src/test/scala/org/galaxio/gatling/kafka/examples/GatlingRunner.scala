package org.galaxio.gatling.kafka.examples

import io.gatling.app.Gatling
import io.gatling.shared.cli.GatlingCliOptions

object GatlingRunner {

  def main(args: Array[String]): Unit = {

    // this is where you specify the class you want to run
    val simulationClass = classOf[BasicSimulation].getName

    Gatling.main(
      args ++
        Array(
          GatlingCliOptions.Simulation.shortOption,
          simulationClass,
          GatlingCliOptions.ResultsFolder.shortOption,
          "results",
          GatlingCliOptions.Launcher.shortOption,
          "sbt",
        ),
    )
  }

}
