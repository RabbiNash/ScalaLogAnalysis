/*
* Author: Tinashe Makuti
* */

import org.apache.spark.sql.SparkSession
import little.cli.Cli.{application, option}
import little.cli.Implicits._
import tasks.ExtractionTask

object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Tinashe Log Analysis")
      .getOrCreate()

    import spark.implicits._

    startExtractionJob(args, spark)

    spark.stop
  }

  private def startExtractionJob(args: Array[String], sparkSession: SparkSession): Unit = {

    //use little CLI
    val app = application(
      "grep [ options ... ] <pattern> [ <fileName> ... ]",
      option("s", "source-path", hasArg = true, "Source gz compressed file path").argName("source_path"),
      option("r", "report-path", hasArg = true, "Path to save the generated reports").argName("report_path"),
      option("h", "help", hasArg = false, description = "Get help"))

    // Parse arguments
    val cmd = app.parse(args)

    if (cmd.hasOption("h")) {
      app.printHelp()
    } else {
      val sourcePath = cmd.getParsedOptionValue("s")
      val reportPath = cmd.getParsedOptionValue("r")

      if (sourcePath == null || reportPath == null) {
        println("Both source path and report path are mandatory")
        app.printHelp()
        return
      }

      ExtractionTask.initJob(sparkSession, sourcePath.toString, reportPath.toString)
    }
  }
}