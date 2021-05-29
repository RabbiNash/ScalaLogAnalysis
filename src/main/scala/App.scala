import jobs.AnalysisJob
import org.apache.spark.sql.SparkSession
import little.cli.Cli.{ application, option }
import little.cli.Implicits._

object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Log Analysis")
      .getOrCreate()

    import spark.implicits._

    startAnalysisJob(args, spark)

    spark.stop
  }

  private def startAnalysisJob(args: Array[String], sparkSession: SparkSession): Unit = {
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

      AnalysisJob.startJob(sparkSession, sourcePath.toString, reportPath.toString)
    }
  }
}