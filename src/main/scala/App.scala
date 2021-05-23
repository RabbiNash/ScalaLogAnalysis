import jobs.AnalysisJob
import org.apache.spark.sql.SparkSession

object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Log Analysis")
      .getOrCreate()

    import spark.implicits._

    AnalysisJob.startJob(spark)

    spark.stop
  }
}