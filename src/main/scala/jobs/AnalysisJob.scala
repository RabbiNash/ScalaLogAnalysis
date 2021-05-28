package jobs

import mappers.AccessLogMapper.toAccessLog
import models.{ AccessLog, IpAddressCount, UriCount }
import org.apache.spark.sql.functions.{ col, collect_list, map_from_arrays, to_timestamp }
import org.apache.spark.sql.{ Dataset, Encoder, Encoders, SparkSession }
import utils.Utils
import utils.Utils.{ AccessLogView, SepRegex }

object AnalysisJob {

  var spark: SparkSession = _

  def startJob(sparkSession: SparkSession) {
    spark = sparkSession

    createReport(Utils.AccessLogPath)
  }

  private def createReport(gzPath: String) {
    val logs = spark.read.text(gzPath)
    val stringLogs = logs.map(_.getString(0))(Encoders.STRING)

    val parsedLogs: Dataset[List[String]] = stringLogs.flatMap(x => SepRegex.unapplySeq(x))(Encoders.product[List[String]])
    val mappedLogs: Dataset[AccessLog] = parsedLogs.map(toAccessLog)(Encoders.product[AccessLog])

    val mappedLogsWithTime = mappedLogs.withColumn("datetime", to_timestamp(mappedLogs("datetime"), "dd/MMM/yyyy:HH:mm:ss X"))

    mappedLogsWithTime.createOrReplaceTempView(AccessLogView)

    val ipCountDateGrouped = spark.sql("select cast(datetime as date) as date, ip, count(*) as count from AccessLog group by date,ip having count > 20000 order by count desc")
      .as[IpAddressCount](Encoders.product[IpAddressCount])

    retrieveIpReport(ipCountDateGrouped, Utils.OutputIpJsonPath)

    //retrieve URIs resources
    val uriCountDateGrouped = spark.sql("select cast(datetime as date) as date, request, count(*) as count from AccessLog group by date,request having count > 20000 order by count desc")
      .as[UriCount](Encoders.product[UriCount])

    retrieveUrisReport(uriCountDateGrouped, Utils.OutputUriJsonPath)
  }

  def retrieveUrisReport(dataset: Dataset[UriCount], outputPath: String): Unit = {
    baseReportExtractor[UriCount](dataset, "request", "requests", "requestCount", outputPath)
  }

  def retrieveIpReport(dataset: Dataset[IpAddressCount], outputPath: String): Unit = {
    baseReportExtractor[IpAddressCount](dataset, "ip", "ips", "ipCount", outputPath)
  }

  def baseReportExtractor[T](dataset: Dataset[T], colName: String, alias: String, updatedColName: String, outputPath: String): Unit = {
    dataset.groupBy("date").agg(collect_list(col(colName))
      .as(alias), collect_list("count")
      .as("counts")).select(col("date"), map_from_arrays(col(alias), col("counts")))
      .withColumnRenamed(s"map_from_arrays(${alias}, counts)", updatedColName)
      .coalesce(1).write.format("json").save(outputPath)
  }
}
