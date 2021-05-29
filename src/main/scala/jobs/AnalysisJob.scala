package jobs

import mappers.AccessLogMapper.toAccessLog
import models.{ AccessLog, IpAddressCount, UriCount }
import org.apache.spark.sql.functions.{ col, collect_list, map_from_arrays, to_timestamp }
import org.apache.spark.sql.{ Dataset, Encoders, SparkSession }
import utils.{ CustomFileUtils, Utils }
import utils.Utils.{ AccessLogView, SepRegex }

import java.nio.file.{ Path, Paths }

object AnalysisJob {

  var spark: SparkSession = _

  def startJob(sparkSession: SparkSession, sourcePath: String, reportPath: String) {
    spark = sparkSession

    createReport(sourcePath, reportPath)
    cleanUp()
  }

  private def createReport(gzPath: String, reportPath: String) {

    extractFromGz(Paths.get(gzPath), Paths.get(Utils.AccessLogPath))

    val logs = spark.read.text(gzPath)
    val stringLogs = logs.map(_.getString(0))(Encoders.STRING)

    val parsedLogs: Dataset[List[String]] = stringLogs.flatMap(x => SepRegex.unapplySeq(x))(Encoders.product[List[String]])
    val mappedLogs: Dataset[AccessLog] = parsedLogs.map(toAccessLog)(Encoders.product[AccessLog])

    val mappedLogsWithTime = mappedLogs.withColumn("datetime", to_timestamp(mappedLogs("datetime"), "dd/MMM/yyyy:HH:mm:ss X"))

    mappedLogsWithTime.createOrReplaceTempView(AccessLogView)

    val ipCountDateGrouped = spark.sql("select cast(datetime as date) as date, ip, count(*) as count from AccessLog group by date,ip having count > 20000 order by count desc")
      .as[IpAddressCount](Encoders.product[IpAddressCount])

    retrieveIpReport(ipCountDateGrouped, s"${reportPath}/report_ips.json")

    //retrieve URIs resources
    val uriCountDateGrouped = spark.sql("select cast(datetime as date) as date, request, count(*) as count from AccessLog group by date,request having count > 20000 order by count desc")
      .as[UriCount](Encoders.product[UriCount])

    retrieveUrisReport(uriCountDateGrouped, s"${reportPath}/report_uris.json")
  }

  private def extractFromGz(sourceFile: Path, destFile: Path): Unit = {
    CustomFileUtils.decompressGzipNio(sourceFile, destFile)
  }

  def retrieveUrisReport(dataset: Dataset[UriCount], outputPath: String): Unit = {
    baseReportExtractor[UriCount](dataset, "request", "requests", "requestCount", outputPath)
  }

  def retrieveIpReport(dataset: Dataset[IpAddressCount], outputPath: String): Unit = {
    baseReportExtractor[IpAddressCount](dataset, "ip", "ips", "ipCount", outputPath)
  }

  def baseReportExtractor[T](dataset: Dataset[T], colName: String, alias: String, updatedColName: String, outputPath: String): Unit = {
    CustomFileUtils.deleteIfExists(Paths.get(outputPath))
    dataset.groupBy("date").agg(collect_list(col(colName))
      .as(alias), collect_list("count")
      .as("counts")).select(col("date"), map_from_arrays(col(alias), col("counts")))
      .withColumnRenamed(s"map_from_arrays(${alias}, counts)", updatedColName)
      .coalesce(1).write.format("json").save(outputPath)
  }

  def cleanUp(): Unit = {
    CustomFileUtils.deleteFile(Paths.get(Utils.AccessLogPath))
  }
}
