package tasks

import mappers.LogMapper.toAccessLog
import models.{AccessLog, IpAddressCount, UriCount}
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.{Dataset, Encoders}
import utils.{LogUtils, BaseUtils}
import utils.BaseUtils.{AccessLogView, SepRegex, ipQuery, uriQuery}
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

object ExtractionTask {

  var sparkSession: SparkSession = _

  def initJob(sparkSession: SparkSession, srcPath: String, repPath: String): Unit = {
    this.sparkSession = sparkSession
    makeReport(srcPath, repPath)
    BaseUtils.cleanUp()
  }

  private def makeReport(gzPath: String, reportPath: String): Unit = {
    BaseUtils.extractFromGz(Paths.get(gzPath), Paths.get(BaseUtils.AccessLogPath))

    val rawLogs = sparkSession.read.text(gzPath)

    val stringifiedRawLogs = rawLogs.map(_.getString(0))(Encoders.STRING)

    val regCleanedLogs: Dataset[List[String]] = stringifiedRawLogs.flatMap(x => SepRegex.unapplySeq(x))(Encoders.product[List[String]])
    val mappedLogs: Dataset[AccessLog] = regCleanedLogs.map(toAccessLog)(Encoders.product[AccessLog])

    val mappedLogsWithTime = mappedLogs.withColumn("datetime", to_timestamp(mappedLogs("datetime"), "dd/MMM/yyyy:HH:mm:ss X"))

    mappedLogsWithTime.createOrReplaceTempView(AccessLogView)

    val ipCountDateGrouped = sparkSession.sql(ipQuery).as[IpAddressCount](Encoders.product[IpAddressCount])

    LogUtils.retrieveIpReport(ipCountDateGrouped, s"${reportPath}/report_ips.json")

    val uriCountDateGrouped = sparkSession.sql(uriQuery).as[UriCount](Encoders.product[UriCount])

    LogUtils.retrieveUrisReport(uriCountDateGrouped, s"${reportPath}/report_uris.json")

  }

}
