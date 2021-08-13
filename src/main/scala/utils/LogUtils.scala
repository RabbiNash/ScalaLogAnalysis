package utils

import models.{IpAddressCount, UriCount}
import org.apache.spark.sql.Dataset

object LogUtils {
  def retrieveUrisReport(dataset: Dataset[UriCount], outputPath: String): Unit = {
    BaseUtils.reportExtractor[UriCount](dataset, "request", "requests", "requestCount", outputPath)
  }

  def retrieveIpReport(dataset: Dataset[IpAddressCount], outputPath: String): Unit = {
    BaseUtils.reportExtractor[IpAddressCount](dataset, "ip", "ips", "ipCount", outputPath)
  }
}
