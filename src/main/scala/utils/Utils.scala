package utils

import scala.util.matching.Regex

object Utils {

  val AccessLogPath = "src/res/access.log"
  val CompressedAccessLogPath = "src/res/access.log.gz"
  val OutputUriJsonPath = "src/res/reports/report_uris.json"
  val OutputIpJsonPath = "src/res/reports/report_ip.json"
  val reportsDir = "src/res/reports/"
  val SepRegex: Regex = """^(?<ip>[0-9.]+) (?<identd>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*) (?<size>[^ ]*) \"(?<referer>[^\"]*)\" \"(?<useragent>[^\"]*)\" \"(?<unk>[^\"]*)\"""".r
  val AccessLogView = "AccessLog"
}
