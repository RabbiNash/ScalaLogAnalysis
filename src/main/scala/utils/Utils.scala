package utils

import scala.util.matching.Regex

object Utils {
  val AccessLogPath = "src/res/access.log"
  val SepRegex: Regex = """^(?<ip>[0-9.]+) (?<identd>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*) (?<size>[^ ]*) \"(?<referer>[^\"]*)\" \"(?<useragent>[^\"]*)\" \"(?<unk>[^\"]*)\"""".r
  val AccessLogView = "AccessLog"
}
