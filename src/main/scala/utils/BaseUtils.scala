package utils

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, collect_list, map_from_arrays}

import java.nio.file.{Path, Paths}
import scala.util.matching.Regex

object BaseUtils {
  val AccessLogPath = "src/res/access.log"
  val SepRegex: Regex = """^(?<ip>[0-9.]+) (?<identd>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*) (?<size>[^ ]*) \"(?<referer>[^\"]*)\" \"(?<useragent>[^\"]*)\" \"(?<unk>[^\"]*)\"""".r
  val AccessLogView = "AccessLog"
  val ipQuery = "select cast(datetime as date) as date, ip, count(*) as count from AccessLog group by date,ip having count > 20000 order by count desc"
  val uriQuery = "select cast(datetime as date) as date, request, count(*) as count from AccessLog group by date,request having count > 20000 order by count desc"

  def reportExtractor[T](dataset: Dataset[T], colName: String, alias: String, updatedColName: String, outputPath: String): Unit = {
    CustFileUtils.deleteIfExists(Paths.get(outputPath))
    dataset.groupBy("date").agg(collect_list(col(colName))
      .as(alias), collect_list("count")
      .as("counts")).select(col("date"), map_from_arrays(col(alias), col("counts")))
      .withColumnRenamed(s"map_from_arrays(${alias}, counts)", updatedColName)
      .coalesce(1).write.format("json").save(outputPath)
  }

  def extractFromGz(sourceFile: Path, destFile: Path): Unit = {
    CustFileUtils.decompressGzipNio(sourceFile, destFile)
  }

  def cleanUp(): Unit = {
    CustFileUtils.deleteFile(Paths.get(BaseUtils.AccessLogPath))
  }

}
