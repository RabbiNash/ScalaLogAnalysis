package mappers

import models.AccessLog

object AccessLogMapper {

  def toAccessLog(params: List[String]): AccessLog = AccessLog(
    params.head,
    params(IdentPos),
    params(UserPos),
    params(DateTimePos),
    params(RequestPos),
    params(StatusPos),
    params(SizePos),
    params(RefererPos),
    params(UserAgentPos),
    params(UnkPos))

  //constants to avoid magic numbers
  val IdentPos = 1
  val UserPos = 2
  val DateTimePos = 3
  val RequestPos = 4
  val StatusPos = 5
  val SizePos = 6
  val RefererPos = 7
  val UserAgentPos = 8
  val UnkPos = 9
}
