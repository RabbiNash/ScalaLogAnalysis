import mappers.LogMapper
import org.scalatest.FunSuite

class AccessLogMapperTest extends FunSuite {

  val testLog: List[String] = List("1", "2", "3", "1", "2", "3", "1", "2", "3", "1")
  test("AccessLogMapper.toAccessLog") {
    assert(testLog.head === LogMapper.toAccessLog(testLog).ip)
  }
}
