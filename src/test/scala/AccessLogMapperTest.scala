import mappers.AccessLogMapper
import org.scalatest.FunSuite

class AccessLogMapperTest extends FunSuite {

  val testLog: List[String] = List("1", "2", "3", "1", "2", "3", "1", "2", "3", "1")
  test("AccessLogMapper.toAccessLog") {
    assert(testLog.head === AccessLogMapper.toAccessLog(testLog).ip)
  }
}
