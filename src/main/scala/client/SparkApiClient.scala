package client
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.thrift.transport.{TSocket, TTransport}
import com.github.msr.SparkAPI
import scala.jdk.CollectionConverters.IterableHasAsScala

object SparkApiClient  extends App{
  val port = 9103
  val host = "127.0.0.1"

  def execute_query():Unit = {
    val query = "select hour,m1 from RawTable limit 10"
    val transport = new TSocket(host,port)
    val protocol = new TBinaryProtocol(transport)
    val client = new SparkAPI.Client(protocol)
    transport.open()
    val res = client.exectureCommand(query)
    println(res)
  }

  execute_query()

}
