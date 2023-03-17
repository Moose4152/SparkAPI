package client
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.thrift.transport.{TSocket, TTransport}
import com.github.msr.{FileType, ReadData, SQLStatment, SparkAPI}

import scala.jdk.CollectionConverters.IterableHasAsScala
object SparkApiClient  extends App{
  val port = 9103
  val host = "127.0.0.1"

  def readData():Unit = {

    val transport = new TSocket(host,port)
    val protocol = new TBinaryProtocol(transport)
    val client = new SparkAPI.Client(protocol)
    transport.open()
    val Query = new ReadData()
    Query.setPath("/Users/mayanksinghrana/Desktop/TdigestExp/Functional/rollup1/")
    Query.setDataType(FileType.Parquet)
    val res = client.readFile(Query)
    println(res)
  }



  def execute_statment() = {
    val query = "select hour,m1 from RAW limit 10"
    val transport = new TSocket(host,port)
    val protocol = new TBinaryProtocol(transport)
    val client = new SparkAPI.Client(protocol)
    transport.open()
    val SQLStatment = new SQLStatment()
    SQLStatment.setSql_statment(query)
    val res = client.exectureCommand(SQLStatment)
    println(res)
  }

  readData()
  execute_statment()

}
