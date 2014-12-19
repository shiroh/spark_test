package citi_test

import scala.Array.canBuildFrom
import scala.actors.Actor
import scala.collection.mutable.HashMap
import scala.math.BigDecimal.int2bigDecimal
import scala.reflect.runtime.universe
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

case class OHLCPrice(date: String, instrument: String, tenor: String, openPrice: BigDecimal, highPrice: BigDecimal, lowPrice: BigDecimal, closePrice: BigDecimal)
case class TENORPrice(date: String, instrument: String, spotPrice: BigDecimal = 0, oneMPrice: BigDecimal = 0, twoMPrice: BigDecimal = 0, threeMPrice: BigDecimal = 0, oneYPrice: BigDecimal = 0)
case class FXPacket(ts: String, currency: String, tenor: String, bid: String, ask: String)

class SocketConsumer extends FIXDecoder {

  var log = Logger.getRootLogger()
  val table = "FXPacket"
  import SocketConsumer._

  def start() {
    Logger.getRootLogger().setLevel(Level.WARN)
    var conf = ConfigFactory.load()

    val sparkConf = new SparkConf().setAppName("socoketConsumer").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val sqc = new SQLContext(sc);
    initSQL(sc, sqc)
    val ssc = new StreamingContext(sc, Seconds(1))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    initStreaming(ssc, sqc, "localhost", 9007, table)
    initStreaming(ssc, sqc, "localhost", 9008, table)

    startJob(sqc, table)
    ssc.start()
    ssc.awaitTermination()
  }

  def initSQL(sc: SparkContext, sqc: SQLContext) {
    import sqc.createSchemaRDD
    import scala.reflect._

    var pFile = sqc.createParquetFile[FXPacket]("tmp.parquet." + System.currentTimeMillis())
    pFile.registerTempTable(table)
  }

  def initStreaming(ssc: StreamingContext, sqc: SQLContext, host: String, port: Int, tableName: String) {
    import sqc.createSchemaRDD
    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    var stream = lines.map(decode).transform(rdd => { rdd.filter(_.nonEmpty).map(_.get) })
    stream.foreachRDD(rdd => {
      if (rdd.count != 0) {
        rdd.insertInto("FXPacket")
      }
    })
    log.info("job start on " + host + ":" + port)
  }

  def startJob(sql: SQLContext, table: String) {
    val processor = new Processor(sql, table)
    val timer = new Timer(processor)

    processor.start
    timer.start
  }
}

object SocketConsumer {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spotConsumer = new SocketConsumer
    spotConsumer.start()
  }
}

class Processor(sqc: SQLContext, table: String) extends Actor {
  var OHLCMap = new HashMap[String, OHLCPrice]
  var TENORMap = new HashMap[String, TENORPrice]

  def act {
    while (true) {
      receive {
        case true => {
          update
          output
          clean
        }
      }
    }
  }

  def update {
    val t = sqc.sql("SELECT * FROM FXPacket ORDER BY ts")
    t.collect.foreach(println)
  }

  def output = {

  }

  def clean = {
    println("start to clean")
//    sqc.uncacheTable(table)
    var pFile = sqc.createParquetFile[FXPacket]("tmp.parquet." + System.currentTimeMillis())
    pFile.registerTempTable(table)
  }
}
