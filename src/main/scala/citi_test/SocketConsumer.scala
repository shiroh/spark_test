package citi_test

import scala.actors.Actor
import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{ Level, Logger }
import scala.reflect._

case class OHLCPrice(date: String, instrument: String, tenor: String, openPrice: BigDecimal, highPrice: BigDecimal, lowPrice: BigDecimal, closePrice: BigDecimal)
case class TENORPrice(date: String, instrument: String, spotPrice: BigDecimal = 0, oneMPrice: BigDecimal = 0, twoMPrice: BigDecimal = 0, threeMPrice: BigDecimal = 0, oneYPrice: BigDecimal = 0)
case class FXPacket(ts: String, currency: String, tenor: String, bid: String, ask: String)

class SocketConsumer extends FIXDecoder {

  var log = Logger.getRootLogger()
  val tables = Array("FXPacket")
  import SocketConsumer._

  def start() {
    var conf = ConfigFactory.load()

    val sparkConf = new SparkConf().setAppName("socoketConsumer").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqc = new SQLContext(sc);
    initSQL(sc, sqc)

    val ssc = new StreamingContext(sc, Seconds(1))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    initStreaming(ssc, sqc, "localhost", 9007, tables(0))
    initStreaming(ssc, sqc, "localhost", 9008, tables(0))

    startJob(sqc, tables)

    ssc.start()
    ssc.awaitTermination()
  }

  def initSQL(sc: SparkContext, sqc: SQLContext) {
    import sqc.createSchemaRDD
    //    var packetRDD: RDD[FXPacket] = sc.emptyRDD(classTag[FXPacket])
//    var pFile = sqc.createParquetFile[FXPacket]("tmp.parquet")
//    pFile.registerAsTable("FXPacket")
    //    packetRDD.registerTempTable(tables(0))
    
    val packetRDD = sc.parallelize(Array(("0", "0", "0", "0", "0"))).map(e => FXPacket(e._1, e._2, e._3, e._4, e._5))
    packetRDD.saveAsParquetFile("tmp.parquet")
    val parquetFile = sqc.parquetFile("tmp.parquet")
    parquetFile.registerAsTable(tables(0))
  }

  def initStreaming(ssc: StreamingContext, sql: SQLContext, host: String, port: Int, tableName: String) {
    import sql.createSchemaRDD
    sql.cacheTable(tableName)
    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    var stream = lines.map(decode).transform(rdd => { rdd.filter(_.nonEmpty).map(_.get) })
    stream.foreachRDD(rdd => {

      if (rdd.count != 0) {
        println(rdd.count)
        rdd.registerTempTable("tmp")
        
        var newRDD = rdd.unionAll(sql.table("FXPacket"))
        newRDD.registerTempTable("FXPacket")
        var table = sql.table("FXPacket")
        for (p <- table.collect) {
          println(p)
//          sql.sql("insert into FXPacket select * from tmp")
//          sql.sql("insert into FXPacket values(" + p.ts + "," + p.currency + "," + p.tenor + "," + p.bid + "," + p.ask + ")")
        }
        
//        rdd.insertInto(tableName)
      }
      //      println(rdd)
    })
    log.info("job start on " + host + ":" + port)
  }

  def startJob(sql: SQLContext, tables: Array[String]) {
    val processor = new Processor(sql, tables)
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

class Processor(sqc: SQLContext, tableNameList: Array[String]) extends Actor {
  var OHLCMap = new HashMap[String, OHLCPrice]
  var TENORMap = new HashMap[String, TENORPrice]

  def act {
    while (true) {
      receive {
        case true => {
          update _
          output _
          clean _
        }
      }
    }
  }

  val update = () => {
    val ohlc = sqc.sql("SELECT * FROM OHLC ORDER BY ts")
    ohlc.collect.foreach(println)
  }

  val output = () => {

  }

  val clean = () => {
    for (table <- tableNameList) yield sqc.uncacheTable(table)
  }
}