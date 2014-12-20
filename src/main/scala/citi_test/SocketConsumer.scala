package citi_test

import scala.actors.Actor
import scala.collection.mutable.HashMap
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
import org.apache.spark.sql.SchemaRDD

case class OHLCPrice(ts: String, currency: String, tenor: String, var openPrice: String, var highPrice: String, var lowPrice: String, var closePrice: String)
case class TENORPrice(ts: String, currency: String, var spotPrice: String, var oneMPrice: String, var twoMPrice: String, var threeMPrice: String, var oneYPrice: String)
case class FXPacket(ts: String, currency: String, tenor: String, bid: String, ask: String)

class SocketConsumer extends FIXDecoder {

  var log = Logger.getRootLogger()
  val table = "FXPacket"
  import SocketConsumer._

  def start() {
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

    var pFile = sqc.createParquetFile[FXPacket]("parquet/tmp.parquet." + System.currentTimeMillis())
    pFile.registerTempTable(table)
  }

  def initStreaming(ssc: StreamingContext, sqc: SQLContext, host: String, port: Int, tableName: String) {
    import sqc.createSchemaRDD
    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    var stream = lines.map(decode).transform(rdd => { rdd.filter(_.nonEmpty).map(_.get) })
    stream.foreachRDD(rdd => {
      if (rdd.count != 0) {
        var a = rdd.collect.foreach(_.ask)
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
  var log = Logger.getRootLogger()
  var OHLCMap = new HashMap[String, OHLCPrice]
  var snapshotMap = new HashMap[String, TENORPrice]

  def act {
    while (true) {
      receive {
        case true => {
          try {
            transfer
            clean
            update
            output
          } catch {
            case _: Throwable => {
              log.error("no message coming or error happen")
            }
          }
        }
      }
    }
  }

  def transfer = {
    //since sqc.sql is extremely slow, create a tmp table in memory for the further sql  
    val schemaRDD = sqc.table(table)
    schemaRDD.registerTempTable("lastMinTable")

  }
  def clean = {
    var pFile = sqc.createParquetFile[FXPacket]("parquet/tmp.parquet." + System.currentTimeMillis())
    pFile.registerTempTable(table)
  }

  def update {
    //get max price 
    sqc.sql("SELECT  currency, tenor, MAX(bid + ask) AS price FROM lastMinTable GROUP BY currency, tenor").registerAsTable("maxt")
    val tmax = sqc.sql("SELECT DISTINCT lastMinTable.ts, lastMinTable.currency, lastMinTable.tenor, (lastMinTable.ask + lastMinTable.bid) AS price FROM lastMinTable INNER JOIN maxt ON lastMinTable.currency = maxt.currency AND lastMinTable.tenor = maxt.tenor AND (lastMinTable.ask + lastMinTable.bid) = maxt.price")

    //get min price
    sqc.sql("SELECT  currency, tenor, MIN(bid + ask) AS price FROM lastMinTable GROUP BY currency, tenor").registerAsTable("mint")
    val tmin = sqc.sql("SELECT DISTINCT lastMinTable.ts, lastMinTable.currency, lastMinTable.tenor, (lastMinTable.ask + lastMinTable.bid) AS price FROM lastMinTable INNER JOIN mint ON lastMinTable.currency = mint.currency AND lastMinTable.tenor = mint.tenor AND (lastMinTable.ask + lastMinTable.bid) = mint.price")

    //open price
    sqc.sql("SELECT MIN(ts) AS ts, currency, tenor FROM lastMinTable GROUP BY currency, tenor").registerAsTable("opent")
    val topen = sqc.sql("SELECT DISTINCT lastMinTable.ts, lastMinTable.currency, lastMinTable.tenor, (lastMinTable.ask + lastMinTable.bid) AS price FROM lastMinTable INNER JOIN opent ON lastMinTable.currency = opent.currency AND lastMinTable.tenor = opent.tenor AND lastMinTable.ts = opent.ts")

    //close price
    sqc.sql("SELECT MAX(ts) AS ts, currency, tenor FROM lastMinTable GROUP BY currency, tenor").registerAsTable("closet")
    val tclose = sqc.sql("SELECT DISTINCT lastMinTable.ts, lastMinTable.currency, lastMinTable.tenor, (lastMinTable.ask + lastMinTable.bid) AS price FROM lastMinTable INNER JOIN closet ON lastMinTable.currency = closet.currency AND lastMinTable.tenor = closet.tenor AND lastMinTable.ts = closet.ts")

    updateMap(topen, "open")
    updateMap(tmax, "high")
    updateMap(tmin, "low")
    updateMap(tclose, "close")

  }

  def updateMap(rdd: SchemaRDD, field: String) {
    rdd.collect.foreach(row => {
      val ts = row.getString(0)
      val currency = row.getString(1)
      val tenor = row.getString(2)
      val price = row.getDouble(3) / 2
      val key = row.getString(1) + row.getString(2)
      var p = OHLCMap.getOrElseUpdate(key, OHLCPrice(ts, currency, tenor, "0", "0", "0", "0"))
      field match {
        case "open" => p.openPrice = price.toString
        case "high" => p.highPrice = price.toString
        case "low" => p.lowPrice = price.toString
        case "close" => {
          p.closePrice = price.toString
          var s = snapshotMap.getOrElseUpdate(currency, TENORPrice(ts, currency, "0", "0", "0", "0", "0"))
          tenor match {
            case TENORS.SPOT => s.spotPrice = price.toString
            case TENORS.ONE_M => s.oneMPrice = price.toString
            case TENORS.TWO_M => s.twoMPrice = price.toString
            case TENORS.THREE_M => s.threeMPrice = price.toString
            case TENORS.ONE_Y => s.oneYPrice = price.toString
          }
        }
      }
    })
  }
  def output = {
    val ohlcTitle = "Time(till min)\tCurrency\tTenor\tOpen-Mid\tHigh-Mid\tLow-Mid\tClose-Mid"
    println(ohlcTitle)
    val minute = System.currentTimeMillis() - 60000
    OHLCMap.valuesIterator.foreach(p => {

      var str = Utils.MinConvert(minute) + "\t" + p.currency + "\t" + p.tenor + "\t" + p.openPrice + "\t" + p.highPrice + "\t" + p.lowPrice + "\t" + p.closePrice
      println(str)
    })

    val tenorTitle = "Time(till min)\tCurrencyPair\tSpot\t1M\t2M\t3M\t1Y"
    println(tenorTitle)
    snapshotMap.valuesIterator.foreach(p => {
      var str = Utils.MinConvert(minute) + "\t" + p.currency + "\t" + p.spotPrice + "\t" + p.oneMPrice + "\t" + p.twoMPrice + "\t" + p.threeMPrice + "\t" + p.oneYPrice
      println(str)
    })
  }
}
