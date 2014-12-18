package citi_test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import citi.test.processor.FIXProcessor
import citi.test.processor.FIXConstructor
import citi.test.processor.Processor
import citi.test.processor.Constructor
import org.apache.spark.Logging
import org.apache.log4j.{ Level, Logger }
import citi.test.processor.FIXPacket
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

class SocketConsumer extends Consumer with FIXDecoder {

  var log = Logger.getRootLogger()
  case class Persons(name: String, age: Int)
  
  import SocketConsumer._
  import sqlContext._
  def start() {

    var conf = ConfigFactory.load()

    val sparkConf = new SparkConf().setAppName("socoketConsumer").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sql = new SQLContext(sc)
    startJob(ssc, sql, "localhost", 9007)
    startJob(ssc, sql, "localhost", 9008)
    
    
    ssc.start()
    ssc.awaitTermination()
  }

  def startJob(ssc: StreamingContext, sql:SQLContext, host: String, port: Int) {
     import sql.createSchemaRDD
    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    var stream = lines.map(decode).transform(rdd => {rdd.map(FIXPacket(_))})
    stream.foreachRDD(rdd => { 
      if (rdd.count >0) {
    	  val bb = rdd.registerAsTable("temp")
    	  val adas = rdd.map(p => Persons("hello", 123))	
      }
    }
    )
    //func is running on the driver , not on the working node.
//    rdds.foreachRDD(processor ! new Chain(_, constructor, publisher))
    log.info("job start on " + host + ":" + port)

  }
}

object SocketConsumer {

  var processor = new FIXProcessor
  var constructor = new FIXConstructor
  var publisher = new StdoutPublisher

  processor.start
  constructor.start
  publisher.start

}