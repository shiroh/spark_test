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

class SocketConsumer extends Consumer with FIXDecoder {

  var log = Logger.getRootLogger()
  
  import SocketConsumer._
  import sqlContext._
  def start() {

    var conf = ConfigFactory.load()

    val sparkConf = new SparkConf().setAppName("socoketConsumer").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    startJob(ssc, "localhost", 9007)
    startJob(ssc, "localhost", 9008)

    
    ssc.start()
    ssc.awaitTermination()
  }

  def startJob(ssc: StreamingContext, host: String, port: Int) {
    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    var rdds = lines.map(decode).transform(rdd => {rdd.map(FIXPacket(_))})
    rdds.foreachRDD(rdd => {rdd.("packet")})
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