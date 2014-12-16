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

class SocketConsumer extends Consumer with FIXDecoder {

  import SocketConsumer._
  def start() {

    var conf = ConfigFactory.load()

    val sparkConf = new SparkConf().setAppName("socoketConsumer").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    compute(ssc, "localhost", 9007)
    compute(ssc, "localhost", 9008)

    ssc.start()
    ssc.awaitTermination()
  }

  def compute(ssc: StreamingContext, host: String, port: Int) {
    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    var rdds = lines.map(decode)
    rdds.foreachRDD(processor ! new Chain(_, constructor, publisher))
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