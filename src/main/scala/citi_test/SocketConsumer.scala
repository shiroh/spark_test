package citi_test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import citi.test.processor.FIXDecoder

class SocketConsumer extends Consumer {

  def start() {
    var conf = ConfigFactory.load()
    
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    var processor = new FIXDecoder()
    processor.start()
    
    val lines = ssc.socketTextStream("localhost", 12345, StorageLevel.MEMORY_AND_DISK_SER)
    processor ! lines
    
    ssc.start()
    ssc.awaitTermination()
  }
}