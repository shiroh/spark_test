package citi.test.processor

import scala.collection.mutable.HashMap
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import citi_test.Tags
import citi_test.Utils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

class FIXPacket(rdd: RDD[Option[Array[(String, String)]]]) extends IsBrokenPackage {
  private var entrySet = new HashMap[String, String]
  
  for (p <- rdd.collect) {
    p match {
      case arr: Some[Array[(String, String)]] => {
        arr.get.foreach(x => entrySet(x._1) = x._2)
      }
      case None => println("none")
    }
  }

  // should check the header in the real FIX protocol
  def isBroken = entrySet.isEmpty

  def getDate = entrySet(Tags.DATE)
  def getInstrument = entrySet(Tags.INSTRUMENT)
  def getTenor = entrySet(Tags.TENOR)
  def getMidPrice = (BigDecimal.apply(entrySet(Tags.BID)) + BigDecimal.apply(entrySet(Tags.ASK))) / 2
  def getTs = Utils.TimeConvert(entrySet(Tags.DATE))
  def getTag = entrySet(Tags.INSTRUMENT) + entrySet(Tags.TENOR)

  override def toString = "Time:" + getDate + " instrument:" + getInstrument + " tenor" + getTenor + " MidPrice" + getMidPrice 
}
object FIXPacket {
  def apply(input: RDD[Option[Array[(String, String)]]]) = {
    new FIXPacket(input)
  }
}


