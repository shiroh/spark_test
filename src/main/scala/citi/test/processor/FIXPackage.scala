package citi.test.processor

import scala.collection.mutable.HashMap
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import citi_test.Tags
import citi_test.Utils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.log4j.{ Level, Logger }
import citi_test.TENORS

class FIXPacket(p: Option[Array[(String, String)]]) extends IsBrokenPacket {
  private var entrySet = new HashMap[String, String]
  var log = Logger.getRootLogger

  p match {
    case arr: Some[Array[(String, String)]] => {
      arr.get.foreach(x => entrySet(x._1) = x._2)
    }
    case None => Unit
  }

  // should check the header in the real FIX protocol
  def isBroken = {
    if (entrySet.isEmpty) {
      true
    } else if (!TENORS.contain(entrySet.getOrElse(Tags.TENOR, "notenor"))) {
      log.error("input tenor:" + entrySet.getOrElse(Tags.TENOR, "noTenor") + " is not supported")
      true
    } else if (Utils.TimeConvert(entrySet.getOrElse(Tags.DATE, "noDate")) == -1) {
      log.error("input date:" + entrySet.getOrElse(Tags.DATE, "nodate") + " is not supported")
      true
    } else if (!entrySet.isEmpty &&
      entrySet.contains(Tags.DATE) &&
      entrySet.contains(Tags.INSTRUMENT) &&
      entrySet.contains(Tags.TENOR) &&
      TENORS.contain(entrySet.getOrElse(Tags.TENOR, "notenor")) &&
      entrySet.contains(Tags.BID) &&
      entrySet.contains(Tags.ASK)) {
      false
    } else {
      log.error("package is broken or incomplete")
      true
    }
  }

  def getDate = entrySet(Tags.DATE)
  def getInstrument = entrySet(Tags.INSTRUMENT)
  def getTenor = entrySet(Tags.TENOR)
  def getMidPrice = (BigDecimal.apply(entrySet(Tags.BID)) + BigDecimal.apply(entrySet(Tags.ASK))) / 2
  def getTs = Utils.TimeConvert(entrySet(Tags.DATE))
  def getTag = entrySet(Tags.INSTRUMENT) + entrySet(Tags.TENOR)

  override def toString = "Time:" + getDate + " instrument:" + getInstrument + " tenor" + getTenor + " MidPrice" + getMidPrice
}
object FIXPacket {
  def apply(input: Option[Array[(String, String)]]) = {
    new FIXPacket(input)
  }
}


