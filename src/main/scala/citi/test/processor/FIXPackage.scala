package citi.test.processor

import scala.collection.mutable.HashMap
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import citi_test.Tags
import citi_test.Utils

class FIXPackage(input: ReceiverInputDStream[String]) extends IsBrokenPackage{
  private val SOH = 0x01.toChar
  private var entrySet = new HashMap[String, String]

  var pairArray = input.map(decode)
  for (pairs <- pairArray) {
    for (p <- pairs.collect) {
      p match {
        case arr: Some[Array[(String, String)]] => {
          arr.get.foreach(x => entrySet(x._1) = x._2)
        }

        case None => println("none")
      }
    }
  }

  // should check the header in the real FIX protocol
  def isBroken = entrySet.isEmpty 

  def getDate = entrySet(Tags.DATE)
  def getInstrument = entrySet(Tags.INSTRUMENT)
  def getTenor = entrySet(Tags.TENOR)
  def getMidPrice = BigDecimal.apply(entrySet(Tags.BID)) + BigDecimal.apply(entrySet(Tags.ASK)) / 2
  def getTs = Utils.TimeConvert(entrySet(Tags.DATE))
  def getTag = entrySet(Tags.INSTRUMENT) + entrySet(Tags.TENOR)
  
  
  def decode(str: String) = {
    try {
      var fields = str.split(SOH)
      val pairArray = for (field <- fields) yield {
        val pair = field.split("=")
        (pair(0), pair(1))
      }
      Some(pairArray)
    } catch {
      case _ => None
    }
  }
}
object FIXPackage {
  def apply(input: ReceiverInputDStream[String]) = {
    new FIXPackage(input)
  }
}


