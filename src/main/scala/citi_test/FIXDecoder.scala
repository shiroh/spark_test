package citi_test

import org.apache.spark.Logging
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap

trait FIXDecoder {
  //Note RDD will be serialized when map, flatmap called.
  //make decode to be a function instead of a method(function are objects in scala), so that spark will be able to serialize it
  //Also remember put the SOH into the function body , otherwise the whole FIXDecoder class will be serialized

  //Alternative way is to serialize the whole class which contian the RDD.map methond
  val decode = (str: String) => {
    var log = Logger.getRootLogger
    val map = new HashMap[String, String]
    val SOH = " "
    try {
      val fields = str.split(SOH)
      for (field <- fields) {
        val pair = field.split("=")
        map(pair(0)) = pair(1)
      }
      if (!TENORS.contain(map(Tags.TENOR))) {
        log.error("unsupported tenor " + map(Tags.TENOR))
        None
      } else if (Utils.TimeConvert(map(Tags.DATE)) == -1) {
        log.error("unsupported date " + map(Tags.DATE))
        None
      } else {
        Some(FXPacket(map(Tags.DATE), map(Tags.INSTRUMENT), map(Tags.TENOR), map(Tags.BID), map(Tags.ASK)))
      }
    } catch {
      case _ => {
        log.error("unrecognized incoming message")
        None
      }
    }
  }
}