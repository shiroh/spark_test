package citi_test

import scala.actors.Actor
import scala.collection.mutable.HashMap
import citi.test.processor.{ OHLCPrice, TENORPrice }
import akka.actor.ActorSystem
import org.apache.spark.streaming.Duration
import com.twitter.util.TimerTask
import com.twitter.util.ScheduledThreadPoolTimer
import com.twitter.util.Time
import com.twitter.conversions.time._
import scala.actors.TIMEOUT

class StdoutPublisher extends Publisher {
  var OHLCMap = new HashMap[String, OHLCPrice]
  var TENORMap = new HashMap[String, TENORPrice]
  var curMin = Time.now.inMinutes

  def act {
    try {
      while (true) {
        receiveWithin(1000) {
          case price: OHLCPrice =>
            OHLCMap(price.instrument + price.tenor) = price; checkOnMinute(emit)
          case price: TENORPrice =>
            TENORMap(price.instrument) = price; checkOnMinute(emit)
          case TIMEOUT => checkOnMinute(emit)
          case _ => println("unknown input data")
        }
      }
    } catch {
      case th: Throwable => println(th)
    }
  }

  def checkOnMinute(func: => Unit) {
    var min = Time.now.inMinutes
    var sec = Time.now.inSeconds

    if (sec%60 == 0 && min != curMin) {
      curMin = min
      func
    }
  }
  def emit {
    OHLCMap.valuesIterator.foreach(p => {
      var str = Utils.TimeConvert(p.date).minutes + "\t" + p.instrument + "\t" + p.tenor + "\t" + p.openPrice + "\t" + p.highPrice + "\t" + p.lowPrice + "\t" + p.closePrice
      println(str)
    })

    TENORMap.valuesIterator.foreach(p => {
      var str = Utils.TimeConvert(p.date).minutes + "\t" + p.instrument + "\t" + p.oneMPrice + "\t" + p.twoMPrice + "\t" + p.threeMPrice + "\t" + p.oneYPrice
      println(str)
    })
  }
}