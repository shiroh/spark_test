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

class StdoutPublisher extends Publisher with Actor {
  var OHLCMap = new HashMap[String, OHLCPrice]
  var TENORMap = new HashMap[String, TENORPrice]

  def act {
    while (true) {
      receiveWithin(1000) {
        case price: OHLCPrice => OHLCMap(price.instrument + price.tenor) = price ; checkOnMinute(emit)
        case price: TENORPrice => TENORMap(price.instrument) = price; checkOnMinute(emit)
        case TIMEOUT => checkOnMinute(emit)
      }
    }
  }

  //TODO Logic error ,only print once every minute
  def checkOnMinute(f: => Unit) {
    if (Time.now.inSeconds == 0) f
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