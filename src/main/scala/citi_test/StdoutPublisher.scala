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
import org.apache.spark.Logging
import org.apache.log4j.{ Level, Logger }

class StdoutPublisher extends Publisher {
  var OHLCMap = new HashMap[String, OHLCPrice]
  var TENORMap = new HashMap[String, TENORPrice]
  var curMin = Time.now.inMinutes
  var log = Logger.getRootLogger

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
      case ex: Throwable => log.error("exception was caught in publisher:" + ex.getMessage(), ex)
    }
  }

  def checkOnMinute(func: => Unit) {
    var min = Time.now.inMinutes
    var sec = Time.now.inSeconds

    if (sec % 60 == 0 && min != curMin) {
      curMin = min
      func
    }
  }
  def emit {
    val ohlcTitle = "Time(till min)\tCurrency\tTenor\tOpen-Mid\tHigh-Mid\tLow-Mid\tClose-Mid"
    println(ohlcTitle)
    OHLCMap.valuesIterator.foreach(p => {
      var str = Utils.DateConvert(p.date) + "\t" + p.instrument + "\t" + p.tenor + "\t" + p.openPrice + "\t" + p.highPrice + "\t" + p.lowPrice + "\t" + p.closePrice
      println(str)
    })

    val tenorTitle = "Time(till min)\tCurrencyPair\tSpot\t1M\t2M\t3M\t1Y"
    println(tenorTitle)
    TENORMap.valuesIterator.foreach(p => {
      var str = Utils.DateConvert(p.date) + "\t" + p.instrument + "\t" + p.spotPrice + "\t" + p.oneMPrice + "\t" + p.twoMPrice + "\t" + p.threeMPrice + "\t" + p.oneYPrice
      println(str)
    })
  }
}