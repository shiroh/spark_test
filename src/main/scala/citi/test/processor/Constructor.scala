package citi.test.processor

import scala.actors.Actor
import scala.collection.mutable.HashMap
import citi_test.TENORS
import citi_test.Utils
import citi_test.StdoutPublisher

class Constructor extends Actor {
  private var OHLCPriceMap = new HashMap[String, OHLCPrice]
  private var TENORPriceMap = new HashMap[String, TENORPrice]
  var publishActor = new StdoutPublisher
  publishActor.start
  
  def act {
    while (true) {
      receive {
        case p: FIXPackage => {
          var oPrice = OHLCPriceMap.getOrElse(p.getTag, new OHLCPrice(p.getDate, p.getInstrument, p.getTenor, p.getMidPrice, p.getMidPrice, p.getMidPrice, p.getMidPrice))
          var tPrice = TENORPriceMap.getOrElse(p.getInstrument,
            p.getTenor match {
              case TENORS.SPOT => TENORPrice(p.getDate, p.getInstrument, p.getMidPrice, _: BigDecimal, _: BigDecimal, _: BigDecimal, _: BigDecimal)
              case TENORS.ONE_M => TENORPrice(p.getDate, p.getInstrument, _: BigDecimal, p.getMidPrice, _: BigDecimal, _: BigDecimal, _: BigDecimal)
              case TENORS.TWO_M => TENORPrice(p.getDate, p.getInstrument, _: BigDecimal, _: BigDecimal, p.getMidPrice, _: BigDecimal, _: BigDecimal)
              case TENORS.THREE_M => TENORPrice(p.getDate, p.getInstrument, _: BigDecimal, _: BigDecimal, _: BigDecimal, p.getMidPrice, _: BigDecimal)
              case TENORS.ONE_Y => TENORPrice(p.getDate, p.getInstrument, _: BigDecimal, _: BigDecimal, _: BigDecimal, _: BigDecimal, p.getMidPrice)
            })
          publishActor != oPrice
          publishActor != tPrice
        }
        case _ => println("Unknown package")
      }
    }
  }

  def refreshOHLC(last: OHLCPrice, p: FIXPackage) = {
    if ((p.getTs / 60000) - (Utils.TimeConvert(last.date) / 60000) > 0)
      OHLCPrice(p.getDate, p.getInstrument, p.getTenor, p.getMidPrice, p.getMidPrice, p.getMidPrice, p.getMidPrice)
    else {
      if (p.getMidPrice > last.highPrice)
        OHLCPrice(p.getDate, p.getInstrument, p.getTenor, last.openPrice, p.getMidPrice, last.lowPrice, p.getMidPrice)
      else if (p.getMidPrice < last.lowPrice)
        OHLCPrice(p.getDate, p.getInstrument, p.getTenor, last.openPrice, last.highPrice, p.getMidPrice, p.getMidPrice)
      else
        OHLCPrice(p.getDate, p.getInstrument, p.getTenor, last.openPrice, last.highPrice, last.lowPrice, p.getMidPrice)
    }
  }

  def refreshOHLC(last: TENORPrice, p: FIXPackage) = {
    p.getTenor match {
      case TENORS.SPOT => TENORPrice(p.getDate, p.getInstrument, p.getMidPrice, last.oneMPrice, last.twoMPrice, last.threeMPrice, last.oneYPrice)
      case TENORS.ONE_M => TENORPrice(p.getDate, p.getInstrument, last.spotPrice, p.getMidPrice, last.twoMPrice, last.threeMPrice, last.oneYPrice)
      case TENORS.TWO_M => TENORPrice(p.getDate, p.getInstrument, last.spotPrice, last.oneMPrice, p.getMidPrice, last.threeMPrice, last.oneYPrice)
      case TENORS.THREE_M => TENORPrice(p.getDate, p.getInstrument, last.spotPrice, last.oneMPrice, last.twoMPrice, p.getMidPrice, last.oneYPrice)
      case TENORS.ONE_Y => TENORPrice(p.getDate, p.getInstrument, last.spotPrice, last.oneMPrice, last.twoMPrice, last.threeMPrice, p.getMidPrice)
      case _ => last
    }
  }
}
