package citi.test.processor

import scala.actors.Actor
import scala.collection.mutable.HashMap
import citi_test.TENORS
import citi_test.Utils
import citi_test.StdoutPublisher
import citi_test.Chain

class FIXConstructor extends Constructor {
  private var OHLCPriceMap = new HashMap[String, OHLCPrice]
  private var TENORPriceMap = new HashMap[String, TENORPrice]

  def act {
    while (true) {
      receive {
        case Chain(data, _, pub) => {
          data match {
            case p: FIXPacket => {
              var oPrice = OHLCPriceMap.getOrElseUpdate(p.getTag, OHLCPrice(p.getDate, p.getInstrument, p.getTenor, p.getMidPrice, p.getMidPrice, p.getMidPrice, p.getMidPrice))
              var tPrice = TENORPriceMap.getOrElseUpdate(p.getInstrument, TENORPrice(p.getDate, p.getInstrument, 0, 0, 0, 0, 0))

              var newOHLC = refreshOHLC(oPrice, p)
              var newTenor = refreshTENOR(tPrice, p)
              OHLCPriceMap(p.getTag) = newOHLC
              TENORPriceMap(p.getInstrument) = newTenor
              pub ! newOHLC
              pub ! newTenor
            }
            case _ => println("incorrect input format , fix packet required")
          }
        }
        case _ => println("Unknown package")
      }
    }
  }

  def refreshOHLC(last: OHLCPrice, p: FIXPacket) = {
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

  def refreshTENOR(last: TENORPrice, p: FIXPacket) = {
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
