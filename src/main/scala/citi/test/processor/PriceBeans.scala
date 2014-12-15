package citi.test.processor

case class OHLCPrice(date: String, instrument: String, tenor: String, openPrice: BigDecimal, highPrice: BigDecimal, lowPrice: BigDecimal, closePrice: BigDecimal)
case class TENORPrice(date: String, instrument: String, spotPrice: BigDecimal = 0, oneMPrice: BigDecimal = 0, twoMPrice: BigDecimal = 0, threeMPrice: BigDecimal = 0, oneYPrice: BigDecimal = 0)