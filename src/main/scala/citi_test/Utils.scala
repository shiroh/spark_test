package citi_test

object Utils {
  val format = new java.text.SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSS")
  val minFormat = new java.text.SimpleDateFormat("yyyyMMdd-HH:mm")
  def TimeConvert(dateStr: String): Long = {
    try {
      format.parse(dateStr).getTime()
    } catch {
      case _:Throwable => -1
    }
  }

  def DateConvert(dateStr: String) = {
    minFormat.parse(dateStr)
  }
}

object Tags {
  val DATE = "52"
  val INSTRUMENT = "A55"
  val TENOR = "A911"
  val BID = "A188_0"
  val ASK = "A190_0"
}

object TENORS {
  val SPOT = "SPOT"
  val ONE_M = "1M"
  val TWO_M = "2M"
  val THREE_M = "3M"
  val ONE_Y = "1Y"

  def contain(t: String) = {
    t match {
      case SPOT => true
      case ONE_M => true
      case TWO_M => true
      case THREE_M => true
      case ONE_Y => true
      case _ => false
    }
  }
}