package citi_test

object Utils {
	val format = new java.text.SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSS")
  def TimeConvert(dateStr: String): Long = {
    try {
      format.parse(dateStr).getTime()
    } catch {
      case _ => -1
    }
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
}