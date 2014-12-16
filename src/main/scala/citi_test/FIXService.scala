package citi_test

import org.apache.spark.Logging
import org.apache.log4j.{ Level, Logger }
import scala.actors.Actor._

object FIXService {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
      var spotConsumer: Consumer = new SocketConsumer
      spotConsumer.start()
  }
}

