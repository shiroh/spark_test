package citi_test

import scala.actors.Actor
import scala.actors.TIMEOUT
import com.twitter.util.Time

class Timer(monitor: Actor) extends Actor {
  var curMin = Time.now.inMinutes
  def act {
    loop {
      reactWithin(1000) {
        case TIMEOUT => if (onMinute) monitor ! true
      }
    }
  }
  def onMinute() = {
    var min = Time.now.inMinutes
    var sec = Time.now.inSeconds

    if (sec % 60 == 0 && min != curMin) {
      curMin = min
      true
    } else {
      false
    }
  }
}