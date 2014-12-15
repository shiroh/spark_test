package citi.test.processor

import scala.actors.Actor
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import scala.collection.mutable.HashMap

//TODO to check whether there is a better way to do the actor connection 
class FIXDecoder extends Actor {
  var constructor = new Constructor
  constructor.start

  def act {
    while (true) {
      receive {
        case input: ReceiverInputDStream[String] => {
          var pack = processInputDStream(input)
          pack match {
            case p: Some[FIXPackage] => constructor ! p.get
            case None => println("error")
          }

        }
        case _ => println("error")
      }
    }
  }

  def processInputDStream(input: ReceiverInputDStream[String]): Option[FIXPackage] = {
    var pack = FIXPackage(input)
    if (pack.isBroken)
      None
    else
      Some(pack)
  }
}

