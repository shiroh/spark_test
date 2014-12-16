package citi.test.processor

import org.apache.spark.rdd.RDD

import citi_test.Chain

//TODO to check whether there is a better way to do the actor connection 
class FIXProcessor extends Processor {

  def act {
    try {
      while (true) {
        receive {
          case Chain(data, c, pub) => {
            if (data.isInstanceOf[RDD[Option[Array[(String, String)]]]]) {
              var pack = processInputDStream(data.asInstanceOf[RDD[Option[Array[(String, String)]]]])
              pack match {
                case p: Some[FIXPacket] =>
                  c ! new Chain(p.get, pub); println(p.get.toString)
                case None => Unit
              }
            } else
              println("error inputDStream")
          }
        }
      }
    } catch {
      case ex:Throwable => println("exception caught:" + ex)
    }
  }

  def processInputDStream(input: RDD[Option[Array[(String, String)]]]): Option[FIXPacket] = {
    var pack = FIXPacket(input)
    if (pack.isBroken)
      None
    else
      Some(pack)
  }
}

