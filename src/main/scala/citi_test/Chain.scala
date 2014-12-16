package citi_test

import citi.test.processor.Constructor
import citi.test.processor.FIXConstructor
import citi.test.processor.Constructor

class Chain[A <: Any](val data: A, val c: Constructor, val pub: Publisher) {

  def this(data: A, pub: Publisher) {
    this(data, null, pub)
  }

}
object Chain {
  def unapply[A <% Any](c: Chain[A]) = {
    if (c == null) None else Some((c.data, c.c, c.pub))
  }
}

object test {
  def main(args: Array[String]) {
    val x = new Chain("hello", new FIXConstructor, new StdoutPublisher)
    x match {
      case Chain(a, b, c) => {
        println(a)
        println(b.isInstanceOf[Constructor])
      }
    }
  }
}
