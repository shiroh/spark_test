package citi_test

trait FIXDecoder {
  //Note RDD will be serialized when map, flatmap called.
  //make decode to be a function instead of a method(function are objects in scala), so that spark will be able to serialize it
  //Also remember put the SOH into the function body , otherwise the whole FIXDecoder class will be serialized
  
  //Alternative way is to serialize the whole class which contian the RDD.map methond
  val decode = (str: String) => {
    val SOH = " "
    try {
      var fields = str.split(SOH)
      val pairArray = for (field <- fields) yield {
        val pair = field.split("=")
        (pair(0), pair(1))
      }
      Some(pairArray)
    } catch {
      case _ => None
    }
  }
}