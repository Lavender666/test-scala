package scala_test

object BlockExpressionTest {
  def main(args: Array[String]): Unit = {
//    val x = 1
//    val result = {
//      if(x < 0) 0
//      else if (x >= 1) 1
//      else -1
//    }
//    println(result)
//
//    val result2 = {
//      if (x < 0){
//        -1
//      } else if(x >= 1) {
//        1
//      } else {
//        "error"
//      }
//
//    }
//    println(result2)


  var result: Unit = {
//  var result = {
    var a=0
    var b=1
    var c=3
    if (a < 10) {
      b = b + 1
      c = c + 1
      c
    }
  }
  println(result)

 }
}
