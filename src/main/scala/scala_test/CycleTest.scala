package scala_test

object CycleTest {
  def main(args: Array[String]): Unit = {
    var n = 10
    while(n > 0){
      println(n)
      n-=1
    }
  }



}
