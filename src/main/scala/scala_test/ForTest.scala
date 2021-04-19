package scala_test

object ForTest {
  def main(args: Array[String]): Unit = {
    //    Java: for(int i = 1; i <= 10; i++){
    //
    //     }

//    for (i <- 1 to 10)
//      println(i)
//
//
//    var arr = Array("a", "b", "c")
//    for (i <- arr)
//      println(i)

//    for (i <- 1 to 3; j <- 1 to 3 if i != j)
//      println(i + " " + j)

    val v = for(i <- 1 to 10)yield i*10
    println(v)
  }

}
