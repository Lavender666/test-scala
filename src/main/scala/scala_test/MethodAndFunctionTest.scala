package scala_test

object MethodAndFunctionTest {
  def m1(f: (Int, Int) => Int): Int={
    f(2, 6)
  }

  val f1 = (x: Int, y: Int) => x+y
  val f2 = (m: Int, n: Int) => m*n

  val f3 = m1 _

  def main(args: Array[String]): Unit = {
    var t1 = m1(f1)
    var t2 = m1(f2)
    for(i <- Array(t1, t2))
     println(i)
    println(f3)
  }
}
