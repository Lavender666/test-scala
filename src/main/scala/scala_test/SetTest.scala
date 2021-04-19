package scala_test

import scala.collection.mutable
import scala.collection.immutable

object SetTest {
  def main(args: Array[String]): Unit = {
//    不可变
    val set1 = new immutable.HashSet[Int]()
    //将元素和set1合并生成一个新的set，原有set不变
    val set2 = set1 + 4
    //set中元素不能重复
    val set3 = set1 ++ Set(5, 6, 7)
    val set0 = Set(1,3,4) ++ set1
    println(set0.getClass)

//    可变
    val set11 = new mutable.HashSet[Int]()
    //向HashSet中添加元素
    set11 += 2
    //add等价于+=
    set11.add(4)
    set11 ++= Set(1,3,5)
    println(set11)
    //删除一个元素
    set11 -= 5
    set11.remove(2)
    println(set11)
  }
}
