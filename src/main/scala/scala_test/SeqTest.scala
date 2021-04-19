package scala_test

import scala.collection.mutable.ListBuffer

object SeqTest {
  def main(args: Array[String]): Unit = {
//    val list1 =List(1, 2, 3)
//    val list2 = 0::list1
//      println(list2)
//    val list4 = list1.::(4)
//    println(list4)
//    println(list1)
//    println(list2)
//    println("=============")
//    val list3 = 0+:list1
//    println(list3)
//    println("=============")
//    val list4 = list1.+:(0)
//    println(list4)
//    println("=============")
//    val list5 = list1.::(0)
//    println(list5)
//    println(list1)
//    println("=============")
//    val list6 = list1:+4
//    println(list6)
//    println("=============")
//    val list7 = list1.:+(4)
//    println(list7)
//    println("=============")
//    val list0 = List(4,5,6)
//    val list8 = list1++list0
//    println(list8)
//    println("=============")
//    val list9= list1++:list0
//    println(list9)
//    println("=============")
//    val list10= list1:::list0
//    println(list10)
//    println("=============")
//    val list11= list0:::list1
//    println(list11)
//    println("=============")
//    val list12= list0.:::(list1)
//    println(list12)
//    println("=============")

    //构建一个可变列表，初始有3个元素1,2,3
    val lst0 = ListBuffer[Int](1, 2, 3)
//    创建一个空的可变列表
    val lst1 = new ListBuffer[Int]
//
////    可变列表不能用::，可以用+:、:+和++:，但是原列表没有改变，需要重新复制给一个新的变量
//      0::lst1
//    lst1.::(0)
//    val test = 0+:lst1
//    println(test)
//    lst1:+0
//    lst0++:lst1
//    println(lst0)
//    println(lst1)
//
////  可变列表有+=、++=，在列表中追加元素，直接改变原列表的值(原列表指调用该方法的列表)，没有生成新的列表
//    lst0+=4
//    println(lst0)
//    lst0++=lst1
//    println(lst0)
//
////  会生成新的列表，需要赋值到新的变量上
//    val lst3 = ListBuffer(5, 6, 7)
//    lst0++lst3
//    println(lst0)
//    println(lst3)

//构建一个不可变列表，初始有3个元素1,2,3
//var lst0 = List[Int](1, 2, 3)
//不能创建一个空的不可变列表
//    val lst1 = new List[Int]

//  不可变列表可以用::，可以用+:、:+（数字仅挨+号）和++:，但是原列表没有改变，需要重新复制给一个新的变量
//    val test = 0::lst0
//    println(test)
//    lst0.::(0)
//    0+:lst0
//    lst0:+0
//    val lst1 = List(4)
//    lst0++:lst1
//    println(lst0)
//    println(lst1)

//    不可变列表声明成val的时候没有++=，但是声明为var的时候有（因为变量所指的起始地址可以改变），因为调用++=方法的数组变量指向的对象会有所变化，对于不可变数组来说相当于重新创建了一个新的对象然后让原先的变量指向它，最开始的对象是不变的
//    lst0++=lst1
//    println(lst0)

//    和不可变数组一样，不能用+=
//    lst0+=4
//    println(lst0)

//  不可变列表有++方法，会生成新的列表，需要赋值到新的变量上，原来的列表不变
//    val lst3 = List(5, 6, 7)
//    lst0++lst3
//    println(lst0)
//    println(lst3)






  }
}
