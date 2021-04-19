package scala_test

import scala.collection.mutable.ArrayBuffer

object ArrayBufferTest {
  def main(args: Array[String]): Unit = {
////    创建一个长度为8的可变数组，这里的可变数组指的是长度可以改变，注意与val和var的区别
////    val类似于和Java中的final相同，修饰的变量其实地址不能变，也即不能指向重新创建的一个新的对象
//    val arr1 = new ArrayBuffer[Int](8)
//    println(arr1)
//    println(arr1)
//    //    for(i <- 0 to 7)
//    //      println(arr1(i))
//
//    val arr2 = ArrayBuffer[Int](10)
//    val arr3 = ArrayBuffer[Int](10, 8)
////    println(arr2)
////    println(arr3)
//
////    对两个数组进行拼接，用++=，+=只能追加元素
//    val arr4 = arr2++=arr3
////    此时arr2已经改变了，而arr3不变，虽然对拼接结果重新赋值了，但是原来的数组变量也改变了，可能是因为arr2是可变数组
//    println(arr2)
//    println(arr3)
////    用+=追加元素
//    arr4+=1
//
//    arr4++=arr3
//    println(arr4)
//
//    val arr5 = ArrayBuffer("hadoop", "storm", "spark")
//    println(arr5)




//    可变数组
      val test = ArrayBuffer(1,2,5,6,7)
      println(test)

//    数组没有::用法，集合有
//    0::test
//    数组有:+，+:用法，用于向原数组添加元素，+号紧挨着元素。原始数组变量不变，指向对象的起始地址也不变，对象本身也不变，只是重新创建了一个新的数组，需要用新的变量承接
      test:+0
      0+:test
      println(test)

      val test2 = ArrayBuffer(8,9)
//    数组有++:用法，用于在原始数组后面加入另外一个数组然后生成一个新的数组（没有:++）。指向对象的起始地址也不变，对象本身也不变，只是重新创建了一个新的数组，需要用新的变量承接
      test++:test2
//    test:++test2
      println(test)

//    将两个数组合并生成一个新的数组。指向对象的起始地址也不变，对象本身也不变，只是重新创建了一个新的数组，需要用新的变量承接
      test++test2
      println(test)

//    数组没有:::用法，集合有
//    test:::test2

//    可变数组不论声明成val还是var都有++=，+=用法，因为调用++=方法的数组变量指向的对象会有所变化，对于可变数组来说直接在原始对象上面改变就行了，不会创建新的对象
      test++=test2
      test+=10
      println(test)

  }

}
