package scala_test

object ArrayTest {
  def main(args: Array[String]): Unit = {
////    创建一个长度为8的数组
//    val arr1 = new Array[Int](8)
////    直接打印的是数组的哈希地址
//    println(arr1)
////    转化成ArrayBuffer之后，打印就能看见数组里面的内容，可能是ArrayBuffer重写了toString方法
//    println(arr1.toBuffer)
//    for(i <- 0 to 7)
//      println(arr1(i))
//
////    创建数组并且指定具体内容（加new和不加new的区别），自动调用了apply方法
//    val arr2 = Array[Int](10)
//    val arr3 = Array[Int](10, 8)
//    println(arr2.toBuffer)
////    toBuffer之后原数组不变
//    println(arr2)
//    println(arr3.toBuffer)
//
////    不可变数组声明为val的时候不能进行追加数组或者元素
////    arr2++=arr3
//
////    不可变数组声明为var的时候可以进行追加数组或者元素，此时相当于原来的test变量重新指向了一个新创建的对象，
////    因为是var，所以可以指向重新创建的对像
//    var test = Array[Int](4)
//    var test1 = Array[Int](4)
//    test++=test1
////    不知道下面这条语句为什么通不过编译，改成test+="1"能通过编译但是执行报错
////    test+=1
//    println(test.toBuffer)
//
////    可以自己识别元素的类型
//    val arr4 = Array("hadoop", "storm", "spark")
//    println(arr4.toBuffer)
//
////  val的不可变数组，可以进行类似于filter、map等的操作，创建了一个新的数组，原始的没有改变
//    arr2.filter(_%2!=0)
//    println(arr2.toBuffer)
//
//    println(arr3.sum)
//
//    val arr5 = Array(3,5,2,6,7,4,9)
//    println(arr5.sorted.toBuffer)
//    println("====================")
////    自定义排序规则，前者大于后者，相当于降序
//    println(arr5.sortWith((x, y) => (x > y)).toBuffer)
//    println(arr5.sortWith(_>_).toBuffer)
//    println("====================")
////    自定义排序规则，前者小于后者，相当于升序
//    println(arr5.sortWith((x, y) => (x < y)).toBuffer)
//    println(arr5.sortWith(_<_).toBuffer)
//    println("====================")
//
//    println(arr5.sortBy(x => x).toBuffer)

//   =======================分割线======================

//    不可变数组
      val test = Array(1,2,5,6,7)
      println(test)

//    数组没有::用法，集合有
//      0::test
//    数组有:+，+:用法，用于向原数组添加元素，+号紧挨着元素。原始数组变量不变，指向对象的起始地址也不变，对象本身也不变，只是重新创建了一个新的数组，需要用新的变量承接
      test:+0
      0+:test
      println(test)

      val test2 = Array(8,9)
//    数组有++:用法，用于在原始数组后面加入另外一个数组然后生成一个新的数组（没有:++）。指向对象的起始地址也不变，对象本身也不变，只是重新创建了一个新的数组，需要用新的变量承接
      test++:test2
//      test:++test2
      println(test)

//    将两个数组合并生成一个新的数组。指向对象的起始地址也不变，对象本身也不变，只是重新创建了一个新的数组，需要用新的变量承接
      test++test2
      println(test)

//      数组没有:::用法，集合有
//      test:::test2

//    不可变数组声明成val时候时没有++=用法，声明称var时可以使用（因为变量所指的起始地址可以改变），因为调用++=方法的数组变量指向的对象会有所变化，对于不可变数组来说相当于重新创建了一个新的对象然后让原先的变量指向它，最开始的对象是不变的
//      test++=test2
//      val test3 = test++=test2
      var test4 = Array(1,2,3,4,5)
      println(test4)
      val test5 = Array(6,7)
//    声明称var的不可变数组，可以使用+=、++=，结果是原始的test4变量所指向的起始地址变了，相当于新创建了个数组，原始地址对应的对象没有变
      test4++=test5
      println(test4)
//    为什么不能用+=？？？
//      test4+=8
//      println(test4)
      println(test4.toBuffer)




  }
}
