package spark_core_test_scala

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("BroadCastTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val list1 = List(("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6))
    val list2 = List(("a", 11), ("b", 22), ("c", 33))

    val rdd1: RDD[(String, Int)] = sparkContext.makeRDD(list1)


    /**
      * 需求：
      *     想统计所有的value的和是多少
      *
      * 一个app应用程序，是由一个driver和多个executor组成
      *
      * 一个executor又并发运行了多个task
      *
      * 这些task其实都是并发运行的
      */
    var countNumber:Int = 0
    rdd1.foreach((x:(String,Int)) => {
      countNumber += x._2
    })
    println(countNumber)    // 结果还是0
    // 如果代码这么写，只有BroadCast组件会帮助我们把countNumber这个变量的副本分发给所有的task
    // 不会再task做完了countNumber的计算之后传回来


    /**
      * 优化措施：
      *     使用累加器去实现
      *     全局累加器
      *
      *     性能监控
      *     统计
      *     ....
      */
    val accmulator: LongAccumulator = sparkContext.longAccumulator("count")
    rdd1.foreach((x:(String,Int)) => {


      /**
        * 第一：
        *     把value的值，累加到了累加器之上
        * 第二：
        *     累加器还保证了这个值，在任何地方被使用的时候，都是最新的计算结果
        */
      accmulator.add(x._2)


//      val newValue = accmulator.value
      //      val newResult = x._2 + newValue
      //      accmulator.reset()  // 累加器的值：0
      //      accmulator.value = newResult
//
    })

//    println(countNumber)
    println(accmulator.value)

    sparkContext.stop()
  }
}
