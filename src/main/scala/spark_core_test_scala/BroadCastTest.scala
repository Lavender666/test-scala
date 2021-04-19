package spark_core_test_scala

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadCastTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("BroadCastTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val list1 = List(("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6))
    val list2 = List(("a", 11), ("b", 22), ("c", 33))


    // 需求：给当前这个RDD中的所有key-value中的value进行加 100 的操作
    val rdd1: RDD[(String, Int)] = sparkContext.makeRDD(list1)


    /**
      * 这个 newNumber 是定义在  driver 程序中
      *
      * 第一次定义的 newNumber就是一个单个的值，所以这个值在task资源的封装包中，根本就不占什么大小。
      * 所以每一个task都自己复制一份，完全OK
      *
      * 第二次定义的 clazz 对象，大小很大， 假如是100M
      * 那么网络传输的压力，就大了， 这种在driver中定义的变量，被复制副本传送到taks中去的时候，还是存储在内存中的
      *
      * 关键的问题：
      *   一个exeuctor会初始化一个线程池
      *   一个线程池可以同时并发运行多个task
      *   那么这个变量又很大，而且每个task都维持一份，
      *   而且每个task还都是在一个JVM进程（executor）中执行的
      *
      *  优化：
      *       使用Broadcast组件对定义在driver中，但是又需要应用在executor中的task中的变量
      *       进行全局广播： 让所有的executor都只维持一个数据副本，而不是每个task维持一个数据
      *
      *       这种优化，尤其当这个要共享的变量很大的时候，特别管用
      */
    var newNumber:Int = 100
    var clazz:Array[Student] = new Array[Student](10000)

    case class Student(name:String, age:Int)

    // 实现了加100的操作
    /**
      * 这个变量被使用在：  x => (x._1, x._2 + newNumber)
      * 这个代码是运行在：  每一个executor中的每一个task中的
      *
      * driver和executor是在同一个节点么？  不一定
      *     如果有多个节点要去执行这个app应用程序的task/executor的话，那么就一定有跟driver不在同一个节点的executor存在
      *
      * 发现了一个问题：
      *     newNumber 定义在 driver中
      *     但是使用在了 executor中
      *
      *     在进行任务分发的时候，就能解析出来这个全局变量是要被所有的task使用的
      *     所以在分发task的时候，也同时会把这个变量都复制出来一个副本，传输给每一个task
      *
      *     一个可以独立运行的task仅仅只包含代码么？
      *       还有很多的其他的资源
      *         全局变量
      *         其他的各种组件
      *         启动脚本
      *         task的配置信息文件
      *         ....
      */
      // 当前这种代码的编写是没有这种优化： 底层实现上，还是每一个task都维持一个数据副本都在内存中
    val newRDD: RDD[(String, Int)] = rdd1.map(x => (x._1, x._2 + newNumber))



    /**  // 优化：  使用广播变量
      // Broadcast 天生就是已经被优化了能放在网络上进行高效传输的对象
      // 执行的效果：
      * 在哪些节点上运行的有当前这个app应用程序的executor,
      * 那么这个bc组件，就会帮我们把这个广播变量newNumber分发到所有的executor里面
      *
      * 而不是分给task
      * 不是每个task都维持一个数据副本，而是一个executor维持一个数据副本
      */
    val bc: Broadcast[Int] = sparkContext.broadcast(newNumber)
    val newRDD1: RDD[(String, Int)] = rdd1.map(x => {

      val bcNumber = bc.value
      (x._1, x._2 + bcNumber)

    })

    newRDD1.foreach(x => println(x))


    sparkContext.stop()

  }
}
