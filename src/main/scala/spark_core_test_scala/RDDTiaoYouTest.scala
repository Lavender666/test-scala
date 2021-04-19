package spark_core_test_scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTiaoYouTest {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("BroadCastTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val list1 = List(("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6))
    val list2 = List(("a", 11), ("b", 22), ("c", 33))


    val rdd1: RDD[(String, Int)] = sparkContext.makeRDD(list1)
    // 做了各种一系列的操作

    /**
      * 上下两句代码都是创建，基于同一份数据的RDD
      * 但是由于相隔了300行代码
      */

    // 下一次要做某个操作的时候，发现又要用到LIST1，结果又创建了一个RDD
    val rdd2: RDD[(String, Int)] = sparkContext.makeRDD(list1)


    //  如果把这种创建RDD的方式修改成使用textFile去读取磁盘文件。
//    rdd1在计算的时候，读取了一次
//    rdd2在计算的时候，也会再重新读取一次

  }
}
