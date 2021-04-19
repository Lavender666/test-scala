package spark_core_test_scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("CreateRDDTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val list1 = List(("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6))
    val list2 = List(("a", 11), ("b", 22), ("c", 33))

    val list3 = List("a", "b", "c")
    val list4 = List(1,2,3)

    /**
      * a, (11, 1)
      * a, (11, 2)
      * a, (11, 3)
      */

    val rdd1 = sparkContext.makeRDD(list1)
    val rdd2 = sparkContext.makeRDD(list2)
    val rdd3 = sparkContext.makeRDD(list3)
    val rdd4 = sparkContext.makeRDD(list4)

    /**
      * (a,(1,11))
      * (a,(2,11))
      * (a,(3,11))
      * (b,(4,22))
      * (c,(5,33))
      * (c,(6,33))
      */
//    val joinResultRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//    joinResultRDD.foreach(println)


    /**
      * (a, ([1,2,3], [11]))
      * (b, ([4],     [22]))
      * (c, ([5,6],  [33]))
      */
//    val coGroupResultRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
//    coGroupResultRDD.foreach(println)


    rdd3.cartesian(rdd4).foreach(println)
    val resultValue1: Int = rdd4.reduce(_+_)
    println(resultValue1)
  }
}
