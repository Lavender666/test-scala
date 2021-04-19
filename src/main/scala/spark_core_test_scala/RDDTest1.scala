package spark_core_test_scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTest1 {

  def main(args: Array[String]): Unit = {


    val list = List(("a", 1), ("a", 1), ("a", 2), ("b", 2), ("c", 1), ("c", 4))

    val sparkConf = new SparkConf().setAppName("CreateRDDTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)


    val rdd1: RDD[(String, Int)] = sparkContext.makeRDD(list)
    rdd1.groupByKey().foreach(x => println(x))
    rdd1.reduceByKey(_+_).foreach(x => println(x))

    // 统计每个key的总value个数， 和总和
    //  三个参数：
    /**
      * Int => C
      * (C,Int) => C
      * (C, C) => C
      *
      * C = (总和，总个数)
      */
//    rdd1.combineByKey(x => (x, 1),  (c:(Int, Int), b) => (c._1 + b, c._2 + 1), (aa, bb) => (aa._1 + bb._1, aa_2 + b._2))
//    rdd1.combineByKey[(Int, Int)](x:Int => (1,x), )
    val result345: RDD[(String, (Int, Int))] = rdd1.combineByKey((x:Int) => (x, 1), (c, x) => (c._1 + x, c._2 + 1), (aa, bb) => (aa._1 + bb._1, aa._2 + bb._2))

    /**
      * List(("a", 1), ("a", 1), ("a", 2), ("c", 1), ("c", 4), ("b", 2))
      *
      * a,(4,3)
      * b,(2,1)
      * c,(5,2)
      *
      * (a,(4,3))
      * (b,(2,1))
      * (c,(5,2))
      */
    result345.foreach(println)


    /**
      * (a,(1,1,2))
      * (b,(2))
      * (c,(1,4))
      */
    rdd1.groupByKey().foreach(println)

    /**
      * (a,4)
      * (b,2)
      * (c,5)
      */
    rdd1.reduceByKey((a:Int, b:Int) => a + b).foreach(println)
  }

}
