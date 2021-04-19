package spark_core_test_scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTest2 {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("CreateRDDTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)



    val array = Array(1,2,3,4,5)
    val list = List(("a", 1), ("a", 1), ("a", 2), ("b", 2), ("c", 1), ("c", 4))



    val rdd1 = sparkContext.makeRDD(array)
    val rdd2 = sparkContext.makeRDD(list)



    val value = rdd1.reduce(_+_)


    /**
      * 三个参数：
      * 1、第一个参数：  U: zeroValue  初始状态
      * 2、第二个参数：  U，Int => U     状态和值进行合并的函数
      * 3、第三个参数：  U，U => U      每个分区中的状态最终合并
      */
    val result: RDD[(String, Int)] = rdd2.aggregateByKey(0)((u, value) => u + value, (u1, u2) => u1 + u2)

    /**
      * (a,4)
      * (b,2)
      * (c,5)
      */
//    result.foreach(println)


    /**
      * 需求：请利用： aggregateByKey求的每个key的所有的value的平均值
      */
    val result2RDD: RDD[(String, (Int, Int))] = rdd2.aggregateByKey((0, 0))((u, score) => (u._1 + score, u._2 + 1),
      (u1, u2) => (u1._1 + u2._1, u1._2 + u2._2))

    val result3RDD = result2RDD.map((x: (String, (Int, Int))) =>
      (x._1, (x._2._1.toDouble / x._2._2))
    )

    /**
      * (a,1.3333333333333333)
      * (b,2.0)
      * (c,2.5)
      */
//    result3RDD.foreach(x => println(x))


    /**
      * 对 result3RDD  结果进行排序
      */
//    result3RDD.sortByKey()  // 直接按照key进行默认规则的排序
//    result3RDD.map(x => (x._2, x._1)).sortByKey().map(x => (x._2, x._1))
    result3RDD.sortBy(x => x._2, false).foreach(x => println(x))

    /**
      * sorted
      * sortBy  按照指定的某个属性去排序
      * sortWith  按照指定的规则去排序
      *     排序规则： MyComparator implements Comaprable<Student>{compre(T o1,  T o2){}}
      */




    sparkContext.stop()
  }
}
