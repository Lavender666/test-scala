package spark_core_test_scala

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RDDTest3 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("CreateRDDTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val list1 = List(("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6))
    val list2 = List(("a", 11), ("b", 22), ("c", 33))


    /**
      * 使用第一种方式去创建RDD：
      *       sparkContext.makeRDD(list1)
      *       sparkContext.parallelize(list1)
      */
    val rdd1: RDD[(String, Int)] = sparkContext.makeRDD(list1)
    val rdd2: RDD[(String, Int)] = sparkContext.parallelize(list2)


    /**
      * 如果不带任何参数，那么这两个方法的作用一致： 都表示把当前调用这两个RDD的数据持久化到内存中
      *
      *  这三个方法效果一模一样
      */
    rdd1.cache()
    rdd2.persist()
    rdd1.persist(StorageLevel.MEMORY_ONLY)

    /**
      * 重点关注 StorageLevel
      *
      *
      */
  }
}
