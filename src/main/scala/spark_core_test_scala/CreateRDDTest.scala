package spark_core_test_scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDDTest {

  def main(args: Array[String]): Unit = {


    val array = Array(1,2,3,4,5)
    val list = 5 :: 4 :: 3 :: 2 :: 1 :: Nil

    val sparkConf = new SparkConf().setAppName("CreateRDDTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)


    // 第一种方式：在驱动程序，通过加载scala的数组和集合得到一个RDD
    val rdd1: RDD[Int] = sparkContext.parallelize(array)
    val rdd2: RDD[Int] = sparkContext.makeRDD(list)
    val rdd11: RDD[Int] = rdd1.map(x => {
      println(x)
      x * 2
    })
    // action算子中，都包含了一个真正去触发任务执行的方法：
    // 这个方法是所有的其他的transformation的算子没有的操作
    // runJob
//    rdd11.collect()
//    填写文件夹名字
//    rdd11.saveAsTextFile("/Users/lavender/test/rdd11")
//    rdd11.foreach(x => println(x))


    // 第二种： 通过加载外部数据源得到
    val rdd3: RDD[String] = sparkContext.textFile("/Users/lavender/test/analysis_feature_important_1.txt")
//    rdd3.foreach(x => println(x))

    // x : rdd3中的每个元素，分别对某一行进行map和foreach然后再执行下一行
//    rdd3.map(x => {println(x)}).foreach(x => println(x + "============"))

    // y: 就是rdd3中的一个分区
//    rdd3.mapPartitions(y => {println(y); null})

    // iNdex: 当前遍历到的这个分区，在这个RDD中的分区编号，编号从0 开始
    rdd3.mapPartitionsWithIndex((index, y) => {println(y); null})


    val result1: RDD[String] = rdd3.sample(false, 0.5, 0)
    val value1: Array[String] = rdd3.takeSample(false, 10, 0)

    // 合并，   不是求并集
    val rdd12: RDD[Int] = rdd2.union(rdd1)






  }
}
