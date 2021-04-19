package spark_core_test_scala

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个 SparkConf 对象，并设置程序的名称
    val conf = new SparkConf().setAppName("WordCount")
    conf.set("fs.defaultFS", "hdfs://localhost:9000/")
//  使用spark-submit提交到集群上必须设置--master，本地模式也要设置master，否则会报错
    conf.setMaster("local[*]")
//    conf.setJars(Array("/Users/lavender/test-scala/target/test-scala-1.0-SNAPSHOT.jar"))
    println(conf.get("fs.defaultFS"))
    // 创建一个 SparkContext 对象
    val sc = new SparkContext(conf)
    // 读取 HDFS 上的文件构建一个 RDD
//    val fileRDD = sc.textFile(args(0))
    val fileRDD = sc.textFile("hdfs://localhost:9000/wordcount.txt")
    // 构建一个单词 RDD
    val wordAndOneRDD = fileRDD.flatMap(_.split(" ")).map((_, 1))
    // 进行单词的聚合
    val resultRDD = wordAndOneRDD.reduceByKey(_+_)
    // 对 resultRDD 进行单词出现次数的降序排序，然后写出结果到 HDFS
//  参数从0开始，不是从1开始
//    resultRDD.sortBy(_._2, false).saveAsTextFile(args(0))
    //  地址必须写全，hdfs前缀要加上
    resultRDD.sortBy(_._2, false).saveAsTextFile("hdfs://localhost:9000/wordcount")
//    resultRDD.sortBy(_._2, false).foreach(println(_))
    println("==================================================")
    sc.stop()
  }

}
