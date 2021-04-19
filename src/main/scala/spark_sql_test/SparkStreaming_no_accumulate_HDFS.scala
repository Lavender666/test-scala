package spark_sql_test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_no_accumulate_HDFS {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("ss")
    val ssc = new StreamingContext(conf,Seconds(1))
//  这里监控的是目录，不会从目录中读取旧文件，因此一旦开始作业，就需要不断将输入文件手动复制到hdfs目录中
//  如果传进去的参数是个文件，就会报错
    val fileStream = ssc.textFileStream("hdfs://localhost:9000/hdfs/")
    val key = fileStream.flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)

    key.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
