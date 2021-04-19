package spark_sql_test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_reduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("s").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(1))
    val textStream = ssc.socketTextStream("localhost",9999)
    val wordCountDStream = textStream.flatMap(_.split(",")).map((_,1))
    //每隔两秒打印前三秒的数据
    val windowDStream = wordCountDStream.reduceByKeyAndWindow((x:Int,y:Int) => x+y, Seconds(3), Seconds(2))

    windowDStream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
