package spark_sql_test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_no_accumulate_9999 {
  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 3) {
      println(
        """Parameter Errors! Usage: <batchInterval> <hostname> <port>
          |batchInterval:   streaming作业启动的间隔频率(s)
          |hostname     :   监听的网络ip
          |port         :   监听的网络端口
        """.stripMargin)
      System.exit(-1)
    }
    val Array(batchInterval, hostname, port) = args
    /**
      * local[2]：给当前程序分配两个线程
      * local:给当前程序之分配一个线程
      * 如果在做测试的时候，使用的master为local模式，该程序的线程数必须要>=2,一个线程用于接收数据，一个线程用于处理数据。
      * 如果配置master为local，那么就只有一个线程，并且只能用来接收数据，是无法对数据进行处理的。
      * 这主要在有接收数据Receiver的时候，上述说明生效
      */
//        val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingNetcat")
//        val sc = new SparkContext(conf)
//        val ssc = new StreamingContext(sc, Seconds(batchInterval.toLong))
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SparkStreamingNetcat")
      .master("local[*]")
      .getOrCreate()

    val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(batchInterval.toLong))
    /*
        读取网络端口的数据
        在Streaming流计算中，为了后续多个算子去操作同一份数据，绝大多数的输入算子都一个默认的持久化级别的参数
        默认的持久化级别大都为MEMORY_AND_DISK_SER_2
     */
    val input: ReceiverInputDStream[String] = ssc.socketTextStream(hostname, port.toInt, StorageLevel.MEMORY_ONLY)

    //正则表达式中\s匹配任何空白字符，包括空格、制表符、换页符等等, 等价于[ \f\n\r\t\v]
    val wordDStream: DStream[String] = input.flatMap(line => line.split("\\s+"))

    val retDS: DStream[(String, Int)] = wordDStream.map((_, 1)).reduceByKey(_ + _)

    retDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
