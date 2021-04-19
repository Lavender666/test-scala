package spark_sql_test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_accumulate_9999 {
  def main(args: Array[String]): Unit = {
    /**
      * 这个地方设置的线程数至少是2，因为一个线程用来接收数据
      * 另外一个线程是用来处理数据的。
      * 如果你只写了一个线程，也不报错，只不过光是接收数据，不处理数据。
      */
    val conf = new SparkConf().setMaster("local[2]").setAppName("updateStateByKey")
    val ssc = new StreamingContext(conf, Seconds(1))
    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //checkpoint保证程序由于故障停止之后下次接着开始计数，而不是从新计数
    ssc.checkpoint("hdfs://localhost:9000/streaming")
    //通过监听一个端口得到一个DStream流    数据的输入
    val DStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordOneDstream = DStream.flatMap(_.split(","))
      .map((_, 1))
    /**
      * updateFunc: (Seq[V], Option[S]) => Option[S]
      * scala功底：  k,v  k：String  每一个单词  v:int 出现的次数
      * 看样子是需要我们传进入两个参数，还要有返回值
      * 参数一：Seq[V]   hadoop,1 hadoop,1 haodop,1
      * 对应这个key在这个批次里面出现的次数
      * 1,1,1
      * 参数二：Option[S]
      * 代表的是这个key，上一次的中间状态  4
      *
      * 返回值：Option[S]
      * 把这个key最后的结果返回去
      *
      * Option:
      * Some
      * None
      */
    val wordCountDStream = wordOneDstream.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val lastCount = state.getOrElse(0)
      Some(currentCount + lastCount)
    })

    wordCountDStream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
  }
