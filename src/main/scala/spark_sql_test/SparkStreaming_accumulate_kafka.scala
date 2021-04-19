package spark_sql_test

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object SparkStreaming_accumulate_kafka {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("updateStateByKey")
    val ssc = new StreamingContext(conf, Seconds(1))
    //设置日志级别，打印日志比较简洁，便于查看程序打印结果
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //checkpoint是程序能都累加的前提，使用updateStateByKey的前提要设定checkpoint目录
    ssc.checkpoint("hdfs://localhost:9000/kafka")
    val topics = Set("topic_a")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    val wordOneDstream = kafkaStream.flatMap(_.toString.split(",")).map((_, 1))
//    必须指定values和state的类型，才能确定调用哪个重载方法，否则会报错
    val wordCountDStream = wordOneDstream.updateStateByKey((values: Seq[Int], state: Option[Int]) => Some(values.sum + state.getOrElse(0)))
//    val wordCountDStream = wordOneDstream.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
//      val currentCount = values.sum
//      val lastCount = state.getOrElse(0)
//      Some(currentCount + lastCount)
//    })

//  输入a,b,c打印结果类似于下面的内容
    //-------------------------------------------
    //Time: 1604393401000 ms
    //-------------------------------------------
    //(ConsumerRecord(topic = topic_a,1)
    //(b,1)
    //(c),1)
    //( serialized value size = 5,1)
    //( CreateTime = 1604393399793,1)
    //( offset = 16,1)
    //( checksum = 3140226542,1)
    //( partition = 0,1)
    //( key = null,1)
    //( value = a,1)
    //...
    wordCountDStream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
