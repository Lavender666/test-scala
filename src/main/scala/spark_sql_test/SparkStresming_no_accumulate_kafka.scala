package spark_sql_test

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStresming_no_accumulate_kafka {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc=new StreamingContext(conf,Seconds(2))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Set("topic_a", "topic_b")
//  起初ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)没有加[String, String]，结果一致报编译错误，各种找原因找不着
    val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)).map(_.value())
    val wordCountStream = kafkaStream.flatMap(_.toString.split(",")).map((_, 1)).reduceByKey(_+_)
    wordCountStream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}