package spark_sql_test

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_accumulate_HA_kafka {

  val checkpointDirectory="hdfs://localhost:9000/kafka_streaming_checkpoint"

  def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    conf.set("dfs.replication", "1")
    System.out.println(conf.get("dfs.replication"))
    val ssc = new StreamingContext(conf,Seconds(1))
    //设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    ssc.checkpoint(checkpointDirectory)
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
    val wordCountDStream = wordOneDstream.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val lastCount = state.getOrElse(0)
      Some(currentCount + lastCount)
    })

    wordCountDStream.print()

    ssc
  }


  def main(args: Array[String]): Unit = {
    //    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
