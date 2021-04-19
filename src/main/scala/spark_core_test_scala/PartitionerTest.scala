package spark_core_test_scala

import java.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object PartitionerTest {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("BroadCastTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val list1 = List(("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
      ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
      ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
        ("d",2),("d",2),("d",2),("d",2),("d",2),
      ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
      ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
      ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
      ("d",2),("d",2),("d",2),("d",2),("d",2),
      ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
      ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
      ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
      ("d",2),("d",2),("d",2),("d",2),("d",2),
      ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
      ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
      ("a", 1), ("a", 2), ("a", 3), ("b", 4), ("c", 5), ("c", 6),
      ("d",2),("d",2),("d",2),("d",2),("d",2)
    )


    // 需求：给当前这个RDD中的所有key-value中的value进行加 100 的操作
    val rdd1: RDD[(String, Int)] = sparkContext.makeRDD(list1)


    val rdd2: RDD[(String, Int)] = rdd1.partitionBy(new RandomPartitioner(3))


    // index:Int : 分区编号
    // data:Iterator[(String, Int)]  当前这个分区的数据的迭代器
    rdd2.mapPartitionsWithIndex((index, data) => {

      val lastResult = new ArrayBuffer[Int]()
      println(index)
      var count = 0
      val values: Iterator[(String, Int)] = data
      for(xx <- values){
        println(xx)
        count += 1
        lastResult.append(xx._2 + 1)
      }

      println(count)
      lastResult.toIterator
    }).collect()
  }
}


class MyPartitioner(val ptnNumber:Int) extends Partitioner{

  // 指定分区的数量
  // 5 :   0 1 2 3 4
  override def numPartitions: Int = ptnNumber

  // 决定一个元素在哪个分区
  // 根据key决定的
  // getPartition的返回结果的取值范围： 0 to (numPartitions - 1)
  //                                  0 until numPartitions
  override def getPartition(key: Any): Int = {

    ( key.hashCode()  & Integer.MAX_VALUE) % numPartitions

  }
}

class RandomPartitioner(val ptnNumber:Int) extends Partitioner{

  // 指定分区的数量
  // 5 :   0 1 2 3 4
  override def numPartitions: Int = ptnNumber

  val r = new Random()

  // 决定一个元素在哪个分区
  // 根据key决定的
  // getPartition的返回结果的取值范围： 0 to (numPartitions - 1)
  //                                  0 until numPartitions
  override def getPartition(key: Any): Int = {

    r.nextInt(numPartitions)

  }
}