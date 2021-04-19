package spark_core_test_scala

import org.apache.spark.{SparkConf, SparkContext}

class SecondSortKey(val first: Int, val second: Int) extends Ordered[SecondSortKey] with Serializable {
  override def compare(that: SecondSortKey): Int = {
    if (this.first - that.first != 0) {
      this.first - that.first
    } else {
      this.second - that.second
    }
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SecondSort")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://localhost:9000/words1.txt", 1)
    val pairs = lines.map { line =>
      (
        new SecondSortKey(line.split(",")(0).toInt, line.split(",")(1).toInt),
        line)
    }
    val sortedPairs = pairs.sortByKey()
    val sortedLines = sortedPairs.map(sortedPair => sortedPair._2)

    sortedLines.foreach { sortedLine => println(sortedLine) }
  }
}