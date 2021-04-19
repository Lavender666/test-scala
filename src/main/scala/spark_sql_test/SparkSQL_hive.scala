package spark_sql_test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

object SparkSQL_hive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("StructTypeSchema")
      .master("local[*]")
      .config("fs.defaultFS", "hdfs://localhost:9000/")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show databases;").show()
  }

}
