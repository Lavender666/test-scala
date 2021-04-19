package spark_sql_test

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrame_test2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("StructTypeSchema")
      .master("local[*]")
      .getOrCreate()

    val lineRDD = spark.sparkContext.textFile("hdfs://localhost:9000/student/student.txt").map(_.split(","))
//  schema是IntegerType类型的话，Row的相应字段要转化成int，否则会报错
    val rowRDD = lineRDD.map(x => Row(x(0).toInt, x(1), x(2), x(3).toInt, x(4)))
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("sex", StringType, true),
        StructField("age", IntegerType, true),
        StructField("department", StringType, true)
      )
    )
    val studentDF = spark.sqlContext.createDataFrame(rowRDD, schema)
    studentDF.show()
    studentDF.createOrReplaceTempView("student")
    spark.sqlContext.sql("select * from student").show()
    spark.stop()
  }
}
