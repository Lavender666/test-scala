package spark_sql_test

import java.util.Properties

import org.apache.log4j.lf5.viewer.configure.ConfigurationManager
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import scala.collection.mutable

object SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
//    本地模式启动spark-shell并加载mysql驱动
//    spark-shell \
//    --jars $SPARK_HOME/mysql-connector-java-5.1.7-bin.jar \
//    --driver-class-path $SPARK_HOME/mysql-connector-java-5.1.7-bin.jar
//    使用JDBC从mysql中读取数据创建DataFrame
//    val jdbcDF = spark.sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/test").option("user", "root").option("dbtable", "student").load()
//    val jdbcDF = spark.sqlContext.read.format("jdbc").options(Map("url" ->"jdbc:mysql://localhost:3306/test", "driver" -> "com.mysql.jdbc.Driver", "dbtable" ->"student")).load()

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

//      以下三种方式都不报错，但是只有第一种真正执行成功了，在数据库中新建了一个表
//      方式一：
        //创建 Properties 存储数据库相关属性
        val prop = new Properties()
        prop.put("user", "root")
        //将数据追加到数据库，表可以不存在
        studentDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/test", "student4", prop)

    //    方式二：
//    val map = new mutable.HashMap[String, String]()
//    map.put("url", "jdbc:mysql://localhost:3306/test")
//    map.put("user", "root")
    //    map.put("password","xxx")
//    map.put("dbtable", "student4")
//    studentDF.write.mode("append").format("jdbc").options(
//      map
//    )

    //    方式三：
//    studentDF.write.mode(SaveMode.Overwrite).format("jdbc").option("url", "jdbc:mysql://localhost:3306/test")
//      .option("user", "root")
//      .option("dbtable", "student4")

//        spark.stop()

  }

}
