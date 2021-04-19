package spark_sql_test

import org.apache.spark.sql.SparkSession

object DataFrame_test1 {
//  case class 一定要事先放到外面定义好，否则会报错。case class 构造参数默认为val，也可以声明成var
//  now i found the reason, you should define case class in the object and outof the main function.
  case class Student(id:Int, name:String, sex:String, age:Int, department:String)

  def main(args: Array[String]): Unit = {
    // 构建 SparkSQL 程序的编程入口对象 SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("MyFirstSparkSQL")
//      下面这个config不要也能运行，不知道作用是什么？
//      .config("fs.defaultFS", "hdfs://localhost:9000/")
      .master("local[*]")
      .getOrCreate()

//    原来需要创建SparkContext来初始化，加上.setMaster("local[*]")就不用在run-configuration里面配置-Dspark.master=local了
//    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
//    conf.set("fs.defaultFS", "hdfs://localhost:9000/")
//    val sc = new SparkContext(conf)

//    现在SparkSession内置了一个sparkContext对象，可以直接用
    val lineRDD = spark.sparkContext.textFile("hdfs://localhost:9000/student/student.txt").map(_.split(","))
    val studentRDD = lineRDD.map(x => Student(x(0).toInt, x(1), x(2), x(3).toInt, x(4)))
    //导入隐式转换，如果不导入无法将 RDD 转换成 DataFrame，下面这两个都行，谁在下面默认用谁
    import spark.implicits._
//    import spark.sqlContext.implicits._
    //将 RDD 转换成 DataFrame
    val studentDF = studentRDD.toDF
//    下面这两种均不用隐式转换
//    val studentDF = spark.sqlContext.createDataFrame(studentRDD)
//    val studentDF = spark.createDataFrame(studentRDD)
    studentDF.show
    //注册表
//    1.x老版写法
//    studentDF.registerTempTable("t_student")
//    2.2以后才有的，但是报错不知道为啥？？
//    studentDF.createOrReplaceGlobalTempView("t_student")
//   2.x新版写法
    studentDF.createOrReplaceTempView("t_student")
    //传入 SQL
    val df = spark.sqlContext.sql("select department, count(*) as total from t_student " +
      "group by department having total > 6 order by total desc")
    //将结果以 JSON 的方式存储到指定位置
//    df.write.json("/Users/lavender/Desktop/student_test.txt")
    df.write.json("hdfs://localhost:9000/student/output")
    //停止 Spark Context
    spark.stop()
  }
}
