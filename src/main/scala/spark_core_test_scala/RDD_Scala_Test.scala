package spark_core_test_scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object RDD_Scala_Test {

// Spark程序的运行分为两大类，本地模式运行和在集群上运行，在集群上运行又有常见的的三种资源模式，分别是yarn,standalone和Mesos，每种资源模式下又有两种任务调度模式，分别问client和cluster。
//Spark中本地运行模式有3种，如下
//（1）local 模式：本地单线程运行；
//（2）local[k]模式：本地K个线程运行；
//（3）local[*]模式：用本地尽可能多的线程运行。
//  单线程的时候sc.parallelize()默认1个partition，local[k]的时候默认k个默认1个partition，local[*]的时候默认和cpu核数一致；当sc.parallelize()设置了第二个参数，则以参数为准
    private val conf: SparkConf = new SparkConf().setAppName("RDD_Test").setMaster("local")
    private val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    def main(args: Array[String]): Unit = {

//        map()
//        flatMap()
//        filter()
//        distinct()
//        mapParations()
//        mapPartitionsWithIndex()
//        reduce()
//        reduceByKey()
//        groupByKey()
//        aggregateByKey()
//        sortByKey()
//        union()
//        join()
//        sample()
//        cartesian()
//        intersection()
//        coalesce()
//        repartition()
//        repartitionAndSortWithinPartitions()
//        cogroup()


//        combineByKey()
        aggregate()

    }

    def aggregate(): Unit ={
        val textRDD = sc.parallelize(List("A", "B", "C", "D", "D", "E"), 3)
        val tuple: (Int, String) = textRDD.aggregate((0, ""))(
            (acc, value) => {
                (acc._1 + 1, acc._2 + ":" + value)
            },
            (acc1, acc2) => {
                (acc1._1 + acc2._1, acc1._2 + ":" + acc2._2)
            }
        )
        println(tuple._1, tuple._2)


    }

    def combineByKey(): Unit = {
        val textRDD = sc.parallelize(List(("A", "aa"), ("B","bb"), ("B","bb"), ("B","bb"), ("B","bb"), ("B","bb"),("C","cc"),("C","cc"), ("D","dd"), ("D","dd")))
//        val textRDD = sc.parallelize(List("a","b","c","d","d","d"))

        val resultRDD: RDD[(String, (String, Int))] = textRDD.combineByKey(
//            (("A", "aa"), 1) (("B","bb"), 1)
            value => (value, 1),
            (c: (String, Int), v: String) => (c._1, c._2 + 1),
            (c: (String, Int), v: (String, Int)) => (c._1, c._2 + v._2)
        )
        resultRDD.foreach(x => {
//            println(x._1, x._2)
            println(x)
        })

//        val textRDD = sc.parallelize(List(("A", "aa"), ("B","bb"),("C","cc"),("C","cc"), ("D","dd"), ("D","dd")))
//        val combinedRDD2 = textRDD.combineByKey(
//            value => 1,
//            (c:Int, String) => (c+1),
//            (c1:Int, c2:Int) => (c1+c2)
//        ).collect.foreach(x=>{
//            println(x._1+","+x._2)
//        })

    }


    def map(): Unit = {
        val list = List("张无忌", "赵敏", "周芷若")
        val listRDD = sc.parallelize(list)
        val nameRDD = listRDD.map(name => "Hello " + name)
        nameRDD.foreach(name => println(name))
    }

    def flatMap(): Unit = {
        val list = List("张无忌 赵敏", "宋青书 周芷若")
        val listRDD = sc.parallelize(list)
        val nameRDD = listRDD.flatMap(line => line.split(" ")).map(name => "Hello " + name)
        nameRDD.foreach(name => println(name))
    }

    def distinct(): Unit = {
        val list = List(1, 1, 2, 2, 3, 3, 4, 5)
        sc.parallelize(list).distinct().foreach(println(_))
    }

    def filter(): Unit = {
        val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val listRDD = sc.parallelize(list)
        listRDD.filter(num => num % 2 == 0).foreach(print(_))
    }

    def mapParations(): Unit = {
        val list = List(1, 2, 3, 4, 5, 6)
        val listRDD = sc.parallelize(list, 4)
//        println(listRDD.getNumPartitions)
//        println(listRDD.getCheckpointFile)

        listRDD.mapPartitions(iterator => {
            val newList: ListBuffer[Int] = ListBuffer()
            while (iterator.hasNext) {
                newList.append(iterator.next())
            }
            println(newList.max,  "---------- ")
//            newList.toIterator
            newList.iterator
        }).foreach(name => println(name))
    }

    def mapPartitionsWithIndex(): Unit = {
        val list = List(1, 2, 3, 4, 5, 6, 7, 8)
        sc.parallelize(list, 2).mapPartitionsWithIndex((index, iterator) => {
            val listBuffer: ListBuffer[String] = new ListBuffer
            while (iterator.hasNext) {
                listBuffer.append(index + "_" + iterator.next())
            }
            listBuffer.iterator
        }, true)
            .foreach(println(_))
    }

    def reduce(): Unit = {
        val list = List(1, 2, 3, 4, 5, 6)
        val listRDD = sc.parallelize(list)
        val result = listRDD.reduce((x, y) => x + y)
        println(result)
        val maxValue = listRDD.reduce((x: Int, y: Int) => {
            if (x > y) x else y
        })
        println(maxValue)
    }

    def reduceByKey(): Unit = {
        val list = List(("武当", 99), ("少林", 97), ("武当", 89), ("少林", 77))
        val mapRDD = sc.parallelize(list)
        val resultRDD = mapRDD.reduceByKey(_ + _)
        resultRDD.foreach(tuple => println("门派: " + tuple._1 + "->" + tuple._2))
    }

    def groupByKey(): Unit = {
        val list = List(("武当", "张三丰"), ("峨眉", "灭绝师太"), ("武当", "宋青书"), ("峨眉", "周芷若"))
        val listRDD = sc.parallelize(list)
        val groupByKeyRDD = listRDD.groupByKey()
        groupByKeyRDD.foreach(t => {
            val menpai = t._1
            val iterator = t._2.iterator
            var people = ""
            while (iterator.hasNext) people = people + iterator.next + " "
            println("门派:" + menpai + "人员:" + people)
        })
    }

    def aggregateByKey(): Unit = {
        val list = List("you,jump", "i,jump")
        sc.parallelize(list)
            .flatMap(_.split(","))
            .map((_, 1))
          // rdd.aggregateByKey(3, seqFunc, combFunc) 其中第一个函数是初始值，3代表每次分完组之后的每个组的初始值。
          //seqFunc代表combine的聚合逻辑吗，每一个mapTask的结果的聚合成为combine。
          //combFunc代表reduce端大聚合的逻辑。
//            .aggregateByKey(3)(_ + _, _ + _)
            .aggregateByKey(0)(_ + _, _ + _)
            .foreach(tuple => println(tuple._1 + "->" + tuple._2))
    }

    def sortByKey(): Unit = {
        val list = List((99, "张三丰"), (96, "东方不败"), (66, "林平之"), (98, "聂风"))
        sc.parallelize(list).sortByKey(false).foreach(tuple => println(tuple._2 + "->" + tuple._1))
    }

    def cogroup(): Unit = {
        val list1 = List((100, "www"), (4, "bbs"))
        val list2 = List((1, "cnblog"), (2, "cnblog"), (3, "very"))
        val list3 = List((1, "com"), (2, "com"), (3, "good"))

        val list1RDD = sc.parallelize(list1)
        val list2RDD = sc.parallelize(list2)
        val list3RDD = sc.parallelize(list3)

        list1RDD.cogroup(list2RDD, list3RDD).foreach(tuple =>
//            println(tuple._1 + " " + tuple._2._1 + " " + tuple._2._2 + " " + tuple._2._3))
            println(tuple))
    }

//    重新分区并且在分区内排序
    def repartitionAndSortWithinPartitions(): Unit = {
        val list = List(1, 4, 55, 66, 33, 48, 23)
        val listRDD = sc.parallelize(list, 1)
        listRDD.map(num => (num, num))
            .repartitionAndSortWithinPartitions(new HashPartitioner(2))
            .mapPartitionsWithIndex((index, iterator) => {
                val listBuffer: ListBuffer[String] = new ListBuffer
                while (iterator.hasNext) {
                    listBuffer.append(index + "_" + iterator.next())
                }
                listBuffer.iterator
            }, false)
            .foreach(println(_))

    }

    def repartition(): Unit = {
        val list = List(1, 2, 3, 4)
        val listRDD = sc.parallelize(list, 1)
        println(listRDD.getNumPartitions)
//        listRDD.repartition(2).foreach(println(_))
        println(listRDD.repartition(2).getNumPartitions)
    }

//    通常对一个RDD执行filter算子过滤掉RDD中较多数据后（比如30%以上的数据），建议使用coalesce算子，手动减少RDD的partition数量，将RDD中的数据压缩到更少的partition中去。因为filter之后，RDD的每个partition中都会有很多数据被过滤掉，此时如果照常进行后续的计算，其实每个task处理的partition中的数据量并不是很多，有一点资源浪费，而且此时处理的task越多，可能速度反而越慢。因此用coalesce减少partition数量，将RDD中的数据压缩到更少的partition之后，只要使用更少的task即可处理完所有的partition。在某些场景下，对于性能的提升会有一定的帮助。
    def coalesce(): Unit = {
        val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
//        sc.parallelize(list, 3).coalesce(1).foreach(println(_))
        val rdd1 = sc.parallelize(list, 3)
        println(rdd1.getNumPartitions)
        val rdd2 = rdd1.coalesce(1)
        println(rdd2.getNumPartitions)
    }

    def intersection(): Unit = {
        val list1 = List(1, 2, 3, 4)
        val list2 = List(3, 4, 5, 6)
        val list1RDD = sc.parallelize(list1)
        val list2RDD = sc.parallelize(list2)
        list1RDD.intersection(list2RDD).foreach(println(_))
    }


    def cartesian(): Unit = {
        val list1 = List("A", "B")
        val list2 = List(1, 2, 3)
        val list1RDD = sc.parallelize(list1)
        val list2RDD = sc.parallelize(list2)
        list1RDD.cartesian(list2RDD).foreach(t => println(t._1 + "->" + t._2))
    }

    def sample(): Unit = {
        val list = 1 to 100
        val listRDD = sc.parallelize(list)
        // 有无放回的抽样， 抽取样本的比例，随机种子
        listRDD.sample(false, 0.2, 1).foreach(num => print(num + " "))
    }

    def join(): Unit = {
        val list1 = List((1, "东方不败"), (2, "令狐冲"), (3, "林平之"))
        val list2 = List((1, 99), (2, 98), (3, 97))
        val list1RDD = sc.parallelize(list1)
        val list2RDD = sc.parallelize(list2)
        val joinRDD = list1RDD.join(list2RDD)
//        joinRDD.foreach(t => println("学号:" + t._1 + " 姓名:" + t._2._1 + " 成绩:" + t._2._2))
        joinRDD.foreach(t => println(t))
    }

//    直接合并，不去重
    def union(): Unit = {
        val list1 = List(1, 2, 3, 4)
        val list2 = List(3, 4, 5, 6)
        val rdd1 = sc.parallelize(list1)
        val rdd2 = sc.parallelize(list2)
        rdd1.union(rdd2).foreach(println(_))
    }
}
