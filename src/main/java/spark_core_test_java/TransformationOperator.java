package spark_core_test_java;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 */
public class TransformationOperator {

     public static  SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
     public static JavaSparkContext sc = new JavaSparkContext(conf);


    public static void map(){
        final List<String> list = Arrays.asList("张无忌", "赵敏", "周芷若");
        final JavaRDD<String> rdd = sc.parallelize(list);

        final JavaRDD<String> nameRDD = rdd.map(new Function<String, String>() {
            @Override
            public String call(String name) throws Exception {
                return "Hello " + name;
            }
        });

        nameRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                println(s);
            }
        });
    }


    public static void flatMap(){
        final List<String> list = Arrays.asList("张无忌 赵敏", "宋青书 周芷若");
        final JavaRDD<String> rdd = sc.parallelize(list);
        rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String names) throws Exception {
                return Arrays.asList(names.split(" ")).iterator();
            }
        }).map(new Function<String, String>() {
            @Override
            public String call(String name) throws Exception {
                return "Hello "+ name;
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String line) throws Exception {
                println(line);
            }
        });
    }

    /**
     * 从RDD过滤出来偶数
     */
    public static void filter(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        final JavaRDD<Integer> rdd = sc.parallelize(list);
        final JavaRDD<Integer> filterRDD = rdd.filter(new Function<Integer, Boolean>() {
            //true 代表这个值我们要
            @Override
            public Boolean call(Integer number) throws Exception {
                return number % 2 == 0;
            }
        });
        filterRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                println(integer + "");
            }
        });

    }

    /**RDD()
     * bykey
     */
    public static void groupBykey(){
        final List<Tuple2<String, String>> list = Arrays.asList(
                new Tuple2<String, String>("峨眉", "周芷若"),
                new Tuple2<String, String>("武当", "宋青书"),
                new Tuple2<String, String>("峨眉", "灭绝师太"),
                new Tuple2<String, String>("武当", "张三丰")
        );

        final JavaPairRDD<String, String> rdd = sc.parallelizePairs(list);

        final JavaPairRDD<String, Iterable<String>> groupBykeyRDD = rdd.groupByKey();

        groupBykeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                final String menpai = tuple._1;
                final Iterator<String> iterator = tuple._2.iterator();
                println(menpai+ " ");
                while (iterator.hasNext()){
                    final String name = iterator.next();
                    System.out.print(name);
                }
                println("");
            }
        });
    }

    /**
     * 一线城市： 8 年  ->  100万
     *   5:  50以上IT
     */
    public static void reduceBykey(){
        final List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("峨眉", 40),
                new Tuple2<String, Integer>("武当", 30),
                new Tuple2<String, Integer>("峨眉",60),
                new Tuple2<String, Integer>("武当",99)
        );
        //reduceBykey
        final JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);

        rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                println( tuple._1 + " "+ tuple._2);
            }
        });
    }


    public static void sortBykey(){
        final List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<Integer, String>(98,"东方不败"),
                new Tuple2<Integer, String>(80,"岳不群"),
                new Tuple2<Integer, String>(85,"令狐冲"),
                new Tuple2<Integer, String>(83,"任我行")
        );
        final JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(list);
        rdd.sortByKey(false)
                .foreach(new VoidFunction<Tuple2<Integer, String>>() {
                    @Override
                    public void call(Tuple2<Integer, String> tuple) throws Exception {
                        println(tuple._1 + " -> "+ tuple._2);
                    }
                });
    }


    public static void join(){
        final List<Tuple2<Integer, String>> names = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "令狐冲"),
                new Tuple2<Integer, String>(3, "林平之")
        );
        final List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 98),
                new Tuple2<Integer, Integer>(3, 97)
        );

        final JavaPairRDD<Integer, String> nemesrdd = sc.parallelizePairs(names);
        final JavaPairRDD<Integer, Integer> scoresrdd = sc.parallelizePairs(scores);
        /**
         * <Integer, 学号
         * Tuple2<String, 名字
         * Integer>> 分数
         */
        final JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = nemesrdd.join(scoresrdd);
//        final JavaPairRDD<Integer, Tuple2<Integer, String>> join = scoresrdd.join(nemesrdd);
        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple) throws Exception {
                println("学号：" + tuple._1 + " 名字："+tuple._2._1 + " 分数："+tuple._2._2);
            }
        });

    }

    public static void union(){
        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        final List<Integer> list2 = Arrays.asList(3, 4, 5, 6);
        final JavaRDD<Integer> rdd1 = sc.parallelize(list1);
        final JavaRDD<Integer> rdd2 = sc.parallelize(list2);
        rdd1.union(rdd2)
                .foreach(new VoidFunction<Integer>() {
                    @Override
                    public void call(Integer number) throws Exception {
                        println(number + "");
                    }
                });
    }

    /**
     * 交集
     */
    public static void intersection(){
        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        final List<Integer> list2 = Arrays.asList(3, 4, 5, 6);
        final JavaRDD<Integer> rdd1 = sc.parallelize(list1);
        final JavaRDD<Integer> rdd2 = sc.parallelize(list2);

      rdd1.intersection(rdd2)
              .foreach(new VoidFunction<Integer>() {
                  @Override
                  public void call(Integer number) throws Exception {
                      println(number + "");
                  }
              });

    }

    public static void distinct(){
        final List<Integer> list1 = Arrays.asList(1, 2, 3,3,4,4);
        final JavaRDD<Integer> rdd1 = sc.parallelize(list1);
        rdd1.distinct()
                .foreach(new VoidFunction<Integer>() {
                    @Override
                    public void call(Integer number) throws Exception {
                        println(number + " ");
                    }
                });
    }

    /**
     * 笛卡尔积
     * A={a,b}
     * B={0,1,2}
     * A B 笛卡尔积
     * a0,a1,a2
     * b0,b1,b2
     */
    public static void cartesian(){

        final List<String> A = Arrays.asList("a", "b");
        final List<Integer> B = Arrays.asList(0, 1, 2);

        final JavaRDD<String> rddA = sc.parallelize(A);
        final JavaRDD<Integer> rddB = sc.parallelize(B);

        rddA.cartesian(rddB)
                .foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> tuple) throws Exception {
                        println(tuple._1 + "->"+ tuple._2);
                    }
                });
    }

    /**
     * map:
     *    一条数据一条数据的处理（文件系统，数据库等等）
     * mapPartitions：
     *    一次获取的是一个分区的数据（hdfs）
     *    正常情况下，mapPartitions 是一个高性能的算子
     *    因为每次处理的是一个分区的数据，减少了去获取数据的次数。
     *
     *    但是如果我们的分区如果设置得不合理，有可能导致每个分区里面的数据量过大。
     */
    public static void  mapPartitions(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        //参数二代表这个rdd里面有两个分区
        final JavaRDD<Integer> rdd = sc.parallelize(list, 2);

        rdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, String>() {
            //每次处理的是一个分区的数据
            @Override
            public Iterator<String> call(Iterator<Integer> iterator) throws Exception {
             List<String> list=new ArrayList<String> ();
                while(iterator.hasNext()){
                    list.add("hello-" + iterator.next());
                }
                return list.iterator();
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                println(s);
            }
        });
    }

    /**
     * 进行重分区
     * HDFS -> hello.txt   2个文件块（不包含副本）
     * 2个文件块 ->2 个分区  -> 当spark任务运行，一个分区就启动一个task任务。
     *
     * 解决的问题：本来分区数少  ->  增加分区数
     */
    public static void repartition(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        final JavaRDD<Integer> rdd = (JavaRDD<Integer>) sc.parallelize(list, 1);
       // coalesce(numPartitions, shuffle = true)
        rdd.repartition(2)
                .foreach(new VoidFunction<Integer>() {
                    @Override
                    public void call(Integer number) throws Exception {
                    println(number+ "");
                    }
                });
    }

    /**
     * 实现单词计数
     */
    public static void aggregateByKey(){
        final List<String> list = Arrays.asList("you,jump", "i,jump");
        final JavaRDD<String> rdd = sc.parallelize(list);
        rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(",")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        }).aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;//局部
                    }
                }, new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;//全局
                    }
                }
        ).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                println(tuple._1 + "  ->"+ tuple._2);
            }
        });
    }

    /**
     * 分区数由多  ->  变少
     */
    public static void coalesce(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        final JavaRDD<Integer> rdd = (JavaRDD<Integer>) sc.parallelize(list, 3);
       rdd.coalesce(1)
               .foreach(new VoidFunction<Integer>() {
                   @Override
                   public void call(Integer integer) throws Exception {
                       println(integer + "");
                   }
               });
    }

    /**
     * map: 每次获取和处理的就是一条数据
     * mapParitions: 每次获取和处理的就是一个分区的数据
     *  mapPartitionsWithIndex:每次获取和处理的就是一个分区的数据,并且知道处理的分区的分区号是啥？
     */
    public static void mapPartitionsWithIndex(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        final JavaRDD<Integer> rdd = sc.parallelize(list, 2);//HashParitioners Rangepartitionw 自定义分区

        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Integer> iterator) throws Exception {
                final ArrayList<String> list = new ArrayList<>();
                while (iterator.hasNext()){
                    list.add(index+"_"+ iterator.next());
                }
                return list.iterator();
            }
        },true)
                .foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        println(s);
                    }
                });

    }

    /**
     * When called on datasets of type (K, V) and (K, W),
     * returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples.
     */
    public static void cogroup(){
        //sh s   sha  shan shang sa san sang
        final List<Tuple2<Integer, String>> list1 = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "林平之"),
                new Tuple2<Integer, String>(3, "岳不群"),
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "林平之"),
                new Tuple2<Integer, String>(3, "岳不群")
        );

        final List<Tuple2<Integer, Integer>> list2 = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 90),
                new Tuple2<Integer, Integer>(2, 91),
                new Tuple2<Integer, Integer>(3, 89),
                new Tuple2<Integer, Integer>(1, 98),
                new Tuple2<Integer, Integer>(2, 78),
                new Tuple2<Integer, Integer>(3, 67)
        );

        final JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(list1);
        final JavaPairRDD<Integer, Integer> rdd2 = sc.parallelizePairs(list2);

        final JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> rdd3 =
                (JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>) rdd1.cogroup(rdd2);
        rdd3.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> tuple) throws Exception {
                final Integer id = tuple._1;
                final Iterable<String> names = tuple._2._1;
                final Iterable<Integer> scores = tuple._2._2;
                println("ID:"+id + " Name: "+names+ " Scores: "+ scores);
            }
        });
    }

    /**
     * 少  ->  多
     *
     */
    public static void repartitionAndSortWithinPartitions(){//调优
        final List<Integer> list = Arrays.asList(1, 2, 11, 3, 12, 4, 5);
        final JavaRDD<Integer> rdd = sc.parallelize(list, 1);
        final JavaPairRDD<Integer, Integer> pairRDD = rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer number) throws Exception {
                return new Tuple2<>(number, number);
            }
        });
         //new HashPartitioner(2) new RangePartitioner<>()
        pairRDD.repartitionAndSortWithinPartitions(new Partitioner() {
            @Override
            public int numPartitions() {
                return 2;
            }

            @Override
            public int getPartition(Object key) {
                final Integer number = Integer.valueOf(key.toString());
                if(number % 2 == 0){
                    return 0;
                }else{
                    return 1;
                }
            }
        }).mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, Integer>>,
                Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Tuple2<Integer, Integer>> iterator) throws Exception {
                final ArrayList<String> list = new ArrayList<>();
                while(iterator.hasNext()){
                    list.add(index + "_"+ iterator.next());
                }
                return list.iterator();
            }
        },false)
                .foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        println(s);
                    }
                });
    }


    /**
     * 有放回
     * 无放回
     */
    public static void sample(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7,9,10);
        final JavaRDD<Integer> rdd = sc.parallelize(list);
        /**
         * withReplacement: Boolean,
         *       true: 有放回的抽样
         *       false: 无放回抽象
         * fraction: Double：
         *      RDD  里面的每个元素被抽到的概率有多大
         * seed: Long：
         *      随机种子
         */
        final JavaRDD<Integer> rdd2 = rdd.sample(false, 0.5);

        rdd2.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                println(integer + "");
            }
        });
    }


    public static  void pipe(){
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7,9,10);
        final JavaRDD<Integer> rdd = sc.parallelize(list);

     //   final JavaRDD<String> pipe = rdd.pipe("sh wordcount.sh");
    }

    public static void println(String str){
        System.out.println(str);
    }



    public static void main(String[] args) {
       // map();
       // filter();
       // flatMap();
       // groupBykey();
       // reduceBykey();
       // sortBykey();
       // join();
       // union();
       // intersection();
       // cartesian();
       // mapPartitions();
       // repartition();
       // coalesce();
        aggregateByKey();
       // mapPartitionsWithIndex();
       // cogroup();
       // repartitionAndSortWithinPartitions();
       // sample();

    }
}
