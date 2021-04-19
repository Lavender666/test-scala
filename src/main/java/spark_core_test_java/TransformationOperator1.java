package spark_core_test_java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Administrator on 2017/8/1.
 */
public class TransformationOperator1 {
    public static void println(String s){
        System.out.println(s);
    }

    public static void map(){
        final SparkConf conf = new SparkConf().setAppName("map").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<String> list = Arrays.asList("zhangwuji", "zhangsanfeng", "songqingshu");
        final JavaRDD<String> listrdd = sc.parallelize(list);

        final JavaRDD<String> helloRDD = listrdd.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return "hello" + v1;
            }
        });

        helloRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                println(s);
            }
        });

    }

    /**
     * 返回true就是我们
     */
    public static void filter(){
        final SparkConf conf = new SparkConf().setAppName("filter").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        final JavaRDD<Integer> listrdd = sc.parallelize(list);
        listrdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        }).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer i) throws Exception {
                println(i+"");
            }
        });
    }


    public static void flatMap(){
        final SparkConf conf = new SparkConf().setAppName("filter").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<String> list = Arrays.asList("you,jump", "i,jump");
        final JavaRDD<String> listrdd = sc.parallelize(list);
        listrdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                println(s);
            }
        });
    }


    public static void groupByKey(){
        final SparkConf conf = new SparkConf().setAppName("filter").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<Tuple2<String, String>> list = Arrays.asList(
                new Tuple2<String, String>("武当", "张五侠"),
                new Tuple2<String, String>("峨眉", "周芷若"),
                new Tuple2<String, String>("武当", "张无忌"),
                new Tuple2<String, String>("峨眉", "灭绝师太")
        );
        final JavaRDD<Tuple2<String, String>> listrdd = sc.parallelize(list);
        //第二个参数让我们指定的是我们要按照分组的那个字段的数据类型
        final JavaPairRDD<String, Iterable<Tuple2<String, String>>> resultRDD = listrdd.groupBy(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._1;
            }
        });
        /**
         *
         *         门派                    门派    人名
         * Tuple2<String, Iterable<Tuple2<String, String>>>
         *
         *
         */
        resultRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Tuple2<String, String>>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Tuple2<String, String>>> t) throws Exception {
                println("门派："+t._1);
                final Iterator<Tuple2<String, String>> persons = t._2.iterator();
                while(persons.hasNext()){
                    println(persons.next()._2+"");
                }
            }
        });
    }

    public static void reduceBykey(){
        final SparkConf conf = new SparkConf().setAppName("filter").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("武当", 50),
                new Tuple2<String, Integer>("峨眉", 40),
                new Tuple2<String, Integer>("武当",70),
                new Tuple2<String, Integer>("峨眉", 80)
        );

        final JavaPairRDD<String, Integer> listrdd = sc.parallelizePairs(list);

        final JavaPairRDD<String, Integer> resultRDD = listrdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        resultRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                println(t._1 + "  "+ t._2);
            }
        });

    }






    public static void sortBykey1(){
        final SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<Integer, String>(99, "张无忌"),
                new Tuple2<Integer, String>(40, "布袋和尚"),
                new Tuple2<Integer, String>(90,"张三丰"),
                new Tuple2<Integer, String>(70, "杨逍")
        );
        //scala sortBy java:sortByKey
        final JavaRDD<Tuple2<Integer, String>> listrdd = sc.parallelize(list);
        final JavaRDD<Tuple2<Integer, String>> resultRDD = listrdd.sortBy(new Function<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, String> v1) throws Exception {
                return v1._1;
            }
        }, true, 1);





        resultRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                println(t._2 + "  "+ t._1);
            }
        });


    }


    public static void sortBykey2(){
        final SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<Integer, String>(99, "张无忌"),
                new Tuple2<Integer, String>(40, "布袋和尚"),
                new Tuple2<Integer, String>(90,"张三丰"),
                new Tuple2<Integer, String>(70, "杨逍")
        );
        //scala sortBy java:sortByKey
        final JavaPairRDD<Integer, String> listrdd = sc.parallelizePairs(list,1);
        listrdd.sortByKey().foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                println(t._2 + "  "+ t._1);
            }
        });


    }

    public static void join(){
        final SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<Tuple2<Integer, String>> list1 = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "令狐冲"),
                new Tuple2<Integer, String>(3, "风清扬")
        );

        final List<Tuple2<Integer, Integer>> list2 = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 88)
        );

        final JavaPairRDD<Integer, String> list1RDD = sc.parallelizePairs(list1);
        final JavaPairRDD<Integer, Integer> list2rdd = sc.parallelizePairs(list2);
        //1
        final JavaPairRDD<Integer, Tuple2<String, Integer>> resultRDD = list1RDD.join(list2rdd);

        /**
         * 1, "东方不败"      join     1, 99
         * 2, "令狐冲"                 2, 90
         *
         *
         * <Integer, Tuple2<String, Integer>>
         *
         *     1  "东方不败"  99
         *     2   "令狐冲"   90
         *
         */


    }

    /*
    并集
     */
    public static void union(){
        final SparkConf conf = new SparkConf().setAppName("union").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5);
        final List<Integer> list2 = Arrays.asList(2, 3, 4, 5, 6, 7, 8);
        final JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        final JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        list1RDD.union(list2RDD)
                .foreach(new VoidFunction<Integer>() {
                    @Override
                    public void call(Integer integer) throws Exception {
                        println(integer+"");
                    }
                });
    }

    /**
     * 求交集
     */
    public static void Intersection(){
        final SparkConf conf = new SparkConf().setAppName("union").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5);
        final List<Integer> list2 = Arrays.asList(2, 3, 4, 5, 6, 7, 8);
        final JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        final JavaRDD<Integer> list2RDD = sc.parallelize(list2);

        list1RDD.intersection(list2RDD)
                .foreach(new VoidFunction<Integer>() {
                    @Override
                    public void call(Integer integer) throws Exception {
                        println(integer + "");
                    }
                });
    }

    public static void dinstinct(){
        final SparkConf conf = new SparkConf().setAppName("union").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5,4,5);

        final JavaRDD<Integer> list1RDD = sc.parallelize(list1);

        list1RDD.distinct()
                .foreach(new VoidFunction<Integer>() {
                    @Override
                    public void call(Integer integer) throws Exception {
                        println(integer+"");
                    }
                });

    }

    /**
     * 可以做笛卡尔积
     * listrdd1   a  b
     *        coalesce
     * listrdd2   1  2
     *       (a,1)(a,2)(b,1)(b2)
     */
    public static void cartesian(){
        final SparkConf conf = new SparkConf().setAppName("union").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<String> list1 = Arrays.asList("a", "b");
        final JavaRDD<String> list1RDD = sc.parallelize(list1);
        final List<Integer> list2 = Arrays.asList(1, 2);
        final JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        list1RDD.cartesian(list2RDD)
                .foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> t) throws Exception {
                        println(t._1 + "->"+t._2);
                    }
                });
    }

    public static void mapPartion(){
        final SparkConf conf = new SparkConf().setAppName("union").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5,6,7,8,9,10);

        final JavaRDD<Integer> list1RDD = sc.parallelize(list1,2);


        list1RDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, String>() {

            @Override
            public Iterator<String> call(Iterator<Integer> iterator) throws Exception {
                final ArrayList<String> strList = new ArrayList<>();
                while(iterator.hasNext()){
                    final Integer i = iterator.next();
                    strList.add("hello-"+i);
                }
                return strList.iterator();
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                println(s);
            }
        });
    }

    public static void repartition(){
        final SparkConf conf = new SparkConf().setAppName("union").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5,6,7,8,9,10);

        final JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        // coalesce(numPartitions, shuffle = true)
        list1RDD.repartition(2)
                .foreach(new VoidFunction<Integer>() {
                    @Override
                    public void call(Integer integer) throws Exception {
                        println(integer+"");
                    }
                });
    }


    public static void coalesce(){
        final SparkConf conf = new SparkConf().setAppName("union").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5,6,7,8,9,10);

        final JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        list1RDD.coalesce(2,true);
    }



    public static void aggregateByKey(){
        final SparkConf conf = new SparkConf().setAppName("filter").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<String> list = Arrays.asList("you,jump", "i,jump");
        final JavaRDD<String> listrdd = sc.parallelize(list);
        final JavaRDD<String> wordRDD = listrdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });
        wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        }).aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                println(t._1 + "->"+t._2);
            }
        });


    }

    /**
     * When called on datasets of type (K, V) and (K, W),
     * returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples.
     * This operation is also called groupWith.
     */
    public static void cogroup(){

        final SparkConf conf = new SparkConf().setAppName("filter").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final List<Tuple2<Integer, String>> list1 = Arrays.asList(
                new Tuple2<Integer, String>(2, "岳不群"),
                //  new Tuple2<Integer, String>(2, "岳不群2"),
                // new Tuple2<Integer,String>(1,"东方不败"),
                new Tuple2<Integer,String>(1,"东方不败2"));
        final List<Tuple2<Integer, Integer>> list2 = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 30),
                new Tuple2<Integer, Integer>(2, 40),
                new Tuple2<Integer, Integer>(1, 50),
                new Tuple2<Integer, Integer>(2, 70)

        );
        final JavaPairRDD<Integer, String> list1RDD = sc.parallelizePairs(list1);
        final JavaPairRDD<Integer, Integer> list2RDD = sc.parallelizePairs(list2);

        final JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>
                resultRDD = list1RDD.cogroup(list2RDD);

        resultRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                println("编号："+t._1);
                println("名字"+t._2._1);
                println("分数"+t._2._2);
            }
        });
        /**
         *
         */


    }

    /**
     * index 索引
     *
     *
     * 这个boolean 值代表啥意思
     */
    public static void mapPartitionsWithIndex(){
        final SparkConf conf = new SparkConf().setAppName("union").setMaster("local[2]");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5,6,7,8,9,10);

        final JavaRDD<Integer> list1RDD = sc.parallelize(list1, 2);

        final JavaRDD<String> resultRDD = list1RDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Integer> iterator) throws Exception {
                final ArrayList<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    final Integer i = iterator.next();
                    list.add(i + "_" + index);

                }
                return list.iterator();
            }
        }, true);
        resultRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                println(s);
            }
        });
    }

    /**
     *
     * 就是边分区的时候，就边进行排序
     * 然后之前我们有一个reptition，这个函数可以进行分区，但是如果我们要进行排序的话
     * 需要在写排序代码。
     *
     * mapreduce  TopN
     * take
     *
     *
     * 1:所有的Java  的 api 都 敲一遍  transformation action
     * 2：把所有用scala实现一遍  transfortion  action
     *
     *   repartitionAndSortWithinPartitions
     *   sample
     */
    public static void repartitionAndSortWithinPartitions(){


    }




    public static void main(String[] args) {
        //map();
        //  filter();
        //flatMap();
        //  groupByKey();
        // reduceBykey();
        // sortBykey1();
        //   join();
        // union();
        //  Intersection();
        //  dinstinct();
        //  sortBykey2();
        //   cartesian();
        //  mapPartion();
        // aggregateByKey();
        //cogroup();
        // mapPartitionsWithIndex();
        repartitionAndSortWithinPartitions();
    }
}
