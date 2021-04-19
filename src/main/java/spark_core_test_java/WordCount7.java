package spark_core_test_java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount7 {
    public static void main(String[] args) {
        if(args.length != 1){
            System.out.println("Usage:JavaWordCount<input><output>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName(WordCount7.class.getSimpleName());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> fileRDD = jsc.textFile(args[0]);
        //切割压平 flatMap() 两个参数，一个输入类型，一个输出类型
        //相当于对fileRDD的每一行数据执行FlatMapFunction函数式接口里面的唯一抽象方法call，因此下面要实现该函数式接口，相当于给flatMap传递了一个匿名类的对象，该匿名累实现了该函数式接口
        //结果：Array[String] = Array(hello, haungbo, hello, xuzheng, hello, world)
        JavaRDD<String> flatRDD = fileRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\t")).iterator();
            }

        });
        JavaPairRDD<String, Integer> pairRDD = flatRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //直接打印
        /*reduceRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });*/
        //排序后打印，默认是升序，如果需要降序，参数 false
        reduceRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });
        //释放资源
        jsc.close();
    }
}
