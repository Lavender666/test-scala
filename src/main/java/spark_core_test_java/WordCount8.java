package spark_core_test_java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import static java.lang.System.exit;

public class WordCount8 {
    public static void main(String[] args) {
        if(args.length != 1) {
            System.out.println("Usage:JavaWordCount<input><output>");
            exit(1);
        }
        SparkConf conf = new SparkConf();
        conf.setAppName(WordCount8.class.getSimpleName());
        conf.setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> fileRDD = jsc.textFile(args[0]);
        //lambda表达式
//        fileRDD.flatMap(line -> Arrays.asList(line.split("\t")).iterator()).mapToPair(e -> new Tuple2<>(e, 1))
//                .reduceByKey((x, y) -> x + y).mapToPair(e -> e.swap()).sortByKey(false).foreach(e -> System.out.println(e));

        System.out.println("==================分隔符===================");
//      方法引用报错
//        fileRDD.flatMap(line -> Arrays.asList(line.split("\t")).iterator()).mapToPair(e -> new Tuple2<>(e, 1))
//                .reduceByKey((x, y) -> x + y).mapToPair(Tuple2::swap).sortByKey(false).foreach(System.out::println);
        jsc.close();
    }
}
