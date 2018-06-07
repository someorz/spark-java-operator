package com.spark.java.operator.action;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * countByKey操作将数据全部加载到driver端的内存，如果数据量比较大，可能出现OOM
 *
 * @author machi
 * @create 2018/05/14
 **/
public class CountByKey {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("CountByKey")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<String> data = Arrays.asList("5", "1", "1", "3", "6", "2", "2");
        JavaRDD<String> javaRDD = jsc.parallelize(data, 5);

        JavaPairRDD<String, String> javaPairRDD = javaRDD.mapToPair(
                (PairFunction<String, String, String>) s -> new Tuple2<>(s, s));

        System.out.println(javaPairRDD.countByKey());

        jsc.close();
        session.stop();
    }
}
