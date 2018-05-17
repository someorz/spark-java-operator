package com.spark.java.operator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class AggregateByKey {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("CombineByKey")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());


        List<Tuple2<Integer, String>> list = new ArrayList<>();

        list.add(new Tuple2<>(1, "www"));
        list.add(new Tuple2<>(1, "iteblog"));
        list.add(new Tuple2<>(1, "com"));
        list.add(new Tuple2<>(2, "bbs"));
        list.add(new Tuple2<>(2, "iteblog"));
        list.add(new Tuple2<>(2, "com"));
        list.add(new Tuple2<>(3, "good"));

        JavaPairRDD<Integer, String> data = jsc.parallelizePairs(list);

        JavaPairRDD<Integer, List<String>> result = data.aggregateByKey(new ArrayList<>(), (c, v) -> {
            c.add(v);
            return c;
        }, (Function2<List<String>, List<String>, List<String>>) (c1, c2) -> {
            c1.addAll(c2);
            return c1;
        });

        result.collect().forEach(System.out::println);
    }
}
