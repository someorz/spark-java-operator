package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class CombineByKey {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("CombineByKey")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 1, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        //转化为pairRDD
        JavaPairRDD<Integer, String> javaPairRDD = javaRDD.mapToPair((PairFunction<Integer, Integer, String>)
                integer -> new Tuple2<>(integer, "1"));

        JavaPairRDD<Integer, List<String>> combineByKeyRDD = javaPairRDD.combineByKey(
                (Function<String, List<String>>) s -> {
                    List<String> collect = Stream.of(s).collect(Collectors.toList());
                    return collect;
                },
                (Function2<List<String>, String, List<String>>) (strings, s) -> {
                    strings.add(s);
                    return strings;
                }, (Function2<List<String>, List<String>, List<String>>) (strings, strings2) -> {
                    strings.addAll(strings2);
                    return strings;
                });
        System.out.println(combineByKeyRDD.collect());

        jsc.stop();
        session.stop();
    }
}
