package com.spark.java.operator.transformation;

import org.apache.commons.lang3.RandomUtils;
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

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 1, 2, 4, 4, 4, 1, 3, 2, 1, 1, 1, 6, 7, 3, 3, 3, 3, 3,
                1);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        //转化为pairRDD
        JavaPairRDD<Integer, Integer> javaPairRDD = javaRDD.mapToPair((PairFunction<Integer, Integer, Integer>)
                integer -> new Tuple2<>(integer, RandomUtils.nextInt(100, 200)));

        JavaPairRDD<Integer, List<Integer>> combineByKeyRDD = javaPairRDD.combineByKey(
                (Function<Integer, List<Integer>>) s -> {
                    List<Integer> collect = Stream.of(s).collect(Collectors.toList());
                    return collect;
                },
                (Function2<List<Integer>, Integer, List<Integer>>) (strings, s) -> {
                    strings.add(s);
                    return strings;
                }, (Function2<List<Integer>, List<Integer>, List<Integer>>) (strings, strings2) -> {
                    strings.addAll(strings2);
                    return strings;
                });
        System.out.println(combineByKeyRDD.collect());

        jsc.stop();
        session.stop();
    }

}
