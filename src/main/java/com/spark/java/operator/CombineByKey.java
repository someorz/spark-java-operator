package com.spark.java.operator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());


        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 1, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        //转化为pairRDD
        JavaPairRDD<Integer, Integer> javaPairRDD = javaRDD.mapToPair((PairFunction<Integer, Integer, Integer>)
                integer -> new Tuple2<>(integer, 1));

        JavaPairRDD<Integer, List<Integer>> combineByKeyRDD = javaPairRDD.combineByKey(
                (Function<Integer, List<Integer>>) v1 -> {
                    ArrayList<Integer> integers = new ArrayList<>();
                    integers.add(v1);
                    return integers;
                }, (Function2<List<Integer>, Integer, List<Integer>>) (integers, integer) -> {
                    integers.add(integer);
                    return integers;
                }, (Function2<List<Integer>, List<Integer>, List<Integer>>) (integers, integers2) -> {
                    integers.addAll(integers2);
                    return integers;
                });
        System.out.println(combineByKeyRDD.collect());

        jsc.stop();
        session.stop();
    }
}
