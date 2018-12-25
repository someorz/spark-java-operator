package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/21
 **/
public class Map {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("map")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<String> data = Arrays.asList("1,2", "3,4","5,6", "7,8");
        JavaRDD<String> javaRDD = jsc.parallelize(data);

        final JavaRDD<String[]> rdd1 = javaRDD.map((Function<String, String[]>) str -> str.split(","));


        System.out.println(rdd1.collect());
        final JavaRDD<String> rdd2 = javaRDD.flatMap(
                (FlatMapFunction<String, String>) str ->
                      Arrays.asList(str.split(",")).iterator());
        System.out.println(rdd2.collect());

        jsc.close();
        session.stop();
    }
}
