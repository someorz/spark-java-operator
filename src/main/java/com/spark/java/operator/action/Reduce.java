package com.spark.java.operator.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class Reduce {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("Reduce")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);

        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);

        Integer reduceRDD = javaRDD.reduce((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
        System.out.println(reduceRDD);

        jsc.close();
        session.stop();
    }
}
