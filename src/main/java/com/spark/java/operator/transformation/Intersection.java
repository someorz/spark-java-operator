package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * 求交集
 *
 * @author machi
 * @create 2018/05/21
 **/
public class Intersection {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("Intersection")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        JavaRDD<Integer> intersectionRDD = javaRDD.intersection(javaRDD);
        System.out.println(intersectionRDD.collect());

        jsc.close();
        session.stop();
    }
}
