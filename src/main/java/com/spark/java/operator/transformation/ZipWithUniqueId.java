package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class ZipWithUniqueId {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("ZipWithUniqueId")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);
        JavaPairRDD<Integer, Long> zipWithIndexRDD = javaRDD.zipWithUniqueId();
        System.out.println(zipWithIndexRDD.collect());

        jsc.close();
        session.stop();
    }
}
