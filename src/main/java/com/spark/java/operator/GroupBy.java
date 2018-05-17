package com.spark.java.operator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class GroupBy {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("GroupBy")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 2, 4, 4, 5, 6, 6, 6);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        //转为k，v格式
        JavaPairRDD<Integer, Iterable<Integer>> javaPairRDD = javaRDD.groupBy(
                (Function<Integer, Integer>) integer -> integer);

        System.out.println(javaPairRDD.collect());

        jsc.close();
        session.stop();
    }

}
