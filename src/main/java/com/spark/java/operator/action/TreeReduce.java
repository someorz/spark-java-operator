package com.spark.java.operator.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class TreeReduce {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("TreeReduce")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 5);
        JavaRDD<String> javaRDD1 = javaRDD.map((Function<Integer, String>) v1 -> Integer.toString(v1));
        String result = javaRDD1.treeReduce((Function2<String, String, String>) (v1, v2) -> {
            System.out.println(v1 + "=" + v2);
            return v1 + "=" + v2;
        });
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + result);

        jsc.close();
        session.stop();
    }
}
