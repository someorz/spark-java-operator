package com.spark.java.operator.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class SortBy {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("SortBy")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);
        final Random random = new Random(100);
        //对RDD进行转换，每个元素有两部分组成
        JavaRDD<String> javaRDD1 = javaRDD.map(
                (Function<Integer, String>) v1 -> v1.toString() + "_" + random.nextInt(100));
        System.out.println(javaRDD1.collect());
        //按RDD中每个元素的第二部分进行排序
        JavaRDD<String> resultRDD = javaRDD1.sortBy((Function<String, Object>) v1 -> v1.split("_")[1], false, 3);
        System.out.println("result--------------" + resultRDD.collect());


        jsc.close();
        session.stop();
    }
}
