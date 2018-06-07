package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * coalesce() 可以将 parent RDD 的 partition 个数进行调整，比如从 5 个减少到 3 个，或者从 5 个增加到 10 个。
 * 需要注意的是当 shuffle = false 的时候，是不能增加 partition 个数的（即不能从 5 个变为 10 个）。
 *
 * @author machi
 * @create 2018/05/21
 **/
public class Coalesce {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("Coalesce")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        // shuffle默认是false
        JavaRDD<Integer> coalesceRDD = javaRDD.coalesce(2);
        System.out.println(coalesceRDD);

        JavaRDD<Integer> coalesceRDD1 = javaRDD.coalesce(2, true);
        System.out.println(coalesceRDD1);

        jsc.close();
        session.stop();
    }
}
