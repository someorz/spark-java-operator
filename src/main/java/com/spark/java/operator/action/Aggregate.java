package com.spark.java.operator.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * aggregate函数将每个分区里面的元素进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
 *
 * @author machi
 * @create 2018/05/14
 **/
public class Aggregate {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("Aggregate")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);
        Integer aggregateValue = javaRDD.aggregate(3, (Function2<Integer, Integer, Integer>) (v1, v2) -> {
            System.out.println("seq~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + v1 + "," + v2);
            return Math.max(v1, v2);
        }, new Function2<Integer, Integer, Integer>() {
            int i = 0;

            @Override
            public Integer call(Integer v1, Integer v2) {
                System.out.println("comb~~~~~~~~~i~~~~~~~~~~~~~~~~~~~" + i++);
                System.out.println("comb~~~~~~~~~v1~~~~~~~~~~~~~~~~~~~" + v1);
                System.out.println("comb~~~~~~~~~v2~~~~~~~~~~~~~~~~~~~" + v2);
                return v1 + v2;
            }
        });
        System.out.println(aggregateValue);

        jsc.close();
        session.stop();
    }
}
