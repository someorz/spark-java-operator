package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * union() 将两个 RDD 简单合并在一起，不改变 partition 里面的数据。
 *
 * @author machi
 * @create 2018/05/21
 **/
public class Union {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("Union")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        JavaRDD<Integer> unionRDD = javaRDD.union(javaRDD);
        System.out.println(unionRDD.collect());

        jsc.close();
        session.stop();
    }
}
