package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * 依据所提供的权重对该RDD进行随机划分
 *
 * @author machi
 * @create 2018/05/21
 **/
public class RandomSplit {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("RandomSplit")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        double[] weights = {0.1, 0.2, 0.7};
        //依据所提供的权重对该RDD进行随机划分
        JavaRDD<Integer>[] randomSplitRDDs = javaRDD.randomSplit(weights);
        System.out.println("randomSplitRDDs of size~~~~~~~~~~~~~~" + randomSplitRDDs.length);
        int i = 0;
        for (JavaRDD<Integer> item : randomSplitRDDs) {
            System.out.println(i++ + " randomSplitRDDs of item~~~~~~~~~~~~~~~~" + item.collect());
        }

        jsc.close();
        session.stop();
    }
}
