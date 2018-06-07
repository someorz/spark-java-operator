package com.spark.java.operator.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class TakeSample {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("TakeSample")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(5, 1, 0, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);
        System.out.println("takeSample-----1-------------" + javaRDD.takeSample(true, 2));
        System.out.println("takeSample-----2-------------" + javaRDD.takeSample(true, 2, 100));
        //返回20个元素
        System.out.println("takeSample-----3-------------" + javaRDD.takeSample(true, 20, 100));
        //返回7个元素
        System.out.println("takeSample-----4-------------" + javaRDD.takeSample(false, 20, 100));

        jsc.close();
        session.stop();
    }
}
