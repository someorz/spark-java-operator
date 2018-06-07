package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Cartesian 对两个 RDD 做笛卡尔集，生成的 CartesianRDD 中 partition 个数 = partitionNum(RDD a) * partitionNum(RDD b)。
 *
 * @author machi
 * @create 2018/05/21
 **/
public class Distinct {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("Distinct")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 1, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);

        JavaRDD<Integer> distinctRDD1 = javaRDD.distinct();
        System.out.println(distinctRDD1.collect());
        JavaRDD<Integer> distinctRDD2 = javaRDD.distinct(2);
        System.out.println(distinctRDD2.collect());

        jsc.close();
        session.stop();
    }
}
