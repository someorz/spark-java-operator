package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * 如果使用repartition对RDD的partition数目进行缩减操作，可以使用coalesce函数，将shuffle设置为false，避免shuffle过程，提高效率。
 *
 * @author machi
 * @create 2018/05/21
 **/
public class Repartition {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("Repartition")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        //等价于 coalesce(numPartitions, shuffle = true)
        JavaRDD<Integer> repartitionRDD = javaRDD.repartition(2);
        System.out.println(repartitionRDD);

        jsc.close();
        session.stop();
    }
}
