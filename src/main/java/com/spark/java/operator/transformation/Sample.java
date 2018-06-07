package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * 返回抽样的样本的子集。
 *
 * @author machi
 * @create 2018/05/21
 **/
public class Sample {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("Sample")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        //false   是伯努利分布  (元素可以多次采样);0.2   采样比例;100   随机数生成器的种子
        JavaRDD<Integer> sampleRDD = javaRDD.sample(false, 0.2, 100);
        System.out.println("sampleRDD~~~~~~~~~~~~~~~~~~~~~~~~~~" + sampleRDD.collect());
        //true  是柏松分布;0.2   采样比例;100   随机数生成器的种子
        JavaRDD<Integer> sampleRDD1 = javaRDD.sample(true, 0.2, 100);
        System.out.println("sampleRDD1~~~~~~~~~~~~~~~~~~~~~~~~~~" + sampleRDD1.collect());

        jsc.close();
        session.stop();
    }
}
