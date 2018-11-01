package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class ZipWithIndex {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("ZipWithIndex")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Tuple3<Integer,Integer,Integer>> data = Arrays.asList(Tuple3.apply(1,1,1), Tuple3.apply(2,1,2), Tuple3.apply(3,1,3), Tuple3.apply(4,2,4), Tuple3.apply(5,2,5));
        JavaRDD<Tuple3<Integer,Integer,Integer>> javaRDD = jsc.parallelize(data, 3);
        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Long> zipWithIndexRDD = javaRDD.zipWithIndex();
        System.out.println(zipWithIndexRDD.collect());
        jsc.close();
        session.stop();
    }
}
