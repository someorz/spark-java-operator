package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class ZipPartitions {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("ZipPartitions")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);
        List<Integer> data1 = Arrays.asList(3, 2, 12, 5, 6, 1);
        JavaRDD<Integer> javaRDD1 = jsc.parallelize(data1, 3);
        JavaRDD<String> zipPartitionsRDD = javaRDD.zipPartitions(javaRDD1,
                (FlatMapFunction2<Iterator<Integer>, Iterator<Integer>, String>) (integerIterator, integerIterator2) -> {
                    List<String> linkedList = new LinkedList<>();
                    while (integerIterator.hasNext() && integerIterator2.hasNext()) {
                        linkedList.add(integerIterator.next().toString() + "_" + integerIterator2.next().toString());
                    }
                    return linkedList.iterator();
                });
        System.out.println(zipPartitionsRDD.collect());

        jsc.close();
        session.stop();
    }
}
