package com.spark.java.operator.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class Fold {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("Fold")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<String> data = Arrays.asList("5", "1", "1", "3", "6", "2", "2");
        JavaRDD<String> javaRDD = jsc.parallelize(data, 5);
        JavaRDD<String> partitionRDD = javaRDD.mapPartitionsWithIndex(
                (Function2<Integer, Iterator<String>, Iterator<String>>) (v1, v2) -> {
                    List<String> linkedList = new LinkedList<>();
                    while (v2.hasNext()) {
                        linkedList.add(v1 + "=" + v2.next());
                    }
                    return linkedList.iterator();
                }, false);

        System.out.println(partitionRDD.collect());

        String foldRDD = javaRDD.fold("0", (Function2<String, String, String>) (v1, v2) -> v1 + " - " + v2);
        System.out.println(foldRDD);

        jsc.close();
        session.stop();
    }
}
