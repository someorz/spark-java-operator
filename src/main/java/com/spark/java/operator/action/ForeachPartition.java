package com.spark.java.operator.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class ForeachPartition {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("ForeachPartition")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);

        //获得分区ID
        JavaRDD<String> partitionRDD = javaRDD.mapPartitionsWithIndex(
                (Function2<Integer, Iterator<Integer>, Iterator<String>>) (v1, v2) -> {
                    List<String> linkedList = new LinkedList<>();
                    while (v2.hasNext()) {
                        linkedList.add(v1 + "=" + v2.next());
                    }
                    return linkedList.iterator();
                }, false);
        System.out.println(partitionRDD.collect());
        javaRDD.foreachPartition((VoidFunction<Iterator<Integer>>) integerIterator -> {
            System.out.println("___________begin_______________");
            while (integerIterator.hasNext()) {
                System.out.print(integerIterator.next() + "      ");
            }
            System.out.println("\n___________end_________________");
        });
        jsc.close();
        session.stop();
    }
}
