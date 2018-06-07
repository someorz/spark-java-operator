package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * mapPartitionsWithIndex与mapPartition基本相同，只是在处理函数的参数是一个二元元组，
 * 元组的第一个元素是当前处理的分区的index，元组的第二个元素是当前处理的分区元素组成的Iterator。
 *
 * @author machi
 * @create 2018/05/21
 **/
public class MapPartitionsWithIndex {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("MapPartitionsWithIndex")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        //RDD有两个分区
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 2);
        //分区index、元素值、元素编号输出
        JavaRDD<String> mapPartitionsWithIndexRDD = javaRDD.mapPartitionsWithIndex(
                (Function2<Integer, Iterator<Integer>, Iterator<String>>) (v1, v2) -> {
                    List<String> linkedList = new LinkedList<>();
                    int i = 0;
                    while (v2.hasNext()) {
                        linkedList.add(Integer.toString(v1) + "|" + v2.next().toString() + Integer.toString(i++));
                    }
                    return linkedList.iterator();
                }, false);

        System.out.println(mapPartitionsWithIndexRDD.collect());

        jsc.close();
        session.stop();
    }
}
