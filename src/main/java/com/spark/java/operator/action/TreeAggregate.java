package com.spark.java.operator.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class TreeAggregate {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("TreeAggregate")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);
        //转化操作
        JavaRDD<String> javaRDD1 = javaRDD.map((Function<Integer, String>) v1 -> Integer.toString(v1));

        String result1 = javaRDD1.treeAggregate("0", (Function2<String, String, String>) (v1, v2) -> {
            System.out.println(v1 + "=seq=" + v2);
            return v1 + "=seq=" + v2;
        }, (Function2<String, String, String>) (v1, v2) -> {
            System.out.println(v1 + "<=comb=>" + v2);
            return v1 + "<=comb=>" + v2;
        });
        System.out.println(result1);

        jsc.close();
        session.stop();
    }
}
