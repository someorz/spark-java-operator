package com.spark.java.operator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/17
 **/
public class Join {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("Join")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Tuple2<Integer, String>> studentsList = Arrays.asList(
                new Tuple2<>(1, "xufengnian"),
                new Tuple2<>(2, "xuyao"),
                new Tuple2<>(2, "wangchudong"),
                new Tuple2<>(3, "laohuang")
        );

        List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 90),
                new Tuple2<>(3, 80),
                new Tuple2<>(1, 101),
                new Tuple2<>(2, 91),
                new Tuple2<>(3, 81),
                new Tuple2<>(3, 71)
        );


        JavaPairRDD<Integer, String> studentsRDD = jsc.parallelizePairs(studentsList);
        JavaPairRDD<Integer, Integer> scoresRDD = jsc.parallelizePairs(scoresList);

        /*
        前面数据
        (1,xufengnian)(2,xuyao)(2,"wangchudong")(3,laohuang)
        (1,100)(2,90)(3,80)(1,101)(2,91)(3,81)(3,71)
        join之后：
        (1,(xufengnian,100))(1,(xufengnian,101))(3,(laohuang,80))(3,(laohuang,81))(3,(laohuang,71))
        (2,(xuyao,90))(2,(xuyao,91))(2,(wangchudong,90))(2,(wangchudong,91))
        */
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = studentsRDD.join(scoresRDD);


        List<Tuple2<Integer, Tuple2<String, Integer>>> collect = studentScores.collect();

        System.out.println(collect);


        jsc.close();
        session.stop();
    }

}
