package com.spark.java.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
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


        List<Tuple2<Integer,String>> namesList=Arrays.asList(
                new Tuple2<>(1, "Spark"),
                new Tuple2<>(3, "Tachyon"),
                new Tuple2<>(4, "Sqoop"),
                new Tuple2<>(2, "Hadoop"),
                new Tuple2<>(2, "Hadoop2")
        );

        List<Tuple2<Integer,Integer>> scoresList=Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(3, 70),
                new Tuple2<>(3, 77),
                new Tuple2<>(2, 90),
                new Tuple2<>(2, 80)
        );
        JavaPairRDD<Integer, String> names=jsc.parallelizePairs(namesList);
        JavaPairRDD<Integer, Integer> scores=jsc.parallelizePairs(scoresList);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> nameScores=names.cogroup(scores);

        nameScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            private static final long serialVersionUID = 1L;
            int i=1;
            @Override
            public void call(
                    Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
                    throws Exception {
                String string="ID:"+t._1+" , "+"Name:"+t._2._1+" , "+"Score:"+t._2._2;
                string+="     count:"+i;
                System.out.println(string);
                i++;
            }
        });

        jsc.close();
        session.stop();
    }

}
