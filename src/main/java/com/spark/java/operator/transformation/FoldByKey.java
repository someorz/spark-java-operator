package com.spark.java.operator.transformation;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author machi
 * @create 2018/05/21
 **/
public class FoldByKey {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("FoldByKey")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 1, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        final Random rand = new Random(10);
        JavaPairRDD<Integer, String> javaPairRDD = javaRDD.mapToPair(
                (PairFunction<Integer, Integer, String>) integer -> new Tuple2<>(integer,
                        Integer.toString(rand.nextInt(10))));

        JavaPairRDD<Integer, String> foldByKeyRDD = javaPairRDD.foldByKey("X",
                (Function2<String, String, String>) (v1, v2) -> v1 + ":" + v2);
        System.out.println(foldByKeyRDD.collect());

        JavaPairRDD<Integer, String> foldByKeyRDD1 = javaPairRDD.foldByKey("X", 2,
                (Function2<String, String, String>) (v1, v2) -> v1 + ":" + v2);
        System.out.println(foldByKeyRDD1.collect());

        JavaPairRDD<Integer, String> foldByKeyRDD2 = javaPairRDD.foldByKey("X", new Partitioner() {
            @Override
            public int numPartitions() {
                return 3;
            }

            @Override
            public int getPartition(Object key) {
                return key.toString().hashCode() % numPartitions();
            }
        }, (Function2<String, String, String>) (v1, v2) -> v1 + ":" + v2);

        System.out.println(foldByKeyRDD2.collect());

        jsc.close();
        session.stop();
    }
}
