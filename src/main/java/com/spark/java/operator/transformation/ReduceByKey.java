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

/**
 * @author machi
 * @create 2018/05/21
 **/
public class ReduceByKey {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("ReduceByKey")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);

        //转化为K，V格式
        JavaPairRDD<Integer, Integer> javaPairRDD = javaRDD.mapToPair(
                (PairFunction<Integer, Integer, Integer>) integer -> new Tuple2<>(integer, 1));
        JavaPairRDD<Integer, Integer> reduceByKeyRDD = javaPairRDD.reduceByKey(
                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        System.out.println(reduceByKeyRDD.collect());

        //指定numPartitions
        JavaPairRDD<Integer, Integer> reduceByKeyRDD2 = javaPairRDD.reduceByKey(
                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2, 2);

        System.out.println(reduceByKeyRDD2.collect());

        //自定义partition
        JavaPairRDD<Integer, Integer> reduceByKeyRDD4 = javaPairRDD.reduceByKey(new Partitioner() {
            @Override
            public int numPartitions() {
                return 2;
            }

            @Override
            public int getPartition(Object o) {
                return (o.toString()).hashCode() % numPartitions();
            }
        }, (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        System.out.println(reduceByKeyRDD4.collect());

        jsc.close();
        session.stop();
    }
}
