package com.spark.java.operator.transformation;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class RightOuterJoin {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("RightOuterJoin")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        final Random random = new Random();
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        JavaPairRDD<Integer, Integer> javaPairRDD = javaRDD.mapToPair(
                (PairFunction<Integer, Integer, Integer>) integer -> new Tuple2<>(integer, random.nextInt(10)));

        //右关联
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> rightJoinRDD = javaPairRDD.rightOuterJoin(javaPairRDD);
        System.out.println(rightJoinRDD);

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> rightJoinRDD1 = javaPairRDD.rightOuterJoin(javaPairRDD,
                2);
        System.out.println(rightJoinRDD1);

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> rightJoinRDD2 = javaPairRDD.rightOuterJoin(javaPairRDD,
                new Partitioner() {
                    @Override
                    public int numPartitions() {
                        return 2;
                    }

                    @Override
                    public int getPartition(Object key) {
                        return (key.toString()).hashCode() % numPartitions();
                    }
                });
        System.out.println(rightJoinRDD2);

        jsc.close();
        session.stop();
    }
}
