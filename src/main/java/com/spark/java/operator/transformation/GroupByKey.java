package com.spark.java.operator.transformation;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class GroupByKey {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("GroupByKey")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 2, 4, 4, 5, 6, 6, 6);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        //转为k，v格式
        JavaPairRDD<Integer, Integer> javaPairRDD = javaRDD.mapToPair(
                (PairFunction<Integer, Integer, Integer>) integer -> new Tuple2<>(integer, 1));

        JavaPairRDD<Integer, Iterable<Integer>> groupByKeyRDD = javaPairRDD.groupByKey(2);

        System.out.println(groupByKeyRDD.collect());

        //自定义partition
        JavaPairRDD<Integer, Iterable<Integer>> groupByKeyRDD3 = javaPairRDD.groupByKey(new Partitioner() {
            //partition各数
            @Override
            public int numPartitions() {
                return 10;
            }

            //partition方式
            @Override
            public int getPartition(Object o) {
                return (o.toString()).hashCode() % numPartitions();
            }
        });
        System.out.println(groupByKeyRDD3.collect());

        jsc.stop();
        session.stop();
    }

}
