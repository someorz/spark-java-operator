package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。
 * 和aggregate函数类似，aggregateByKey返回值的类型不需要和RDD中value的类型一致。因为aggregateByKey是对相同Key中的值进行聚合操作，
 * 所以aggregateByKey函数最终返回的类型还是Pair RDD，对应的结果是Key和聚合好的值
 *
 * @author machi
 * @create 2018/05/14
 **/
public class AggregateByKey {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("AggregateByKey")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Tuple2<Integer, String>> list = new ArrayList<>();

        list.add(new Tuple2<>(1, "aaa"));
        list.add(new Tuple2<>(1, "bbb"));
        list.add(new Tuple2<>(1, "ccc"));
        list.add(new Tuple2<>(2, "aaa"));
        list.add(new Tuple2<>(2, "ddd"));
        list.add(new Tuple2<>(2, "eee"));
        list.add(new Tuple2<>(3, "bbb"));

        JavaPairRDD<Integer, String> data = jsc.parallelizePairs(list);

        JavaPairRDD<Integer, List<String>> result = data.aggregateByKey(new ArrayList<>(), (c, v) -> {
            c.add(v);
            return c;
        }, (Function2<List<String>, List<String>, List<String>>) (c1, c2) -> {
            c1.addAll(c2);
            return c1;
        });

        result.collect().forEach(System.out::println);

        jsc.close();
        session.stop();
    }
}
