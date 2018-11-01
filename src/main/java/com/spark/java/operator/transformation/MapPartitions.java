package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.StreamSupport;

/**
 * mapPartitions函数会对每个分区依次调用分区函数处理，然后将处理的结果(若干个Iterator)生成新的RDDs。
 * mapPartitions与map类似，但是如果在映射的过程中需要频繁创建额外的对象，使用mapPartitions要比map高效的过。
 * 比如，将RDD中的所有数据通过JDBC连接写入数据库，如果使用map函数，可能要为每一个元素都创建一个connection，
 * 这样开销很大，如果使用mapPartitions，那么只需要针对每一个分区建立一个connection。
 *
 * @author machi
 * @create 2018/05/21
 **/
public class MapPartitions {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("MapPartitions")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        //RDD有两个分区
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 2);
        //计算每个分区的合计
        JavaRDD<Integer> rdd1 = javaRDD.mapPartitions(
                (FlatMapFunction<Iterator<Integer>, Integer>) integerIterator -> {
                    List<Integer> linkedList = new LinkedList<>();
                    while (integerIterator.hasNext()) {
                        linkedList.add(integerIterator.next()+1);
                    }
                    return linkedList.iterator();
                });

        System.out.println(rdd1.collect());
        //计算每个分区的合计
        JavaRDD<Integer> rdd2 = javaRDD.mapPartitions(
                (FlatMapFunction<Iterator<Integer>, Integer>) integerIterator -> {
                    Iterable<Integer> iterator = () -> integerIterator;

                    new Iterable<Integer>() {
                        @Override
                        public Iterator<Integer> iterator() {
                            return integerIterator;
                        }
                    };

                    return StreamSupport.stream(iterator.spliterator(), false)
                            .map(t -> ++t)
                            .iterator();
                });

        System.out.println(rdd2.collect());
        jsc.close();
        session.stop();
    }
}
