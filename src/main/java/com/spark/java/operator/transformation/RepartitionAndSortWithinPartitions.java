package com.spark.java.operator.transformation;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.internal.Trees;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class RepartitionAndSortWithinPartitions {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("RepartitionAndSortWithinPartitions")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Tuple2<PartKey,Tuple3<Integer,Integer,Integer>>> data = Arrays.asList(
                Tuple2.apply(new PartKey(1,1D),Tuple3.apply(1,1,1)),
                Tuple2.apply(new PartKey(1,2D),Tuple3.apply(1,2,1)),
                Tuple2.apply(new PartKey(1,3D),Tuple3.apply(1,3,1)),
                Tuple2.apply(new PartKey(1,4D),Tuple3.apply(1,4,1)),
                Tuple2.apply(new PartKey(2,1D),Tuple3.apply(2,1,2)),
                Tuple2.apply(new PartKey(2,2D),Tuple3.apply(2,2,2)),
                Tuple2.apply(new PartKey(2,3D),Tuple3.apply(2,3,2)),
                Tuple2.apply(new PartKey(2,4D),Tuple3.apply(2,4,2)),
                Tuple2.apply(new PartKey(3,1D),Tuple3.apply(3,1,3)),
                Tuple2.apply(new PartKey(4,2D),Tuple3.apply(4,2,4)),
                Tuple2.apply(new PartKey(5,2D),Tuple3.apply(5,2,5)));
        JavaPairRDD<PartKey, Tuple3<Integer, Integer, Integer>> javaRDD = jsc.parallelizePairs(data);

        JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> integerTuple3JavaPairRDD = javaRDD.repartitionAndSortWithinPartitions(
                new StudentPartitioner(10), new Comp()).mapPartitionsToPair(
                (PairFlatMapFunction<Iterator<Tuple2<PartKey, Tuple3<Integer, Integer, Integer>>>, Integer, Tuple3<Integer, Integer, Integer>>) tuple2Iterator -> {
                    Iterable<Tuple2<PartKey, Tuple3<Integer, Integer, Integer>>> iterator = () -> tuple2Iterator;
                    return StreamSupport.stream(iterator.spliterator(), false)
                            .map(t -> new Tuple2<>(t._2._1(), t._2)).iterator();
                });

        System.out.println(integerTuple3JavaPairRDD.collect());
        JavaPairRDD<Integer, List<Tuple3<Integer, Integer, Integer>>> combineByKeyRDD = integerTuple3JavaPairRDD.combineByKey(
                (Function<Tuple3<Integer, Integer, Integer>, List<Tuple3<Integer, Integer, Integer>>>) s -> {
                    List<Tuple3<Integer, Integer, Integer>> collect = Stream.of(s).collect(Collectors.toList());
                    return collect;
                },
                (Function2<List<Tuple3<Integer, Integer, Integer>>, Tuple3<Integer, Integer, Integer>, List<Tuple3<Integer, Integer, Integer>>>) (strings, s) -> {
                    strings.add(s);
                    return strings;
                }, (Function2<List<Tuple3<Integer, Integer, Integer>>, List<Tuple3<Integer, Integer, Integer>>, List<Tuple3<Integer, Integer, Integer>>>) (strings, strings2) -> {
                    strings.addAll(strings2);
                    return strings;
                });
        System.out.println(combineByKeyRDD.collect());
        jsc.close();
        session.stop();
    }


    private static class Comp implements Comparator<PartKey>,Serializable{
        @Override
        public int compare(PartKey o1, PartKey o2) {
            return o1.getAvg().compareTo(o2.getAvg());
        }
    }


    static class PartKey implements Serializable {
        private Integer mid;
        private Double avg;

        public PartKey(Integer mid, Double avg) {
            this.mid = mid;
            this.avg = avg;
        }

        public Integer getMid() {
            return mid;
        }

        public void setMid(Integer mid) {
            this.mid = mid;
        }

        public Double getAvg() {
            return avg;
        }

        public void setAvg(Double avg) {
            this.avg = avg;
        }
    }


    static class StudentPartitioner extends Partitioner {

        private int partitionsNum;

        public StudentPartitioner(int partitionsNum) {
            this.partitionsNum = partitionsNum;
        }

        @Override
        public int numPartitions() {
            return partitionsNum;
        }

        @Override
        public int getPartition(Object key) {
            PartKey key1 = (PartKey) key;
            if (key1.getMid() == null) {
                key1.setMid(0);
            }
            return key1.getMid().hashCode() % partitionsNum;
        }
    }
}
