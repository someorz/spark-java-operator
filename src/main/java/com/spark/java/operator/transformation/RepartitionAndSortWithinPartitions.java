package com.spark.java.operator.transformation;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

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

        List<Tuple2< PartKey,Tuple3<Integer,Integer,Integer>>> data = Arrays.asList(
                Tuple2.apply(new PartKey(1L,1D),Tuple3.apply(1,1,1)),
                Tuple2.apply(new PartKey(1L,2D),Tuple3.apply(2,1,2)),
                Tuple2.apply(new PartKey(1L,3D),Tuple3.apply(3,1,3)),
                Tuple2.apply(new PartKey(2L,5D),Tuple3.apply(4,2,4)),
                Tuple2.apply(new PartKey(2L,4D),Tuple3.apply(5,2,5)));
        JavaPairRDD<PartKey, Tuple3<Integer, Integer, Integer>> javaRDD = jsc.parallelizePairs(data);

        JavaPairRDD<PartKey, Tuple3<Integer, Integer, Integer>> RepartitionAndSortWithPartitionsRDD = javaRDD.repartitionAndSortWithinPartitions(
                new StudentPartitioner(10) , new Comp());

        System.out.println(RepartitionAndSortWithPartitionsRDD.collect());

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
        private Long mid;
        private Double avg;

        public PartKey(Long mid, Double avg) {
            this.mid = mid;
            this.avg = avg;
        }

        public Long getMid() {
            return mid;
        }

        public void setMid(Long mid) {
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
                key1.setMid(0L);
            }
            return key1.getMid().hashCode() % partitionsNum;
        }
    }
}
