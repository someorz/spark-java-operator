package com.spark.java.operator.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author machi
 * @create 2018/05/14
 **/
public class TakeOrdered {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("TakeOrdered")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());


        List<Integer> data = Arrays.asList(5, 1, 0, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);
        System.out.println("takeOrdered-----1-------------" + javaRDD.takeOrdered(2));

        List<Integer> list = javaRDD.takeOrdered(2, new TakeOrderedComparator());
        System.out.println("takeOrdered----2--------------" + list);

        jsc.close();
        session.stop();
    }

    //注意comparator需要序列化
    public static class TakeOrderedComparator implements Serializable, Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            return -o1.compareTo(o2);
        }
    }


}
