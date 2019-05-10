package com.spark.java.operator.transformation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.SqlBaseParser;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.catalyst.plans.JoinType$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
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

        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        List<Tuple2<Integer, String>> studentsList = Arrays.asList(
                new Tuple2<>(1, "xufengnian"),
                new Tuple2<>(1, "xufengnian"),
                new Tuple2<>(2, "xuyao"),
                new Tuple2<>(3, "laohuang"),
                new Tuple2<>(5, "wang")
        );

        List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 91),
                new Tuple2<>(3, 71),
                new Tuple2<>(4, 71)
        );

        JavaPairRDD<Integer, String> studentsRDD = jsc.parallelizePairs(studentsList);
        JavaPairRDD<Integer, Integer> scoresRDD = jsc.parallelizePairs(scoresList);
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = studentsRDD.join(scoresRDD);

        List<Tuple2<Integer, Tuple2<String, Integer>>> collect = studentScores.collect();

        System.out.println(collect);

        JavaRDD<Row> personRowRdd = studentsRDD.map(person -> RowFactory.create(person._1, person._2));
        List<StructField> fieldList = new ArrayList<>();
        fieldList.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
        fieldList.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        StructType rowAgeNameSchema = DataTypes.createStructType(fieldList);
        final Dataset<Row> personDF = session.createDataFrame(personRowRdd, rowAgeNameSchema);
        personDF.show();
        personDF.printSchema();

        JavaRDD<Row> scoresRowRDD = scoresRDD.map(item -> RowFactory.create(item._1, item._2));
        fieldList = new ArrayList<>();
        fieldList.add(DataTypes.createStructField("sid", DataTypes.IntegerType, false));
        fieldList.add(DataTypes.createStructField("scores", DataTypes.IntegerType, false));
        rowAgeNameSchema = DataTypes.createStructType(fieldList);
        final Dataset<Row> scoresDF = session.createDataFrame(scoresRowRDD, rowAgeNameSchema);
        scoresDF.show();
        scoresDF.printSchema();

        Dataset<Row> joined = personDF.join(scoresDF,
                personDF.col("id").equalTo(scoresDF.col("sid")), "left").
                dropDuplicates("id").drop("sid");
        joined.show();
        joined.printSchema();

        jsc.close();
        session.stop();
    }

}
