package com.vivek.spark.pairRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Vivek Kumar Mishra on 04/06/2018.
 */
public class PairRDDFromTupleList {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PairRDD").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> tuple2List =
                Arrays.asList(new Tuple2<>("Vivek", 32),
                                new Tuple2<>("Harsha", 27),
                                new Tuple2<>("Arti", 33),
                                new Tuple2<>("Viaksh", 28));

        JavaPairRDD<String, Integer> pairRDD = context.parallelizePairs(tuple2List);

//        parallelize.saveAsTextFile("out/pairRDD");
        pairRDD.take(2).forEach(System.out::println);
    }
}
