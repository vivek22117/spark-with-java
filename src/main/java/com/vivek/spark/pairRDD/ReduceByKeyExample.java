package com.vivek.spark.pairRDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import javax.xml.parsers.SAXParser;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by Vivek Kumar Mishra on 07/06/2018.
 */
public class ReduceByKeyExample {
    public static void main(String[] args) {
        Logger.getLogger(ReduceByKeyExample.class).setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("ReduceByKey").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = context.textFile("data/word_count.text");

        JavaPairRDD<String, Integer> javaPairRDD = javaRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1));

        Map<String, Integer> wordCountMap = javaPairRDD.reduceByKey((x, y) -> x + y).collectAsMap();


    }
}
