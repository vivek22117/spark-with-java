package com.vivek.spark.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by Vivek Kumar Mishra on 07/06/2018.
 */
public class WordCountProblem {

    public static void main(String[] args) {
        Logger.getLogger(WordCountProblem.class).setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> wordRDD = context.textFile("data/word_count.text");
        JavaRDD<String> stringJavaRDD = wordRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        Map<String, Long> stringLongMap = stringJavaRDD.countByValue();

        stringLongMap.entrySet().stream().forEach(entry -> {
            System.out.println(entry.getKey() +" : " +entry.getValue());
        });
    }
}
