package com.vivek.spark.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by Vivek Kumar Mishra on 07/06/2018.
 */
public class PrimeSum {
    public static void main(String[] args) {
        Logger.getLogger("Prime").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("PrimeSum").setMaster("local[*]");
        JavaSparkContext context  = new JavaSparkContext(conf);

        Integer reduce = context.textFile("data/prime_nums.text")
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .filter(num -> !num.isEmpty())
                .map(num -> Integer.valueOf(num)).reduce((x, y) -> x + y);

        System.out.println(reduce);
    }
}
