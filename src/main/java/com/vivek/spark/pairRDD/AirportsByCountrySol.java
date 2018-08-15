package com.vivek.spark.pairRDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

/**
 * Created by Vivek Kumar Mishra on 08/06/2018.
 */
public class AirportsByCountrySol {
    private static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        Logger.getLogger(AirportsByCountrySol.class).setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("AirportMap").setMaster("local[*]");

        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> stringJavaRDD = context.textFile("data/airports.text");

        JavaPairRDD<String, String> mapToPair =
                stringJavaRDD.mapToPair(line -> new Tuple2<>(line.split(COMMA_DELIMITER)[3], line.split(COMMA_DELIMITER)[1]));

        Map<String, Iterable<String>> stringIterableMap = mapToPair.groupByKey().collectAsMap();
    }
}
