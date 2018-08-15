package com.vivek.spark.pairRDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

/**
 * Created by Vivek Kumar Mishra on 07/06/2018.
 */
public class MapValuesExample {

    private static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        Logger.getLogger(MapValuesExample.class).setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("MapValues").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> stringJavaRDD = context.textFile("data/airports.text");

        JavaPairRDD<String, String> mapOfCityAndAirport = stringJavaRDD.mapToPair(getMapOfCityAndAirport());

        Map<String, String> collectedAsMap = mapOfCityAndAirport.mapValues(country -> country.toUpperCase()).collectAsMap();

        collectedAsMap.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() +" : "+ entry.getValue());
        });
    }

    private static PairFunction<String, String, String> getMapOfCityAndAirport() {
        return line -> new Tuple2<>(line.split(COMMA_DELIMITER)[1], line.split(COMMA_DELIMITER)[3]);
    }
}
