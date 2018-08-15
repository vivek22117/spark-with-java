package com.vivek.spark.rdd;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Vivek Kumar Mishra on 07/06/2018.
 */
public class AirportLatSolution {

    private static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
    
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LatSolution").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> mapOfAirportAndLat = context.textFile("data/airports.text")
                .filter(line -> Float.valueOf(line.split(COMMA_DELIMITER)[6]) > 40)
                .map(line -> {
                    String[] split = line.split(COMMA_DELIMITER);
                    return StringUtils.join(new String[]{split[1], split[6], ","});
                });

        mapOfAirportAndLat.take(10).forEach(System.out::println);
    }
}
