package com.vivek.spark.rdd;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Vivek Kumar Mishra on 03/06/2018.
 */
public class AirportsUSASolution {
    private static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Airports").setMaster("local[2]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> dataRDD = context.textFile("data/airports.text");
        JavaRDD<String> filteredData = dataRDD.filter(line -> line.split(COMMA_DELIMITER)[3].equals("\"United States\""));

        JavaRDD<String> cityWithAirport = filteredData.map(line -> {
            String[] split = line.split(COMMA_DELIMITER);
            return StringUtils.join(new String[]{split[1], split[2]}, ",");
        });

        cityWithAirport.take(10).forEach(System.out::println);
//        cityWithAirport.saveAsTextFile("out/airports_out.text");
    }
}
