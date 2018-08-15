package com.vivek.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;


/**
 * Created by Vivek Kumar Mishra on 09/06/2018.
 */
public class Accumlators {
    private static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Accum").setMaster("local[*]");
        SparkContext sc = new SparkContext(conf);

        JavaSparkContext context = new JavaSparkContext(sc);

        final LongAccumulator total = new LongAccumulator();
        final LongAccumulator missing_records = new LongAccumulator();
        final LongAccumulator bytesProcessed = new LongAccumulator();

        total.register(sc, Option.apply("total"), false);
        missing_records.register(sc, Option.apply("missing_records"), false);
        bytesProcessed.register(sc,Option.apply("Bytes"),true);

        JavaRDD<String> javaRDD = context.textFile("data/2016-stack-overflow-survey-responses.csv");

        JavaRDD<String> filter = javaRDD.filter(line -> {
            bytesProcessed.add(line.getBytes().length);
            String[] splits = line.split(COMMA_DELIMITER, -1);
            total.add(1);

            if (splits[14].isEmpty()) {
                missing_records.add(1);
            }
            return splits[2].equals("Canada");
        });

        System.out.println("Counts for Canada is: " + filter.count());
        System.out.println("Total records processed: "+ total.value());
        System.out.println("Total missing records: " + missing_records.value());
        System.out.println("Total bytes processed: " + bytesProcessed.value());
    }
}
