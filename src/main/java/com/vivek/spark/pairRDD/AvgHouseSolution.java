package com.vivek.spark.pairRDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by Vivek Kumar Mishra on 08/06/2018.
 */
public class AvgHouseSolution {
    public static void main(String[] args) {
        Logger.getLogger(AvgHouseSolution.class).setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("HouseSolution").setMaster("local[*]");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = context.textFile("data/RealEstate.csv");
        JavaPairRDD<String, AvgCount> mapToPair = javaRDD.filter(line -> !line.contains("Bedrooms"))
                .mapToPair(line -> new Tuple2<>(line.split(",")[3], new AvgCount(1, Double.parseDouble(line.split(",")[2]))));


        JavaPairRDD<String, AvgCount> stringAvgCountJavaPairRDD =
                mapToPair.reduceByKey((x, y) -> new AvgCount(x.getCount() + y.getCount(), x.getPrice() + y.getPrice()));

        JavaPairRDD<String, Double> stringDoubleJavaPairRDD =
                stringAvgCountJavaPairRDD.mapValues(avgCount -> avgCount.getPrice() / avgCount.getCount());

        stringDoubleJavaPairRDD.collectAsMap().entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        });
    }
}
