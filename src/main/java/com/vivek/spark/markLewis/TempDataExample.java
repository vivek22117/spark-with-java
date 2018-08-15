package com.vivek.spark.markLewis;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

/**
 * Created by Vivek Kumar Mishra on 06/06/2018.
 */
public class TempDataExample {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().appName("TempData").master("local[*]").getOrCreate();

        JavaRDD<Row> dataRDD = session.read().option("header", true).csv("data/MMDataFile1.csv").javaRDD();


        Encoder<TempData> tempDataEncoder = Encoders.bean(TempData.class);
        Dataset<TempData> dataDataset = session.read().option("header", true).csv("data/MMDataFile1.csv").as(tempDataEncoder);

        dataDataset.show();

        dataRDD.take(10).forEach(System.out::println);

    }
}
