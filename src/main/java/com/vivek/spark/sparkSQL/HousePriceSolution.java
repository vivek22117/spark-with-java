package com.vivek.spark.sparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Vivek Kumar Mishra on 01/06/2018.
 */
public class HousePriceSolution {

    private static final String PRICE = "Price";
    private static final String PRICE_SQ_FEET = "Price SQ Ft";

    public static void main(String[] args) {
        Logger.getLogger(HousePriceSolution.class).setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("HouseProblem").master("local[*]").getOrCreate();

        DataFrameReader reader = session.read();
        Dataset<Row> dataset = reader.option("header", "true").csv("data/RealEstate.csv");


        Dataset<Row> rowDataset = dataset.withColumn(PRICE, col(PRICE).cast(DataTypes.LongType))
                .withColumn(PRICE_SQ_FEET, col(PRICE_SQ_FEET).cast(DataTypes.LongType));

        rowDataset.groupBy(col("Location")).agg(avg(PRICE_SQ_FEET), max(PRICE))
                .withColumnRenamed("avg("+PRICE_SQ_FEET+")","PerFeet").orderBy(col("PerFeet").desc()).show();
    }
}