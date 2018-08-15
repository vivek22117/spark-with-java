package com.vivek.spark.sparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.LongType;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Vivek Kumar Mishra on 09/06/2018.
 */
public class AvgHouseSQLSol {
    private static final String PRICE = "Price";
    private static final String PRICE_SQ_FEET = "Price SQ Ft";

    public static void main(String[] args) {
        Logger.getLogger(AvgHouseSQLSol.class).setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("HouseSol").master("local[*]").getOrCreate();

        Dataset<Row> dataset = session.read().option("header", "true").csv("data/RealEstate.csv");

        Dataset<Row> rowDataset = dataset.withColumn(PRICE, col(PRICE).cast("long"))
                                        .withColumn(PRICE_SQ_FEET, col(PRICE_SQ_FEET).cast("long"));

        rowDataset.groupBy(col("Location")).agg(avg(PRICE_SQ_FEET),max(PRICE))
                .withColumnRenamed("avg("+PRICE_SQ_FEET+")","avgPrice")
        .orderBy(col("avgPrice").desc()).show();
    }
}
