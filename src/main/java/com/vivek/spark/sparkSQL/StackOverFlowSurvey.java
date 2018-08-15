package com.vivek.spark.sparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Vivek Kumar Mishra on 01/06/2018.
 */
public class StackOverFlowSurvey {
    private static final String AGE_MIDPOINT = "age_midpoint";
    private static final String SALARY_MIDPOINT = "salary_midpoint";

    public static void main(String[] args) {
        Logger.getLogger(StackOverFlowSurvey.class).setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("Survey").master("local[*]").getOrCreate();

        DataFrameReader reader = session.read();
        Dataset<Row> dataset = reader.option("header", true).csv("data/2016-stack-overflow-survey-responses.csv");

        dataset.printSchema();
        dataset.show(20);

        dataset.select(col("so_region"), col("salary_midpoint"));
//        dataset.filter(col("country").equalTo("Algeria")).show();

        RelationalGroupedDataset relationalDataset = dataset.groupBy(col("occupation"));
        relationalDataset.count().show();

        Dataset<Row> rowDataset = dataset.withColumn(AGE_MIDPOINT, col(AGE_MIDPOINT).cast("integer"))
                                    .withColumn(SALARY_MIDPOINT, col(SALARY_MIDPOINT).cast("integer"));

        rowDataset.printSchema();
//        rowDataset.filter(col(AGE_MIDPOINT).$less(20)).show();

//        rowDataset.orderBy(col(AGE_MIDPOINT).desc()).show();

//        dataset.groupBy(col("country")).agg(avg(SALARY_MIDPOINT),max(AGE_MIDPOINT)).show();


        StructType arraySchema = new StructType(new StructField[]{
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("state", DataTypes.StringType, true)
        });

        StructType schema = new StructType( new StructField[] {
                DataTypes.createStructField("firstName", DataTypes.StringType, true),
                DataTypes.createStructField("lastName", DataTypes.StringType, true)});



        List<StructField> employeeFields = new ArrayList<>();
        employeeFields.add(DataTypes.createStructField("firstName", DataTypes.StringType, true));
        employeeFields.add(DataTypes.createStructField("lastName", DataTypes.StringType, true));

        List<StructField> addressFields = new ArrayList<>();
        addressFields.add(DataTypes.createStructField("city", DataTypes.StringType, true));
        addressFields.add(DataTypes.createStructField("state", DataTypes.StringType, true));
        ArrayType addressStruct = DataTypes.createArrayType(DataTypes.createStructType(addressFields));

        employeeFields.add(DataTypes.createStructField("addresses", addressStruct, true));
        StructType employeeSchema = DataTypes.createStructType(employeeFields);

        Dataset<Row> json = session.read().format("json").schema(employeeSchema).load("data/data_json.json");
        json.show();
        json.printSchema();


        session.stop();

    }
}
