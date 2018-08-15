package com.vivek.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;

import java.io.Serializable;

/**
 * Created by Vivek Kumar Mishra on 22/07/2018.
 */
public abstract class SharedSparkSession  implements Serializable{

    private static final long serialVersionUID = -6728856777064443536L;

    protected transient SparkSession spark;
    protected transient JavaSparkContext sparkContext;


    @Before
    public void setUp(){
        spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();
        sparkContext = new JavaSparkContext(spark.sparkContext());
    }

    @After
    public void tearDown(){
        try {
            spark.stop();
            spark = null;

        } finally {
            SparkSession.clearDefaultSession();
            SparkSession.clearDefaultSession();
        }
    }
}
