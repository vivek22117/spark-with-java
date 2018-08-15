package com.vivek.spark.pairRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by Vivek Kumar Mishra on 07/06/2018.
 */
public class PairRDDFromRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PairRDD").setMaster("local[2]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = context.parallelize(Arrays.asList("Vivek 24", "Arti 34", "Harsha 27", "Viaksh 27"));
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = javaRDD.mapToPair(getNameAndAgePair());

        stringIntegerJavaPairRDD.collectAsMap().entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        });

        stringIntegerJavaPairRDD.saveAsTextFile("out/pari_rdd.text");
    }

    private static PairFunction<String,String,Integer> getNameAndAgePair() {
        return s -> new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
    }
}
