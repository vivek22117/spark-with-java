package com.vivek.spark.broadcast;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Created by Vivek Kumar Mishra on 09/06/2018.
 */
public class UkMakerSpaceII {

    private static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {

        Logger.getLogger(UkMakerSpaces.class).setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("UKSpace").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        Broadcast<Map<String, String>> postCodeMap = context.broadcast(getPostCode());

        JavaRDD<String> javaRDD = context.textFile("data/uk-makerspaces-identifiable-data.csv");

        JavaRDD<String> regions = javaRDD.filter(line -> !line.contains("Timestamp")).map(line -> {
            Optional<String> code = postPrefix(line);
            if (code.isPresent() && postCodeMap.value().containsKey(code.get())) {
                return postCodeMap.value().get(code.get());
            }
            return "unKnown";
        });
        regions.countByValue().entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        });

    }

    private static Optional<String> postPrefix(String line) {
        String[] splits = line.split(COMMA_DELIMITER, -1);
        String postCode = splits[4];
        if(postCode.isEmpty()){
            return Optional.empty();
        }
        return Optional.of(postCode.split(" ")[0]);
    }

    private static Map<String, String> getPostCode() {
        Map<String, String> postCode = new HashMap<>();
        try(Stream<String> lines = Files.lines(Paths.get("data/uk-postcode.csv"))){
            lines.forEach(line ->{
                String[] splits = line.split(COMMA_DELIMITER, -1);
                postCode.put(splits[0],splits[7]);
            });
        } catch (IOException e){
            e.printStackTrace();
        }
        return postCode;
    }
}
