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
 * Created by Vivek Kumar Mishra on 01/06/2018.
 */
public class UkMakerSpaces {
    private static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        Logger.getLogger(UkMakerSpaces.class).setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("UkMakers").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        final Broadcast<Map<String, String>> postCodeMap = context.broadcast(loadPostCode());

        JavaRDD<String> makerSpaceRDD = context.textFile("data/uk-makerspaces-identifiable-data.csv");

        JavaRDD<String> regions = makerSpaceRDD.filter(line -> !line.split(COMMA_DELIMITER, -1)[0].equals("TimeStamp")).map(line -> {
            Optional<String> postPrefix = getPostPrefix(line);
            if (postPrefix.isPresent() && postCodeMap.value().containsKey(postPrefix.get())) {
                return postCodeMap.value().get(postPrefix.get());
            }
            return "unKnown";
        });

        regions.countByValue().entrySet().stream()
                .forEach(entry -> System.out.println(entry.getKey() + " : " + entry.getValue()));
    }

    private static Optional<String> getPostPrefix(String line) {
        String[] split = line.split(COMMA_DELIMITER, -1);
        String code = split[4];
        if (code.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(code.split(" ")[0]);
    }

    private static Map<String, String> loadPostCode() {
        Map<String, String> postCode = new HashMap<>();
        try (Stream<String> lines = Files.lines(Paths.get("data/uk-postcode.csv"))) {
            lines.forEach(line -> {
                String[] split = line.split(COMMA_DELIMITER, -1);
                postCode.put(split[0], split[7]);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return postCode;
    }
}
