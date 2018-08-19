package com.vivek.spark.utility;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.vivek.spark.markLewis.TempData;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by Vivek Kumar Mishra on 18/08/2018.
 */
public class JsonUtility {

    private static final Logger LOGGER = LogManager.getLogger(JsonUtility.class);
    private ObjectMapper objectMapper;

    public JsonUtility() {
        this((new ObjectMapper()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false));
    }

    public JsonUtility(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public <T> String convertToString(T object) throws JsonProcessingException {
        try{
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException ex){
            LOGGER.error("Unable to convert into string.." + ex.getMessage());
        }
        return null;
    }

    public <T> T convertFromJson(String json, Class<T> type) throws IOException {
        try{
            return objectMapper.readValue(json, type);
        } catch (IOException ex){
            LOGGER.error("Unable to covert from json.." + ex.getMessage());
        }
        return null;
    }

    public <T> T convertCollectionFromJson(String json, Class<T> type) throws IOException {
        TypeFactory typeFactory = objectMapper.getTypeFactory();
        try{
            return objectMapper.readValue(json, typeFactory.constructCollectionType(List.class, TempData.class));
        } catch (IOException ex){
            LOGGER.error("Unable to convert into list..." + ex.getMessage());
        }
        return null;
    }
}