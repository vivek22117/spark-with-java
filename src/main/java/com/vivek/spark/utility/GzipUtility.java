package com.vivek.spark.utility;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by Vivek Kumar Mishra on 19/08/2018.
 */
public class GzipUtility {

    public static byte[] compressData(byte[] bytes) {

        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()){
            try(GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)){
                gzipOutputStream.write(bytes);
            }
            System.out.println(outputStream.toByteArray());
            return outputStream.toByteArray();
        } catch (IOException ex){
            throw  new RuntimeException("Failed to zip the file.." + ex.getMessage());
        }
    }

    public static String decompressData(byte[] compressData){

        if(!isZipped(compressData)){
            return new String(compressData);
        }
        try(ByteArrayInputStream inputStream = new ByteArrayInputStream(compressData)){
            try(GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream)){
                try(InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8)){
                    try(BufferedReader bufferedReader = new BufferedReader(inputStreamReader)){
                        StringBuilder builder = new StringBuilder();
                        String line;
                        while((line = bufferedReader.readLine()) != null){
                            builder.append(line);
                        }
                        return builder.toString();
                    }
                }
            }
        } catch (IOException ex) {
            throw  new RuntimeException("Failed to decompress the file.." + ex.getMessage());
        }
    }

    private static boolean isZipped(byte[] compressData) {
        return (compressData[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) && (compressData[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
    }
}
