package com.siva.standalone;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Sivakumar on 26/4/15.
 */
public class PropertiesTest {
    public static void main(String args[]) throws IOException {
        String filename = "/home/Sivakumar/workspace/MapReduce/src/main/resources/test.tsv";
        Properties properties = new Properties();
        FileInputStream fileInputStream = new FileInputStream(filename);
        properties.load(fileInputStream);
        System.out.println("Key:hadoop  Value:"+properties.getProperty("hadoop"));
        System.out.println("Key:java  Value:"+properties.getProperty("java"));
        System.out.println("Key:php  Value:"+properties.getProperty("php"));
    }
}
