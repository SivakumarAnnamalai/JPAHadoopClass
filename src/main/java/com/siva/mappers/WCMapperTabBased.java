package com.siva.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Sivakumar on 24/4/15.
 */
public class WCMapperTabBased extends Mapper<LongWritable,Text,Text,LongWritable> {
    LongWritable one = new LongWritable(1);

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        // Split the record delimited by Space, this will return array of words
        String words[] = value.toString().split("\\t");

        // Iterate the words in the array and sent to the reducer by writing on context object
        for(String word:words){
            context.write(new Text(word),one);
        }
    }
}
