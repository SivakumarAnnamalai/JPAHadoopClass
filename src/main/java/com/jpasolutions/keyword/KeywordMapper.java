package com.jpasolutions.keyword;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by nrelate on 20/6/15.
 */
public class KeywordMapper extends Mapper<LongWritable,Text,Text,Text> {
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String data[] = value.toString().split("\\t");
        context.write(new Text(data[1]+data[2]+data[3]),new Text(data[3]));
    }
}
