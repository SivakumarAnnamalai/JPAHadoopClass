package com.jpasolutions.advancedMR.joins;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Sivakumar on 24/4/15.
 */
public class EmpSalaryMapperUsingReduceSideJoin extends
        Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key,Text value,Context context)
            throws IOException, InterruptedException {
        // Split the record delimited by Space,
        String emp[] = value.toString().split(" ");


        context.write(new Text(emp[0]), new Text("ES "+emp[1]));
    }
}
