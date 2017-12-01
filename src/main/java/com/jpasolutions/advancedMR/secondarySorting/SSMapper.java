package com.jpasolutions.advancedMR.secondarySorting;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Sivakumar on 29/4/15.
 */
public class SSMapper extends Mapper<LongWritable,Text,Student,NullWritable> {
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        // Split based on tab. Data format given below
        // StudentName  Subject Marks
        String tokens[] = value.toString().split("\\t");

        Student studentObj = new Student(tokens[0],tokens[1]);

        context.write(studentObj,NullWritable.get());
    }
}
