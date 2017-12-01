package com.jpasolutions.studentexample;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Sivakumar on 28/6/15.
 */
public class StudentMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String studentData[] = value.toString().split(" ");
        Text studentName = new Text(studentData[0]);
        LongWritable studentMark = new LongWritable(Long.parseLong(studentData[1]));
        context.write(studentName,studentMark);
    }
}
