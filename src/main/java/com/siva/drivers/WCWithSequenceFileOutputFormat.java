package com.siva.drivers;

import com.siva.mappers.WCMapper;
import com.siva.reducers.WCReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by Sivakumar on 19/4/15.
 */
public class WCWithSequenceFileOutputFormat {

        public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
            Job job = Job.getInstance();

            // Input format
            job.setInputFormatClass(TextInputFormat.class);

            // set outputformat as sequencefile
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Mapper,Reducer and Invoker classes
            job.setMapperClass(WCMapper.class);
            job.setReducerClass(WCReducer.class);
            job.setJarByClass(WCWithSequenceFileOutputFormat.class);

            // set input and output path details, Output path should not exist.
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // set output key and value types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            // submit the job and wait for the completion
            job.waitForCompletion(true);
        }
}
