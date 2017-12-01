package com.jpasolutions.drivers;

import com.jpasolutions.wordcount.WCMapper;
import com.jpasolutions.mapreduce.WCPartitioner;
import com.jpasolutions.mapreduce.WCReducerUsingMultipleOutputs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Sivakumar on 19/4/15.
 */
public class WCWithMultipleOutputs {

        public static void main(String args[])
                throws IOException, ClassNotFoundException,
                InterruptedException {
            Job job = new Job();

            // Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            //job.setOutputFormatClass(TextOutputFormat.class);
            LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);


            // Mapper,Reducer and Invoker classes
            job.setMapperClass(WCMapper.class);
            job.setReducerClass(WCReducerUsingMultipleOutputs.class);
            job.setJarByClass(WCWithMultipleOutputs.class);
            job.setPartitionerClass(WCPartitioner.class);

            // set input and output path details, Output path should not exist.
            FileInputFormat.addInputPath(job, new Path(args[0]));

            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // set output key and value types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setNumReduceTasks(10);

            //MultipleOutputs.addNamedOutput(job,"WC",TextOutputFormat.class,Text.class,LongWritable.class);

            // submit the job and wait for the completion
            job.waitForCompletion(true);

        }
}
