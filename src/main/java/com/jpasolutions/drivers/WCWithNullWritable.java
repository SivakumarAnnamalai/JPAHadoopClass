package com.jpasolutions.drivers;

import com.jpasolutions.mappers.WCMapperUsingNullWritable;
import com.jpasolutions.reducers.WCReducerUsingNullWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by Sivakumar on 19/4/15.
 */
public class WCWithNullWritable {

        public static void main(String args[])
                throws IOException, ClassNotFoundException,
                InterruptedException {
            Job job = Job.getInstance();

            // Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Mapper,Reducer and Invoker classes
            job.setMapperClass(WCMapperUsingNullWritable.class);
            job.setReducerClass(WCReducerUsingNullWritable.class);
            job.setJarByClass(WCWithNullWritable.class);

            // set input and output path details, Output path should not exist.
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // set output key and value types
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(LongWritable.class);

            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(LongWritable.class);

            //job.getConfiguration().set("today","VERY_HIGH");
            // submit the job and wait for the completion
            job.waitForCompletion(true);

        }
}
