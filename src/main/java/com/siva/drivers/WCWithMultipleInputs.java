package com.siva.drivers;

import com.siva.mappers.WCMapper;
import com.siva.mappers.WCMapperTabBased;
import com.siva.reducers.WCReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Sivakumar on 19/4/15.
 */
public class WCWithMultipleInputs {

        public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
            Job job = Job.getInstance();

            // Input and Output formats
            //job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // Mapper,Reducer and Invoker classes
            //job.setMapperClass(WCMapper.class);
            job.setReducerClass(WCReducer.class);
            job.setJarByClass(WCWithMultipleInputs.class);

            // set Multiple input with different mapper each
            MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, WCMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, WCMapperTabBased.class);


            // set output path
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            // set output key and value types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            // submit the job and wait for the completion
            job.waitForCompletion(true);
        }
}
