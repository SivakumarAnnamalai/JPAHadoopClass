package com.jpasolutions.drivers;

import com.jpasolutions.mapreduce.WCMapperUsingCounters;
import com.jpasolutions.wordcount.WCReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Sivakumar on 19/4/15.
 */
public class WCWithCounters {
        public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
            Job job = new Job();

            // Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // Mapper,Reducer and Invoker classes
            job.setMapperClass(WCMapperUsingCounters.class);
            job.setReducerClass(WCReducer.class);
            job.setJarByClass(WCWithCounters.class);

            // set input and output path details, Output path should not exist.
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // set output key and value types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            // set the number of reducers to 3
            job.setNumReduceTasks(3);

            // submit the job and wait for the completion
            job.waitForCompletion(true);

            // Retrieve the counter after the job gets over
            System.out.println("Counter Value is "+job.getCounters().findCounter(WCMapperUsingCounters.CUSTOMCOUNTER.TWO_LETTER_WORDS).getValue()
            );
        }
}
