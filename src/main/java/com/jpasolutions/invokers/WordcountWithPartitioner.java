package com.jpasolutions.invokers;

import com.jpasolutions.mapreduce.WCMapper;
import com.jpasolutions.mapreduce.WCPartitioner;
import com.jpasolutions.mapreduce.WCReducer;
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
 * Created by nrelate on 19/4/15.
 */
public class WordcountWithPartitioner {
        public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
            Job job = new Job();

            // Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // Mapper,Reducer and Invoker classes
            job.setMapperClass(WCMapper.class);
            job.setReducerClass(WCReducer.class);
            job.setJarByClass(WordcountWithPartitioner.class);

            // set input and output path details, Output path should not exist.
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // set output key and value types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            // set the number of reducers to 3
            job.setNumReduceTasks(3);

            // set the partitioner class
            job.setPartitionerClass(WCPartitioner.class);

            // submit the job and wait for the completion
            job.waitForCompletion(true);
        }
}
