package com.jpasolutions.joins;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Sivakumar on 19/4/15.
 */
public class EmpWithMapsideJoin {
        public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
            Job job = new Job();

            // Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // Mapper,Reducer and Invoker classes
            job.setMapperClass(EmpMapperUsingMapsideJoin.class);
            job.setNumReduceTasks(0);
            job.setJarByClass(EmpWithMapsideJoin.class);

            // set input and output path details, Output path should not exist.
            FileInputFormat.addInputPath(job, new Path(args[0]));
            //FileInputFormat.addInputPath(job,new Path(args[2]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));


            // set output key and value types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            // add a file to distributed cache
            DistributedCache.addCacheFile(new URI("/cache/emp_salary.txt"),job.getConfiguration());

            // submit the job and wait for the completion
            job.waitForCompletion(true);
        }
}























