package com.jpasolutions.drivers;

import com.jpasolutions.wordcount.WCMapper;
import com.jpasolutions.mapreduce.WCReducerUsingSideDataDistributionInConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by Sivakumar on 19/4/15.
 */
public class WCWithSideDataDistributionInConfiguration {
        public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
            Configuration conf = new Configuration();
            conf.set("personName","kumar");

            Job job = new Job(conf);

            // Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // Mapper,Reducer and Invoker classes
            job.setMapperClass(WCMapper.class);
            job.setReducerClass(WCReducerUsingSideDataDistributionInConfiguration.class);
            job.setJarByClass(WCWithSideDataDistributionInConfiguration.class);

            // set input and output path details, Output path should not exist.
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // set output key and value types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            // Side data distribution through configuration object
            job.getConfiguration().set("today","25/April/2015");

            // submit the job and wait for the completion
            job.waitForCompletion(true);
        }
}
