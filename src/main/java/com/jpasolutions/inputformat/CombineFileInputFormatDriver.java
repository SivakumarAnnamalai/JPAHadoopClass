package com.jpasolutions.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by nrelate on 19/4/15.
 */
public class CombineFileInputFormatDriver {

        public static void main(String args[])
                throws IOException, ClassNotFoundException,
                InterruptedException {
            Job job = new Job();

            // Input and Output formats
            job.setInputFormatClass(CombineFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // Mapper,Reducer and Invoker classes
            job.setJarByClass(CombineFileInputFormatDriver.class);

            // set input and output path details, Output path should not exist.
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // set output key and value types
            //job.setOutputKeyClass(NullWritable.class);
            //job.setOutputValueClass(BytesWritable.class);

            job.getConfiguration().set("mapred.child.java.opts","-Xmx1G");

            //job.getConfiguration().set("today","VERY_HIGH");
            // submit the job and wait for the completion
            job.waitForCompletion(true);

        }
}
