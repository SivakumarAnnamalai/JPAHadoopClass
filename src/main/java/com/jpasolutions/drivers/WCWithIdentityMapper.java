package com.jpasolutions.drivers;

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
public class WCWithIdentityMapper {

        public static void main(String args[])
                throws IOException, ClassNotFoundException,
                InterruptedException {
            Job job = new Job();

            // Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // We dint set any mapper class so it will take the default one.
            // Default one will look like the below one
            // job.setMapperClass(Mapper.class);

            //Reducer and Invoker classes
            job.setReducerClass(WCReducer.class);
            job.setJarByClass(WCWithIdentityMapper.class);

            // set input and output path details, Output path should not exist.
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // set output key and value types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            //job.getConfiguration().set("today","VERY_HIGH");
            // submit the job and wait for the completion
            job.waitForCompletion(true);

        }
}
