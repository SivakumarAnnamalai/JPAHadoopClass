package com.siva.advancedMR.joins;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by Sivakumar on 19/4/15.
 */
public class EmpWithReduceSideJoin {
        public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
            Job job = Job.getInstance();

            // Mapper,Reducer and Invoker classes
            job.setJarByClass(EmpWithReduceSideJoin.class);

            // set input and output path details, Output path should not exist.
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, EmpMapperUsingReduceSideJoin.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, EmpSalaryMapperUsingReduceSideJoin.class);
            job.setReducerClass(EmpReducerUsingReduceSideJoin.class);

            FileOutputFormat.setOutputPath(job, new Path(args[2]));


            // set output key and value types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // submit the job and wait for the completion
            job.waitForCompletion(true);
        }
}























