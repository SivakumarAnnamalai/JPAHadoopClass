package com.siva.drivers;

import com.siva.mappers.WCMapper;
import com.siva.reducers.WCReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Sivakumar on 19/4/15.
 */

// we have to extends the configured class and implement Tool interface
public class WCWithGenericOptionsParser extends Configured implements Tool {

        public static void main(String args[])
                throws Exception {
            ToolRunner.run(new WCWithGenericOptionsParser(),args);

        }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf);

        // Input and Output formats
        job.setInputFormatClass(NLineInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        // Mapper,Reducer and Invoker classes
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setJarByClass(WCWithGenericOptionsParser.class);


        // set input and output path details, Output path should not exist.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // submit the job and wait for the completion
        job.waitForCompletion(true);

        return 0;
    }
}
