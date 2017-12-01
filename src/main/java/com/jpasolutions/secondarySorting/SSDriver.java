package com.jpasolutions.secondarySorting;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.security.auth.login.Configuration;

/**
 * Created by Sivakumar on 2/6/15.
 */
public class SSDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(),"Secondary Sorting Example");

        job.setJarByClass(SSDriver.class);
        job.setMapperClass(SSMapper.class);
        job.setReducerClass(SSReducer.class);
        job.setSortComparatorClass(StudentKeyComparator.class);
        job.setGroupingComparatorClass(SSGroupComparator.class);
        job.setPartitionerClass(SSPartitioner.class);

        job.setMapOutputKeyClass(Student.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String args[]) throws Exception {
        ToolRunner.run(new SSDriver(),args);
    }
}
