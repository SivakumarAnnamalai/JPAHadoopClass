package com.jpasolutions.joins;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by nrelate on 24/4/15.
 */
public class EmpMapperUsingReduceSideJoin extends
        Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key,Text value,Context ctx)
            throws IOException, InterruptedException {
        // Split the record delimited by Space,
        String emp[] = value.toString().split(" ");


        ctx.write(new Text(emp[1]),new Text("EA "+emp[0]+" "+emp[2]));
    }
}
