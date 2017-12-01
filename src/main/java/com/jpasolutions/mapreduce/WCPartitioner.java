package com.jpasolutions.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Sivakumar on 24/4/15.
 */
public class WCPartitioner extends Partitioner<Text,LongWritable> {

    @Override
    public int getPartition(Text key, LongWritable value, int numPartitions) {
        return key.toString().length() % numPartitions ;
    }
}
