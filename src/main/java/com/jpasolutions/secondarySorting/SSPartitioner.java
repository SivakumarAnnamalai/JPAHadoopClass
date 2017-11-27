package com.jpasolutions.secondarySorting;

import com.jpasolutions.writables.CustomWritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by nrelate on 29/4/15.
 */
public class SSPartitioner extends Partitioner<Student,NullWritable> {

    @Override
    public int getPartition(Student studentObj, NullWritable nullWritable, int numPartitions) {
        return Math.abs(studentObj.getStudentName().toString().hashCode()) % numPartitions ;
    }
}
