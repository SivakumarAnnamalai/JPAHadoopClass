package com.jpasolutions.secondarySorting;

import com.jpasolutions.writables.CustomWritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by nrelate on 29/4/15.
 */
public class SSReducer extends Reducer<Student,NullWritable,Text,NullWritable> {
    public void reduce(Student key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
        context.write(new Text(key.getStudentName()+"\t"+key.getMark()),NullWritable.get());
    }
}
