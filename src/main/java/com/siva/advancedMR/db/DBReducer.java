package com.siva.advancedMR.db;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Sivakumar on 26/4/15.
 */
public class DBReducer extends Reducer<Text, IntWritable, DBOutputWritable, NullWritable>
{
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
    {
        int sum = 0;

        for(IntWritable value : values)
        {
            sum += value.get();
        }

        try
        {
            context.write(new DBOutputWritable(key.toString(), sum), NullWritable.get());
        } catch(IOException e)
        {
            e.printStackTrace();
        } catch(InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
