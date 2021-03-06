package com.siva.advancedMR.db;

/**
 * Created by Sivakumar on 26/4/15.
 */
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class DBMapper extends Mapper<LongWritable, DBInputWritable, Text, IntWritable>
{
    private IntWritable one = new IntWritable(1);

    protected void map(LongWritable id, DBInputWritable value, Context context)
    {
        try
        {
            String[] keys = value.getName().split(" ");

            for(String key : keys)
            {
                context.write(new Text(key),one);
            }
        } catch(IOException e)
        {
            e.printStackTrace();
        } catch(InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}


