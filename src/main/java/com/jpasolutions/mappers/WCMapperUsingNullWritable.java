package com.jpasolutions.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Sivakumar on 24/4/15.
 */
public class WCMapperUsingNullWritable extends
        Mapper<LongWritable,Text,NullWritable,LongWritable> {
    LongWritable one = new LongWritable(1);
    //long longone = 1;

    public void map(LongWritable key,Text value,Context context)
            throws IOException, InterruptedException {
        // Split the record delimited by Space,
        // this will return array of words
        String words[] = value.toString().split(" ");

        // Iterate the words in the array
        // and sent to the reducer by writing on context object
        /*for(int i=0;i<words.length;i++){
            context.write(new Text(words[i]),one);
        }*/

        // Other way to iterate the words
        for(String word:words){
            Text outValue = new Text(word);
            context.write(NullWritable.get(),one);
        }
    }
}
