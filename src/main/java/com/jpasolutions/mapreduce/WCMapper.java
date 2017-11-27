package com.jpasolutions.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by nrelate on 24/4/15.
 */
public class WCMapper extends
        Mapper<LongWritable,Text,Text,LongWritable> {
    LongWritable one = new LongWritable(1);
    //long longone = 1;

    public void map(LongWritable key,Text value,Context ctx)
            throws IOException, InterruptedException {
        // Split the record delimited by Space,
        // this will return array of words
        String words[] = value.toString().split(" ");

        // Iterate the words in the array
        // and sent to the reducer by writing on ctx object
        /*for(int i=0;i<words.length;i++){
            ctx.write(new Text(words[i]),one);
        }*/

        // Other way to iterate the words
        for(String word:words){
            Text outValue = new Text(word);
            ctx.write(outValue,one);
        }
    }
}
