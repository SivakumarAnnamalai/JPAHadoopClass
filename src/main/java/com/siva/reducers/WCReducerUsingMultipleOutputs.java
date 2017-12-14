package com.siva.reducers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by Sivakumar on 24/4/15.
 */
public class WCReducerUsingMultipleOutputs extends Reducer<Text,LongWritable,Text,LongWritable> {
    MultipleOutputs<Text,LongWritable> multipleOutputs;
    public static String WORDS[] = {"ZERO","ONE","TWO","THREE","FOUR","FIVE"};

    public void setup(Context context){
        multipleOutputs =  new MultipleOutputs<Text,LongWritable>(context);
    }

    public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
        // initialize the variable sum
        long sum=0;

        // Iterate the list of values for the key and sum it
        for(LongWritable value:values){
            sum = sum + value.get();
        }

        // In the end you got the word(as key) and corresponding count(as sum). Write to context object.
        //context.write(key,new LongWritable(sum));

        // multiple output usage
        //multipleOutputs.write(key,new LongWritable(sum),
          //      WORDS[key.toString().length()]);

        multipleOutputs.write(key,new LongWritable(sum),
                key.toString());
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}


















