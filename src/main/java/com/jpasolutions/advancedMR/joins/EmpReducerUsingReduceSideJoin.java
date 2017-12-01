package com.jpasolutions.advancedMR.joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Sivakumar on 24/4/15.
 */
public class EmpReducerUsingReduceSideJoin extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
        // initialize the variable sum\
        String finalValue="";
        String salary="";
        for(Text text:values){
            if(text.toString().startsWith("EA")){
                finalValue = text.toString();
            }else{
                salary = text.toString();
            }
        }
            finalValue = finalValue + salary;

        // In the end you got the word(as key) and corresponding count(as sum). Write to context object.
        context.write(key,new Text(finalValue));
    }
}
