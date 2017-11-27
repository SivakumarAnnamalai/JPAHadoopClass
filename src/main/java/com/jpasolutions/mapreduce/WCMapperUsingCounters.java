package com.jpasolutions.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by nrelate on 24/4/15.
 */
public class WCMapperUsingCounters extends Mapper<LongWritable,Text,Text,LongWritable> {
    LongWritable one = new LongWritable(1);

    public static enum CUSTOMCOUNTER {
        TWO_LETTER_WORDS,THREE_LETTER_WORDS
    }


    public void map(LongWritable key,Text value,Context ctx) throws IOException, InterruptedException {
        // Split the record delimited by Space, this will return array of words
        String words[] = value.toString().split("\\s+");

        // Iterate the words in the array and sent to the reducer by writing on ctx object
        for(String word:words){
            if(word.length() == 2){
                ctx.getCounter(CUSTOMCOUNTER.TWO_LETTER_WORDS).increment(1);
                ctx.getCounter("LetterGroup","TwoLetterWords").increment(1);
            }
            ctx.write(new Text(word),one);
        }
    }
}
