package com.siva.reducers;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Sivakumar on 24/4/15.
 */
public class WCReducerUsingDistributedCache extends Reducer<Text,LongWritable,Text,LongWritable> {

    Properties properties ;
    String cacheFileName = "/cache/test.tsv";
    String cacheFileLocation;

    public void setup(Context context) throws IOException {

        Path paths[] = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        for(Path path:paths){
            context.getCounter("Cache Files",path.toString()).increment(1);
            if(path.toString().contains(cacheFileName)){
                cacheFileLocation = path.toString();
            }
        }
        properties = new Properties();
        properties.load(new FileInputStream(cacheFileLocation));

        context.getCounter("Distributed Cache FileLocation",cacheFileLocation).increment(1);
        context.getCounter("Distributed Cache Value of Hadoop",properties.getProperty("hadoop")).increment(1);
    }
    public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
        // initialize the variable sum
        long sum=0;

        // Iterate the list of values for the key and sum it
        for(LongWritable value:values){
            sum = sum + value.get();
        }

        // In the end you got the word(as key) and corresponding count(as sum). Write to context object.
        context.write(key,new LongWritable(sum));
    }
}
