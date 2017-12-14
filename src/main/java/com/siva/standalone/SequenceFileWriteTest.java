package com.siva.standalone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Sivakumar on 26/4/15.
 */
public class SequenceFileWriteTest {

private static final String[] DATA = {
        "One, two, buckle my shoe",
        "Three, four, shut the door",
        "Five, six, pick up sticks",
        "Seven, eight, lay them straight",
        "Nine, ten, a big fat hen"
};
    public static void main(String args[]) throws IOException {
        String uri = "sample_sequencefile.seq"  ;
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri),configuration);
        Path path = new Path(uri);

        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        writer = SequenceFile.createWriter(fileSystem,configuration,path,key.getClass(),value.getClass());
        for(int i=0;i<100;i++){
            key.set(i);
            value.set(DATA[i % DATA.length]);
            System.out.println("Key: "+i+" Value: "+DATA[i % DATA.length]);
            writer.append(key,value);
        }
        IOUtils.closeStream(writer);
    }
}