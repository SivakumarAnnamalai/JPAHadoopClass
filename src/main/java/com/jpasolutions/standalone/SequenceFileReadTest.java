package com.jpasolutions.standalone;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;

/**
 * Created by nrelate on 26/4/15.
 */
public class SequenceFileReadTest {
    public static void main(String args[]) throws IOException {
        String uri = "sample_sequencefile.seq";
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri),configuration);
        Path path = new Path(uri);

        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem,path,configuration);
        Writable key  = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),configuration);
        Writable value  = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),configuration);
        long position = reader.getPosition();
        while (reader.next(key,value)){
            System.out.println("Key: "+key+" Value: "+value);
        }
        IOUtils.closeStream(reader);
    }
}
