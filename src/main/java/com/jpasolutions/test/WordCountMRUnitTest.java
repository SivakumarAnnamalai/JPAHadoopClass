package com.jpasolutions.test;

import com.jpasolutions.mapreduce.WCMapper;
import com.jpasolutions.mapreduce.WCReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by nrelate on 24/4/15.
 */
public class WordCountMRUnitTest {
    MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
    LongWritable one ;

    @Before
    public void setUp() {
        one = new LongWritable(1);
        WCMapper mapper = new WCMapper();
        WCReducer reducer = new WCReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
    @Test
    public void testMapper() {
        mapDriver.withInput(one,new Text("hi abc hello hi"));
        mapDriver.withOutput(new Text("hi"), one);
        mapDriver.withOutput(new Text("abc"), one);
        mapDriver.withOutput(new Text("hello"), one);
        mapDriver.withOutput(new Text("hi"),one);
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(one);
        values.add(one);
        reduceDriver.withInput(new Text("hi"), values);
        reduceDriver.withOutput(new Text("hi"), new LongWritable(2));
        reduceDriver.runTest();
    }
    @Test  public void testMapReduce() {
        mapReduceDriver.withInput(one, new Text("hi abc hello hi"));

        mapReduceDriver.withOutput(new Text("abc"), new LongWritable(1));
        mapReduceDriver.withOutput(new Text("hello"), new LongWritable(1));
        mapReduceDriver.withOutput(new Text("hi"), new LongWritable(2));
        mapReduceDriver.runTest();
    }
    
}
