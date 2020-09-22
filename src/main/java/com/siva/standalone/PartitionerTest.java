package com.siva.standalone;

import org.apache.hadoop.io.Text;

/**
 * Created by sivakumaran on 12/1/2017.
 */
public class PartitionerTest {
    public static void main(String args[]){
        String data[] = "abc,xyz,mnp,pqr".split(",");
        int numReducers = 3;
        for(String word:data){
            System.out.println(word+": "+getPartition(word,numReducers));
        }
    }

    public static int getPartition(String word,int numReducers){
        return (new Text(word).hashCode() & Integer.MAX_VALUE ) % numReducers;
    }
}
