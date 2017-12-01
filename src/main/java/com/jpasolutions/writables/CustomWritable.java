package com.jpasolutions.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Sivakumar on 24/4/15.
 */
public class CustomWritable implements Writable {
    private Text studentName;
    private Text mark;


    @Override
    public void write(DataOutput out) throws IOException {
        studentName.write(out);
        mark.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        studentName.readFields(in);
        mark.readFields(in);
    }


    /*public CustomWritable() {
        set(new Text(), new Text());
    }

    public CustomWritable(String studentName, String mark) {
        set(new Text(studentName), new Text(mark));
    }

    public CustomWritable(Text studentName, Text mark) {
        set(studentName, mark);
    }

    public void set(Text first, Text second) {
        this.studentName = first;
        this.mark = second;
    }

    public Text getStudentName() {
        return studentName;
    }

    public Text getMark() {
        return mark;
    }*/
}
