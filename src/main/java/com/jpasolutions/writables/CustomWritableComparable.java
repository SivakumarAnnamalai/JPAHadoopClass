package com.jpasolutions.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by nrelate on 24/4/15.
 */
public class CustomWritableComparable implements WritableComparable<CustomWritableComparable> {
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

    @Override
    public int compareTo(CustomWritableComparable tp) {
        int cmp = studentName.compareTo(tp.studentName);
        if (cmp != 0) {
            return cmp;
        }
        return mark.compareTo(tp.mark);
    }

    @Override
    public int hashCode() {
        return studentName.hashCode() * 163 + mark.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CustomWritableComparable) {
            CustomWritableComparable tp = (CustomWritableComparable) o;
            return studentName.equals(tp.studentName) && mark.equals(tp.mark);
        }
        return false;
    }

    @Override
    public String toString() {
        return studentName + "\t" + mark;
    }

    /*
    public CustomWritableComparable() {
        set(new Text(), new Text());
    }

    public CustomWritableComparable(String studentName, String mark) {
        set(new Text(studentName), new Text(mark));
    }

    public CustomWritableComparable(Text studentName, Text mark) {
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
    }
    */

}
