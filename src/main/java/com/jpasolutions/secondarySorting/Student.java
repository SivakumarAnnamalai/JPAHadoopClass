package com.jpasolutions.secondarySorting;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Sivakumar on 24/4/15.
 */
public class Student implements WritableComparable<Student> {
    private Text studentName;
    private Text mark;

    public Student() {
        set(new Text(), new Text());
    }

    public Student(String studentName, String mark) {
        set(new Text(studentName), new Text(mark));
    }

    public Student(Text studentName, Text mark) {
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
    public int hashCode() {
        return studentName.hashCode() * 163 + mark.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Student) {
            Student tp = (Student) o;
            return studentName.equals(tp.studentName) && mark.equals(tp.mark);
        }
        return false;
    }

    @Override
    public String toString() {
        return studentName + "\t" + mark;
    }

    @Override
    public int compareTo(Student tp) {
        int cmp = studentName.compareTo(tp.studentName);
        if (cmp != 0) {
            return cmp;
        }
        int m1 = Integer.parseInt(mark.toString());
        int m2 = Integer.parseInt(tp.mark.toString());

        return m1==m2?0:(m1>m2?-1:1);
    }


























    public static class Comparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator() {
            super(Student.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    return cmp;
                }
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                        b2, s2 + firstL2, l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static {
        WritableComparator.define(Student.class, new Comparator());
    }
    // ^^ TextPairComparator

    // vv TextPairFirstComparator
    public static class FirstComparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FirstComparator() {
            super(Student.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof Student && b instanceof Student) {
                return ((Student) a).studentName.compareTo(((Student) b).studentName);
            }
            return super.compare(a, b);
        }
    }
}
