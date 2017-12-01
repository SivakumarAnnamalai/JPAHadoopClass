package com.jpasolutions.advancedMR.secondarySorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Sivakumar on 2/6/15.
 */
public class SSGroupComparator extends WritableComparator {
    protected SSGroupComparator(){
        super(Student.class,true);
    }

    public int compare(WritableComparable w1,WritableComparable w2){
        Student s1 = (Student) w1;
        Student s2 = (Student) w2;
        return s1.getStudentName().compareTo(s2.getStudentName());
    }
}
