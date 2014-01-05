package com.nileshc.graphfu.matrix.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author nilesh
 */
public class LongPair implements WritableComparable<LongPair> {

    private LongWritable first;
    private LongWritable second;

    public LongPair() {
        set(new LongWritable(), new LongWritable());
    }

    public LongPair(long first, long second) {
        set(new LongWritable(first), new LongWritable(second));
    }

    public LongPair(LongWritable first, LongWritable second) {
        set(first, second);
    }

    public void set(LongWritable first, LongWritable second) {
        this.first = first;
        this.second = second;
    }

    public LongWritable getFirst() {
        return first;
    }

    public LongWritable getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        first.write(output);
        second.write(output);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        first.readFields(input);
        second.readFields(input);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LongPair) {
            LongPair tp = (LongPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return "LongPair{" + "first=" + first + ", second=" + second + '}';
    }

    @Override
    public int compareTo(LongPair tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(LongPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            long i1 = readLong(b1, s1);
            long i2 = readLong(b2, s2);

            int comp = (i1 < i2) ? -1 : (i1 == i2) ? 0 : 1;
            if (0 != comp) {
                return comp;
            }

            long j1 = readLong(b1, s1 + 8);
            long j2 = readLong(b2, s2 + 8);
            comp = (j1 < j2) ? -1 : (j1 == j2) ? 0 : 1;

            return comp;
        }
    }
}