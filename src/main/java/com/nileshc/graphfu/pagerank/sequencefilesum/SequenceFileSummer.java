package com.nileshc.graphfu.pagerank.sequencefilesum;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author nilesh
 */
public class SequenceFileSummer {

    private Path input;

    public SequenceFileSummer(String input) {
        Configuration conf = new Configuration();
        this.input = new Path(input);
    }

    public double getSum() throws IOException, InstantiationException, IllegalAccessException {
        double sum = 0;
        SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(input));
        WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
        DoubleWritable value = new DoubleWritable();
        while (reader.next(key, value)) {
            sum += value.get();
        }
        return sum;
    }
}
