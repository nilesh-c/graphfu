package com.nileshc.graphfu.matrix.types;

import com.intel.hadoop.graphbuilder.graph.Edge;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author nilesh
 */
public class LongDoubleEdgeWritable extends Edge<LongWritable, DoubleWritable> implements Writable {

    public LongDoubleEdgeWritable() {
        super(new LongWritable(), new LongWritable(), new DoubleWritable());
    }

    public LongDoubleEdgeWritable(Edge<LongWritable, DoubleWritable> edge) {
        super(edge.source(), edge.target(), edge.EdgeData());
    }

    public void set(Edge<LongWritable, DoubleWritable> edge) {
        super.set(edge.source(), edge.target(), edge.EdgeData());
    }

    public LongDoubleEdgeWritable get() {
        return this;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        source().write(output);
        target().write(output);
        EdgeData().write(output);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        source().readFields(input);
        target().readFields(input);
        EdgeData().readFields(input);
    }
}
