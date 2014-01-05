package com.nileshc.graphfu.matrix.parsers;

import com.intel.hadoop.graphbuilder.parser.FieldParser;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author nilesh
 */
public class CDRVertexIdParser implements FieldParser<LongWritable> {

    public LongWritable getValue(String text) {
        return new LongWritable(Long.parseLong(text));
    }

    public Class getType() {
        return LongWritable.class;
    }
}
