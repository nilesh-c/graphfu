package com.nileshc.graphfu.matrix.parsers;

import com.intel.hadoop.graphbuilder.parser.FieldParser;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;

/**
 *
 * @author nilesh
 */
public class EdgeDataParser implements FieldParser<DoubleWritable> {

    public DoubleWritable getValue(String text) {
        StringTokenizer tokenzier = new StringTokenizer(text, ",");
        int count = Integer.parseInt(tokenzier.nextToken());
        int totalVolume = Integer.parseInt(tokenzier.nextToken());
        int maxVolume = Integer.parseInt(tokenzier.nextToken());
        int minVolume = Integer.parseInt(tokenzier.nextToken());

        return new DoubleWritable(totalVolume + count * (maxVolume + minVolume));
    }

    public Class getType() {
        return DoubleWritable.class;
    }
}
