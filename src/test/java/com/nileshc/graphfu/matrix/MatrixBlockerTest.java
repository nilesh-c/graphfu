package com.nileshc.graphfu.matrix;

import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix2D;
import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.parser.BasicGraphParser;
import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.nileshc.graphfu.matrix.parsers.CDREdgeDataParser;
import com.nileshc.graphfu.matrix.parsers.CDRVertexIdParser;
import com.nileshc.graphfu.matrix.types.BlockMatrixElement;
import com.nileshc.graphfu.matrix.types.LongDoubleEdgeWritable;
import com.nileshc.graphfu.matrix.types.LongPair;
import com.nileshc.graphfu.pagerank.AbstractMapReduceTest;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import static junit.framework.Assert.*;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author nilesh
 */
public class MatrixBlockerTest extends AbstractMapReduceTest<LongWritable, Text, LongPair, LongDoubleEdgeWritable, LongWritable, BlockMatrixElement> {

    @Override
    @Before
    public void init() throws IOException {
        mapper = new MatrixBlocker.MatrixBlockerMapper();
        reducer = new MatrixBlocker.MatrixBlockerReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.getConfiguration().setLong("blockSize", 5);
        mapDriver.getConfiguration().setClass("GraphParser", BasicGraphParser.class, GraphParser.class);
        mapDriver.getConfiguration().setClass("VertexIdParser", CDRVertexIdParser.class, FieldParser.class);
        mapDriver.getConfiguration().setClass("EdgeDataParser", CDREdgeDataParser.class, FieldParser.class);
//        mapDriver.getConfiguration().set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
//                + "org.apache.hadoop.io.serializer.WritableSerialization");
//        mapDriver.withOutputFormat(SequenceFileOutputFormat.class, TextInputFormat.class);

        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        reduceDriver.getConfiguration().setLong("blockSize", 5);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.getConfiguration().setLong("blockSize", 5);
        mapReduceDriver.getConfiguration().setClass("GraphParser", BasicGraphParser.class, GraphParser.class);
        mapReduceDriver.getConfiguration().setClass("VertexIdParser", CDRVertexIdParser.class, FieldParser.class);
        mapReduceDriver.getConfiguration().setClass("EdgeDataParser", CDREdgeDataParser.class, FieldParser.class);
        //mapReduceDriver.withKeyGroupingComparator(new LongPair.Comparator());
        mapReduceDriver.withKeyOrderComparator(new LongPair.Comparator());

        mapperInput = new Pair<LongWritable, Text>(new LongWritable(0), new Text("5\t0\t2,323,280,43,01-08-2008"));

        mapperOutput = new Pair<LongPair, LongDoubleEdgeWritable>(
                new LongPair(1, 0),
                new LongDoubleEdgeWritable(
                new Edge<LongWritable, DoubleWritable>(
                new LongWritable(0), new LongWritable(0), new DoubleWritable(969.0))));

        List<LongDoubleEdgeWritable> list = new ArrayList<LongDoubleEdgeWritable>();
        list.add(mapperOutput.getSecond());
        reducerInput = new Pair<LongPair, List<LongDoubleEdgeWritable>>(mapperOutput.getFirst(), list);

        BlockMatrixElement blockMatrixElement = new BlockMatrixElement();
        blockMatrixElement.setBlockRowColumn(new LongPair(1, 0));
        SparseDoubleMatrix2D m = new SparseDoubleMatrix2D(5, 5);
        m.setQuick(0, 0, 969.0);
        blockMatrixElement.setBlockElement(m);
        reducerOutput = new Pair<LongWritable, BlockMatrixElement>(new LongWritable(1), blockMatrixElement);
    }

    @Override
    public List<Pair<LongWritable, BlockMatrixElement>> getSampleOutput() throws IOException {
        String[] testInput = {
            "5\t0\t2,323,280,43,01-08-2008",
            "1\t1\t1,33,33,33,01-08-2008",
            "1\t3\t1,10,10,10,01-08-2008",
            "3\t3\t1,25,25,25,01-08-2008",
            "1\t4\t1,36,36,36,01-08-2008",
            "0\t5\t1,66,66,66,01-08-2008",
            "1\t5\t20,220,12,9,01-08-2008",
            "3\t5\t1,72,72,72,01-08-2008",
            "4\t6\t2,14,7,7,01-08-2008",
            "8\t7\t2,42,34,8,01-08-2008"
        };
        for (String s : testInput) {
            mapReduceDriver.addInput(new Pair<LongWritable, Text>(new LongWritable(), new Text(s)));
        }
        List<Pair<LongWritable, BlockMatrixElement>> results = mapReduceDriver.run(true);
        return results;
    }
}
