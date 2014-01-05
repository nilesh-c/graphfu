package com.nileshc.graphfu.pagerank;

import cern.colt.function.tdouble.IntIntDoubleFunction;
import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix1D;
import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix2D;
import com.nileshc.graphfu.matrix.MatrixBlockerTest;
import com.nileshc.graphfu.matrix.types.BlockMatrixElement;
import com.nileshc.graphfu.matrix.types.BlockMatrixVectorElementPair;
import com.nileshc.graphfu.matrix.types.BlockVectorElement;
import com.nileshc.graphfu.matrix.types.LongPair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author nilesh
 */
@Ignore
public class PageRankPrepTest extends AbstractMapReduceTest<LongWritable, BlockMatrixElement, LongWritable, BlockMatrixVectorElementPair, LongWritable, BlockMatrixElement> {

    @Override
    @Before
    public void init() throws IOException {
        mapper = new PageRankPrep.PageRankPrepMapper();
        reducer = new PageRankPrep.PageRankPrepReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.getConfiguration().setLong("blockSize", 5);
        mapDriver.getConfiguration().setLong("numNodes", 10);
        mapDriver.getConfiguration().set("danglingNodeVectorOutput", "danglingNodeVectorOutput");
        mapDriver.getConfiguration().set("initialRankVectorOutput", "initialRankVectorOutput");

        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        reduceDriver.getConfiguration().setLong("blockSize", 5);
        reduceDriver.getConfiguration().setLong("numNodes", 10);
        reduceDriver.getConfiguration().set("danglingNodeVectorOutput", "danglingNodeVectorOutput");
        reduceDriver.getConfiguration().set("initialRankVectorOutput", "initialRankVectorOutput");

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.getConfiguration().setLong("blockSize", 5);
        mapReduceDriver.getConfiguration().setLong("numNodes", 10);
        mapReduceDriver.getConfiguration().set("danglingNodeVectorOutput", "danglingNodeVectorOutput");
        mapReduceDriver.getConfiguration().set("initialRankVectorOutput", "initialRankVectorOutput");

        MatrixBlockerTest matrixBlockerTest = new MatrixBlockerTest();
        matrixBlockerTest.init();
        mapperInput = matrixBlockerTest.getSampleOutput().get(0);

        final SparseDoubleMatrix1D vector = new SparseDoubleMatrix1D(5);
        mapperInput.getSecond().getBlockElement().forEachNonZero(new IntIntDoubleFunction() {

            public double apply(int i, int i1, double d) {
                vector.set(i, vector.get(i) + d);
                return d;
            }
        });
        BlockMatrixVectorElementPair blockMatrixVectorElementPair =
                new BlockMatrixVectorElementPair(
                mapperInput.getSecond(),
                new BlockVectorElement(vector));
        mapperOutput = new Pair<LongWritable, BlockMatrixVectorElementPair>(new LongWritable(0), blockMatrixVectorElementPair);

        List<BlockMatrixVectorElementPair> list = new ArrayList<BlockMatrixVectorElementPair>();
        list.add(mapperOutput.getSecond());
        reducerInput = new Pair<LongWritable, List<BlockMatrixVectorElementPair>>(mapperInput.getFirst(), list);

        SparseDoubleMatrix2D matrix = new SparseDoubleMatrix2D(5, 5);
        matrix.setQuick(1, 1, 0.4177215189873418);
        matrix.setQuick(1, 3, 0.12658227848101267);
        matrix.setQuick(1, 4, 0.45569620253164556);
        matrix.setQuick(3, 3, 1.0);
        reducerOutput = new Pair<LongWritable, BlockMatrixElement>(
                new LongWritable(0), new BlockMatrixElement(matrix, new LongPair(0, 0)));
    }

    @Override
    public List<Pair<LongWritable, BlockMatrixElement>> getSampleOutput() throws IOException {
        MatrixBlockerTest matrixBlockerTest = new MatrixBlockerTest();
        matrixBlockerTest.init();
        List<Pair<LongWritable, BlockMatrixElement>> sampleOutput = matrixBlockerTest.getSampleOutput();
        List<Pair<LongWritable, BlockMatrixElement>> results = mapReduceDriver.withAll(sampleOutput).run();
        return results;
    }
}
