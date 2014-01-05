package com.nileshc.graphfu.pagerank;

import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix1D;
import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix2D;
import com.intel.hadoop.graphbuilder.parser.BasicGraphParser;
import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.nileshc.graphfu.matrix.MatrixBlocker;
import com.nileshc.graphfu.matrix.parsers.CDREdgeDataParser;
import com.nileshc.graphfu.matrix.parsers.CDRVertexIdParser;
import com.nileshc.graphfu.matrix.types.BlockMatrixElement;
import com.nileshc.graphfu.matrix.types.BlockVectorElement;
import com.nileshc.graphfu.matrix.types.LongPair;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author nilesh
 */
public class BlockMatVec {

    private float alpha;
    private long blockSize;

    public BlockMatVec(float alpha, long blockSize) {
        this.alpha = alpha;
        this.blockSize = blockSize;
    }

    public int run(String blockMatrixInput, String blockVectorInput, String newBlockVectorOutput, String danglingNodeVectorDCInput, String partialRankVectorSumOutput, String danglingRankVectorDotProductOutput) throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat("alpha", alpha);
        conf.setLong("blockSize", blockSize);
        conf.set("partialRankVectorSumOutput", partialRankVectorSumOutput);
        conf.set("danglingRankVectorDotProductOutput", danglingRankVectorDotProductOutput);

        Job job = new Job(conf, "Block MatVec");
        job.setJarByClass(BlockMatVec.class);

        job.setMapperClass(BlockMatVec.BlockMatVecMapper.class);
        job.setReducerClass(BlockMatVec.BlockMatVecReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(BlockVectorElement.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(BlockVectorElement.class);

        job.setInputFormatClass(CompositeInputFormat.class);
        String joinStatement = CompositeInputFormat.compose("inner", SequenceFileInputFormat.class, blockMatrixInput);
        System.out.println(joinStatement);
        conf.set("mapred.join.expr", joinStatement);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(newBlockVectorOutput));

        MultipleOutputs.addNamedOutput(job, partialRankVectorSumOutput, SequenceFileOutputFormat.class, NullWritable.class, DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, danglingRankVectorDotProductOutput, SequenceFileOutputFormat.class, NullWritable.class, DoubleWritable.class);

        //job.addCacheFile(new URI(danglingNodeVectorDCInput));
        DistributedCache.addCacheFile(new URI(danglingNodeVectorDCInput), conf);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class BlockMatVecMapper extends Mapper<LongWritable, TupleWritable, LongWritable, BlockVectorElement> {

        private BlockVectorElement blockVectorElement;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            blockVectorElement = new BlockVectorElement();
        }

        @Override
        protected void map(LongWritable key, TupleWritable value, Context context) throws IOException, InterruptedException {
            BlockMatrixElement blockMatrixElement = (BlockMatrixElement) value.get(0);
            BlockVectorElement blockVectorElement = (BlockVectorElement) value.get(1);
            blockVectorElement.set((SparseDoubleMatrix1D) blockMatrixElement.getBlockElement().zMult(blockVectorElement.get(), null));
            context.write(blockMatrixElement.getBlockRowColumn().getFirst(), blockVectorElement);
        }
    }

    static class BlockMatVecReducer extends Reducer<LongWritable, BlockVectorElement, LongWritable, BlockVectorElement> {

        private double alpha;
        private long blockSize;
        private DoubleWritable doubleWritable;
        private BlockVectorElement blockVectorElement;
        private String partialRankVectorSumOutput;
        private String danglingRankVectorDotProductOutput;
        private MultipleOutputs multipleOutputs;
        private HashMap<Long, SparseDoubleMatrix1D> danglingNodeMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            alpha = context.getConfiguration().getFloat("alpha", 0.85f);
            blockSize = context.getConfiguration().getLong("blockSize", 1);
            partialRankVectorSumOutput = context.getConfiguration().get("partialRankVectorSumOutput");
            danglingRankVectorDotProductOutput = context.getConfiguration().get("danglingRankVectorDotProductOutput");
            doubleWritable = new DoubleWritable();
            blockVectorElement = new BlockVectorElement();
            multipleOutputs = new MultipleOutputs(context);

            Path[] cachedFiles = context.getLocalCacheFiles();
            SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(cachedFiles[0]));
            danglingNodeMap = new HashMap<Long, SparseDoubleMatrix1D>();
            LongWritable key = new LongWritable();
            BlockVectorElement value = new BlockVectorElement();
            while (reader.next(key, value)) {
                danglingNodeMap.put(key.get(), value.get());
            }
            reader.close();
        }

        @Override
        protected void reduce(LongWritable key, Iterable<BlockVectorElement> values, Context context) throws IOException, InterruptedException {
            SparseDoubleMatrix1D rowSumVector = new SparseDoubleMatrix1D((int) blockSize);
            for (BlockVectorElement partialRowSumVector : values) {
                rowSumVector.assign(partialRowSumVector.get(), cern.jet.math.tdouble.DoubleFunctions.plus);
            }
            rowSumVector.assign(cern.jet.math.tdouble.DoubleFunctions.mult(alpha));

            doubleWritable.set(rowSumVector.zSum());
            multipleOutputs.write(partialRankVectorSumOutput, NullWritable.get(), doubleWritable, partialRankVectorSumOutput + "/part");
            doubleWritable.set(rowSumVector.zDotProduct(danglingNodeMap.get(key.get())));
            multipleOutputs.write(danglingRankVectorDotProductOutput, NullWritable.get(), doubleWritable, danglingRankVectorDotProductOutput + "/part");
            blockVectorElement.set(rowSumVector);
            context.write(key, blockVectorElement);
        }
    }
}
