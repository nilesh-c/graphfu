package com.nileshc.graphfu.pagerank;

import cern.colt.function.tdouble.DoubleDoubleFunction;
import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix1D;
import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix2D;
import com.nileshc.graphfu.matrix.types.BlockMatrixElement;
import com.nileshc.graphfu.matrix.types.BlockMatrixVectorElementPair;
import com.nileshc.graphfu.matrix.types.BlockVectorElement;
import com.nileshc.graphfu.matrix.types.LongPair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author nilesh
 */
public class PageRankPrep {

    private long blockSize;
    private long numNodes;

    public PageRankPrep(int blockSize, long numNodes) {
        this.blockSize = blockSize;
        this.numNodes = numNodes;
    }

    public int run(String matrixInput, String normalizedMatrixOutput, String danglingNodeVectorOutput, String initialRankVectorOutput) throws Exception {
        Configuration conf = new Configuration();
        conf.setLong("blockSize", blockSize);
        conf.setLong("numNodes", numNodes);
        conf.set("danglingNodeVectorOutput", danglingNodeVectorOutput);
        conf.set("initialRankVectorOutput", initialRankVectorOutput);

        Job job = new Job(conf, "PageRank Prep");
        job.setJarByClass(PageRankPrep.class);

        job.setMapperClass(PageRankPrep.PageRankPrepMapper.class);
        job.setReducerClass(PageRankPrep.PageRankPrepReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(BlockMatrixVectorElementPair.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(BlockMatrixElement.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(matrixInput));
        FileOutputFormat.setOutputPath(job, new Path(normalizedMatrixOutput));

        MultipleOutputs.addNamedOutput(job, danglingNodeVectorOutput, SequenceFileOutputFormat.class, LongWritable.class, BlockVectorElement.class);
        MultipleOutputs.addNamedOutput(job, initialRankVectorOutput, SequenceFileOutputFormat.class, LongWritable.class, BlockVectorElement.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class PageRankPrepMapper extends Mapper<LongWritable, BlockMatrixElement, LongWritable, BlockMatrixVectorElementPair> {

        private BlockMatrixVectorElementPair blockMatrixVectorPair;
        private BlockVectorElement blockVectorElement;
        private long blockSize;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.blockMatrixVectorPair = new BlockMatrixVectorElementPair();
            this.blockVectorElement = new BlockVectorElement();
            this.blockSize = context.getConfiguration().getLong("blockSize", 1);
        }

        @Override
        protected void map(LongWritable key, BlockMatrixElement value, Context context) throws IOException, InterruptedException {
            SparseDoubleMatrix2D matrix = value.getBlockElement();
            SparseDoubleMatrix1D vector = new SparseDoubleMatrix1D((int) blockSize);
            int rows = matrix.rows();
            for (int i = 0; i < rows; i++) {
                vector.setQuick(i, matrix.viewRow(i).zSum());
            }
            blockVectorElement.set(vector);
            blockMatrixVectorPair.setBlockMatrixElement(value);
            blockMatrixVectorPair.setBlockVectorElement(blockVectorElement);
            context.write(key, blockMatrixVectorPair);
        }
    }

    static class PageRankPrepReducer extends Reducer<LongWritable, BlockMatrixVectorElementPair, LongWritable, BlockMatrixElement> {

        private BlockVectorElement blockVectorElement;
        private long blockSize;
        private DoubleDoubleFunction assignOneIfZero;
        private DoubleDoubleFunction zeroSafeDiv;
        private String danglingNodeVectorOutput;
        private String initialRankVectorOutput;
        private BlockVectorElement initialRankVectorElement;
        private MultipleOutputs multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.blockVectorElement = new BlockVectorElement();
            this.initialRankVectorElement = new BlockVectorElement();

            this.blockSize = context.getConfiguration().getLong("blockSize", 1);
            double inverseNumNodes = 1.0 / context.getConfiguration().getLong("numNodes", 1);

            // Compute initial rank vector
            SparseDoubleMatrix1D rankVector = new SparseDoubleMatrix1D((int) blockSize);
            rankVector.assign(inverseNumNodes);
            initialRankVectorElement.set(rankVector);

            this.danglingNodeVectorOutput = context.getConfiguration().get("danglingNodeVectorOutput");
            this.initialRankVectorOutput = context.getConfiguration().get("initialRankVectorOutput");
            this.multipleOutputs = new MultipleOutputs(context);

            this.assignOneIfZero = new DoubleDoubleFunction() {

                @Override
                public double apply(double d, double d1) {
                    return d1 == 0 ? 1 : d;
                }
            };

            this.zeroSafeDiv = new DoubleDoubleFunction() {

                @Override
                public double apply(double d, double d1) {
                    return d1 == 0 ? d : d / d1;
                }
            };
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

//        @Override
//        protected void reduce(LongWritable key, Iterable<BlockMatrixVectorElementPair> values, Context context) throws IOException, InterruptedException {
//            List<BlockMatrixVectorElementPair> backupStore = new ArrayList<BlockMatrixVectorElementPair>();
//            SparseDoubleMatrix1D rowSumVector = new SparseDoubleMatrix1D((int) blockSize);
//            SparseDoubleMatrix1D danglingNodeVector = new SparseDoubleMatrix1D((int) blockSize);
//
//            Iterator<BlockMatrixVectorElementPair> itr = values.iterator();
//
//            // Compute row sums from partial row sum vectors
//            while (itr.hasNext()) {
//                BlockMatrixVectorElementPair item = itr.next();
//                LongWritable a = item.getBlockMatrixElement().getBlockRowColumn().getFirst();
//                LongWritable b = item.getBlockMatrixElement().getBlockRowColumn().getSecond();
//                backupStore.add(new BlockMatrixVectorElementPair(
//                        new BlockMatrixElement(item.getBlockMatrixElement().getBlockElement(),
//                        new LongPair(new LongWritable(a.get()), new LongWritable(b.get()))),
//                        new BlockVectorElement(item.getBlockVectorElement().get())));
//                SparseDoubleMatrix1D partialRowSumVector = item.getBlockVectorElement().get();
//                rowSumVector.assign(partialRowSumVector, cern.jet.math.tdouble.DoubleFunctions.plus);
//            }
//
//            // Compute dangling node vector
//            danglingNodeVector.assign(rowSumVector, this.assignOneIfZero);
//            blockVectorElement.set(danglingNodeVector);
//            multipleOutputs.write(danglingNodeVectorOutput, key, blockVectorElement, danglingNodeVectorOutput + "/part");
//
//            // Divide columns by rowSumVector, ie. normalize rows
//            itr = backupStore.iterator();
//            while (itr.hasNext()) {
//                BlockMatrixElement blockMatrixElement = itr.next().getBlockMatrixElement();
//                LongWritable blockColumn = blockMatrixElement.getBlockRowColumn().getSecond();
//                SparseDoubleMatrix2D block = blockMatrixElement.getBlockElement();
//                int columns = block.columns();
//                for (int i = 0; i < columns; i++) {
//                    block.viewColumn(i).assign(rowSumVector, this.zeroSafeDiv);
//                }
//                context.write(blockColumn, blockMatrixElement);
//            }
//            multipleOutputs.write(initialRankVectorOutput, key, initialRankVectorElement, initialRankVectorOutput + "/part");
//        }
        @Override
        protected void reduce(LongWritable key, Iterable<BlockMatrixVectorElementPair> values, Context context) throws IOException, InterruptedException {
            SparseDoubleMatrix1D rowSumVector = new SparseDoubleMatrix1D((int) blockSize);
            SparseDoubleMatrix1D danglingNodeVector = new SparseDoubleMatrix1D((int) blockSize);
            for (Iterator it = ClassUtils.getAllInterfaces(values.iterator().getClass()).iterator(); it.hasNext();) {
                System.out.println(ClassUtils.getAllInterfaces(it.next().getClass()));
            }
            MarkableIterator<BlockMatrixVectorElementPair> itr = new MarkableIterator<BlockMatrixVectorElementPair>(values.iterator());
            itr.mark();

            // Compute row sums from partial row sum vectors
            while (itr.hasNext()) {
                SparseDoubleMatrix1D partialRowSumVector = itr.next().getBlockVectorElement().get();
                rowSumVector.assign(partialRowSumVector, cern.jet.math.tdouble.DoubleFunctions.plus);
            }

            // Compute dangling node vector
            danglingNodeVector.assign(rowSumVector, this.assignOneIfZero);
            blockVectorElement.set(danglingNodeVector);
            multipleOutputs.write(danglingNodeVectorOutput, key, blockVectorElement, danglingNodeVectorOutput + "/part");

            // Divide rows by rowSumVector, ie. normalize rows
            itr.reset();
            while (itr.hasNext()) {
                BlockMatrixElement blockMatrixElement = itr.next().getBlockMatrixElement();
                LongWritable blockColumn = blockMatrixElement.getBlockRowColumn().getSecond();
                SparseDoubleMatrix2D block = blockMatrixElement.getBlockElement();
                int columns = block.columns();
                for (int i = 0; i < columns; i++) {
                    block.viewColumn(i).assign(rowSumVector, this.zeroSafeDiv);
                }
                context.write(blockColumn, blockMatrixElement);
            }
            multipleOutputs.write(initialRankVectorOutput, key, initialRankVectorElement, initialRankVectorOutput + "/part");
        }
    }
}
