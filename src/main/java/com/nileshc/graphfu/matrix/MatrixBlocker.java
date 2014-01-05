package com.nileshc.graphfu.matrix;

import cern.colt.matrix.tdouble.impl.SparseCCDoubleMatrix2D;
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
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author nilesh
 */
public class MatrixBlocker {

    private long blockSize;

    public MatrixBlocker(int blockSize) {
        this.blockSize = blockSize;
    }

    public int run(String edgeListInput, String blockedMatrixOutput) throws Exception {
        Configuration conf = new Configuration();
        conf.setLong("blockSize", blockSize);

        Job job = new Job(conf, "Matrix Blocker");
        job.setJarByClass(MatrixBlocker.class);
        job.setSortComparatorClass(LongPair.Comparator.class);

        job.setMapperClass(MatrixBlockerMapper.class);
        job.setReducerClass(MatrixBlockerReducer.class);

        job.setMapOutputKeyClass(LongPair.class);
        job.setMapOutputValueClass(LongDoubleEdgeWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(BlockMatrixElement.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(edgeListInput));
        FileOutputFormat.setOutputPath(job, new Path(blockedMatrixOutput));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class MatrixBlockerMapper extends Mapper<LongWritable, Text, LongPair, LongDoubleEdgeWritable> {

        private long blockSize;
        private LongDoubleEdgeWritable edgeWritable;
        private LongPair rowColumnPair;
        private LongWritable longWritable1;
        private LongWritable longWritable2;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.blockSize = context.getConfiguration().getLong("blockSize", 1);
            this.edgeWritable = new LongDoubleEdgeWritable();
            this.rowColumnPair = new LongPair();
            this.longWritable1 = new LongWritable();
            this.longWritable2 = new LongWritable();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            long source = Long.parseLong(stringTokenizer.nextToken());
            long target = Long.parseLong(stringTokenizer.nextToken());
            double data = Double.parseDouble(stringTokenizer.nextToken());

            this.longWritable1.set(source / this.blockSize);
            this.longWritable2.set(target / this.blockSize);
            this.rowColumnPair.set(this.longWritable1, this.longWritable2);

            this.edgeWritable.source().set(source % blockSize);
            this.edgeWritable.target().set(target % blockSize);
            this.edgeWritable.EdgeData().set(data);

            context.write(this.rowColumnPair, this.edgeWritable);
        }
    }

    static class MatrixBlockerReducer extends Reducer<LongPair, LongDoubleEdgeWritable, LongWritable, BlockMatrixElement> {

        private long blockSize;
        BlockMatrixElement blockMatrixElement;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.blockMatrixElement = new BlockMatrixElement();
            this.blockSize = context.getConfiguration().getLong("blockSize", 1);
        }

        @Override
        protected void reduce(LongPair key, Iterable<LongDoubleEdgeWritable> values, Context context) throws IOException, InterruptedException {
            SparseDoubleMatrix2D matrix = new SparseDoubleMatrix2D((int) this.blockSize, (int) this.blockSize);
            for (LongDoubleEdgeWritable edgeWritable : values) {
                Edge<LongWritable, DoubleWritable> edge = edgeWritable.get();
                matrix.setQuick((int) edge.source().get(), (int) edge.target().get(), edge.EdgeData().get());
            }
            this.blockMatrixElement.set(matrix, key);
            context.write(key.getFirst(), blockMatrixElement);
        }
    }
}
