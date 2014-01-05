package com.nileshc.graphfu.pagerank;

import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix1D;
import com.nileshc.graphfu.matrix.types.BlockVectorElement;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
public class RankVectorNormalize {

    private double rightHandSideSum;
    private double rankVectorSum;

    public RankVectorNormalize(double rightHandSideSum, double rankVectorSum) {
        this.rightHandSideSum = rightHandSideSum;
        this.rankVectorSum = rankVectorSum;
    }

    public int run(String oldRankVectorInput, String newRankVectorInput, String output, String l1NormOutput) throws Exception {
        Configuration conf = new Configuration();
        conf.set("rightHandSideSum", rightHandSideSum + "");
        conf.set("rankVectorSum", rankVectorSum + "");

        Job job = new Job(conf, "Rank Vector Normalize");
        job.setJarByClass(RankVectorNormalize.class);

        job.setMapperClass(RankVectorNormalize.RankVectorNormalizeMapper.class);
        job.setReducerClass(RankVectorNormalize.RankVectorNormalizeReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(BlockVectorElement.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(BlockVectorElement.class);

        job.setInputFormatClass(CompositeInputFormat.class);
        String joinStatement = CompositeInputFormat.compose("inner", SequenceFileInputFormat.class, oldRankVectorInput, newRankVectorInput);
        conf.set("mapred.join.expr", joinStatement);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(output));

        MultipleOutputs.addNamedOutput(job, l1NormOutput, SequenceFileOutputFormat.class, NullWritable.class, DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class RankVectorNormalizeMapper extends Mapper<LongWritable, TupleWritable, LongWritable, TupleWritable> {

        private double rightHandSideSum;
        private double rankVectorSum;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            rightHandSideSum = Double.parseDouble(context.getConfiguration().get("rightHandSideSum"));
            rankVectorSum = Double.parseDouble(context.getConfiguration().get("rankVectorSum"));
        }

        @Override
        protected void map(LongWritable key, TupleWritable value, Context context) throws IOException, InterruptedException {
            BlockVectorElement blockVectorElement = (BlockVectorElement) value.get(1);
            blockVectorElement.get().assign(cern.jet.math.tdouble.DoubleFunctions.plus(rightHandSideSum)).assign(cern.jet.math.tdouble.DoubleFunctions.div(rankVectorSum));
            context.write(key, value);
        }
    }

    static class RankVectorNormalizeReducer extends Reducer<LongWritable, TupleWritable, LongWritable, BlockVectorElement> {

        private DoubleWritable doubleWritable;
        private String l1NormOutput;
        private MultipleOutputs multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            doubleWritable = new DoubleWritable();
            l1NormOutput = context.getConfiguration().get("l1NormOutput");
            multipleOutputs = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<TupleWritable> values, Context context) throws IOException, InterruptedException {
            cern.jet.math.tdouble.DoubleFunctions F = cern.jet.math.tdouble.DoubleFunctions.functions;
            for (TupleWritable value : values) {
                SparseDoubleMatrix1D oldVector = ((BlockVectorElement) value.get(0)).get();
                SparseDoubleMatrix1D newVector = ((BlockVectorElement) value.get(1)).get();
                doubleWritable.set(oldVector.assign(newVector, F.chain(F.abs, F.minus)).zSum());
                multipleOutputs.write(l1NormOutput, NullWritable.get(), doubleWritable, l1NormOutput + "/part");
            }
        }
    }
}
