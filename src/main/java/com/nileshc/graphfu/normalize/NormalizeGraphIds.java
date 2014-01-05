package com.nileshc.graphfu.normalize;

import com.intel.hadoop.graphbuilder.idnormalize.mapreduce.HashIdMR;
import com.intel.hadoop.graphbuilder.idnormalize.mapreduce.SortDictMR;
import com.intel.hadoop.graphbuilder.idnormalize.mapreduce.SortEdgeMR;
import com.intel.hadoop.graphbuilder.idnormalize.mapreduce.TransEdgeMR;
import com.intel.hadoop.graphbuilder.parser.*;
import com.nileshc.graphfu.matrix.parsers.CDREdgeDataParser;
import com.nileshc.graphfu.matrix.parsers.CDRVertexIdParser;
import java.io.IOException;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class NormalizeGraphIds {

    private static final Logger LOG = Logger.getLogger(NormalizeGraphIds.class);

    class Job {

        /**
         * @param n number of partitions of the dictionary
         */
        public void setDictionaryParts(int n) {
            this.numParts = n;
        }

        /**
         * Running the normalization job. Reads input from {@code input}, and
         * outputs to {@code output} directory.
         *
         * @param nparts
         * @param inputs
         * @param output
         * @return
         * @throws NotFoundException
         * @throws CannotCompileException
         */
        public boolean run(String input, String output) {
            if (numParts <= 0) {
                numParts = 64;
            }

            GraphParser graphparser = new BasicGraphParser();
            FieldParser vidparser = new StringParser();
            FieldParser vdataparser = new EmptyParser();
            FieldParser edataparser = new CDREdgeDataParser();

            try {
                HashIdMR job1 = new HashIdMR(graphparser, vidparser, vdataparser);
                job1.run(input + "/vdata", output);

                SortDictMR job2 = new SortDictMR(numParts, true, vidparser);
                job2.run(output + "/vidmap", output + "/temp/partitionedvidmap");

                SortEdgeMR job3 = new SortEdgeMR(numParts, graphparser, vidparser,
                        edataparser);
                job3.run(input + "/edata", output + "/temp/partitionededata");

                TransEdgeMR job4 = new TransEdgeMR(numParts, output
                        + "/temp/partitionedvidmap", graphparser, vidparser, edataparser);
                job4.run(output + "/temp/partitionededata", output + "/edata");
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }
        private int numParts;
    }

    public static void main(String[] args) throws IOException,
            InstantiationException, IllegalAccessException {
        String input = args[0];
        String output = args[1];
        
        LOG.info("========== Normalizing Graph ============");
        new NormalizeGraphIds().new Job().run(input, output);
        LOG.info("========== Done normalizing graph ============");
    }
}
