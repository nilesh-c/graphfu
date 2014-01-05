package com.nileshc.graphfu;

import com.nileshc.graphfu.matrix.MatrixBlocker;
import com.nileshc.graphfu.pagerank.BlockMatVec;
import com.nileshc.graphfu.pagerank.PageRankPrep;
import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;

/**
 * Hello world!
 *
 */
public class Main {

    public static void main(String[] args) throws Exception {
        int blockSize = 53;
        int numNodes = 106;
        switch (args[0]) {
            case "block":
                MatrixBlocker matrixBlocker = new MatrixBlocker(blockSize);
                matrixBlocker.run(args[1], args[2]);
                break;
            case "prep":
                PageRankPrep pageRankPrep = new PageRankPrep(blockSize, numNodes);
                pageRankPrep.run(args[1], args[2], args[3], args[4]);
            case "mult":
                BlockMatVec blockMatVec = new BlockMatVec(0.85f, blockSize);
                blockMatVec.run(args[1], args[2], args[3], args[4], args[5], args[6]);
        }


    }
}
