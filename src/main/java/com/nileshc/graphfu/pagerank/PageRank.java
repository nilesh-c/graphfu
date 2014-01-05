package com.nileshc.graphfu.pagerank;

import com.nileshc.graphfu.matrix.MatrixBlocker;

/**
 *
 * @author nilesh
 */
public class PageRank {

    public static void main(String... args) throws Exception {
        MatrixBlocker matrixBlocker = new MatrixBlocker(Integer.parseInt(args[2]));
        matrixBlocker.run(args[0], args[1]);
    }
}
