package com.nileshc.graphfu.pagerank.sequencefilesum;

import java.io.IOException;

/**
 *
 * @author nilesh
 */
public class RightHandSideSummer extends SequenceFileSummer {

    private double alpha;

    public RightHandSideSummer(String input, double alpha) {
        super(input);
        this.alpha = alpha;
    }

    @Override
    public double getSum() throws IOException, InstantiationException, IllegalAccessException {
        return (super.getSum() * alpha + 1 - alpha);
    }
}
