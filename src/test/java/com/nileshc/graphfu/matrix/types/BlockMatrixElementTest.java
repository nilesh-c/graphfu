package com.nileshc.graphfu.matrix.types;

import cern.colt.matrix.tdouble.DoubleFactory2D;
import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix2D;
import com.nileshc.graphfu.testutils.WritableTestUtils;
import junit.framework.TestCase;

/**
 *
 * @author nilesh
 */
public class BlockMatrixElementTest extends TestCase {

    public BlockMatrixElementTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testWriteRead() throws Exception {
        DoubleFactory2D factory = DoubleFactory2D.sparse;
        SparseDoubleMatrix2D matrix = (SparseDoubleMatrix2D) factory.random(10, 10);
        BlockMatrixElement instance = new BlockMatrixElement(matrix, new LongPair());
        byte[] bytes = WritableTestUtils.serialize(instance);
        BlockMatrixElement unserializedInstance = WritableTestUtils.asWritable(bytes, BlockMatrixElement.class);
        assertEquals("Serialized/Unserialized matrices not equal: Reading and writing test failed!", unserializedInstance.getBlockElement().toString(), matrix.toString());
    }
}
