package com.nileshc.graphfu.matrix.types;

import cern.colt.function.tdouble.IntIntDoubleFunction;
import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author nilesh
 */
public class BlockMatrixElement implements Writable {

    SparseDoubleMatrix2D blockElement;
    LongPair blockRowColumn;

    public BlockMatrixElement(int rows, int columns) {
        set(new SparseDoubleMatrix2D(rows, columns), new LongPair());
    }

    public BlockMatrixElement(SparseDoubleMatrix2D matrix, LongPair blockRowColumn) {
        set(matrix, blockRowColumn);
    }

    public BlockMatrixElement() {
        blockRowColumn = new LongPair();
    }

    public void set(SparseDoubleMatrix2D blockElement, LongPair blockRowColumn) {
        this.blockElement = blockElement;
        this.blockRowColumn = blockRowColumn;
    }

    public LongPair getBlockRowColumn() {
        return blockRowColumn;
    }

    public void setBlockRowColumn(LongPair blockRowColumn) {
        this.blockRowColumn = blockRowColumn;
    }

    public SparseDoubleMatrix2D getBlockElement() {
        return blockElement;
    }

    public void setBlockElement(SparseDoubleMatrix2D blockElement) {
        this.blockElement = blockElement;
    }

    public void write(final DataOutput output) throws IOException {
        blockRowColumn.write(output);
        output.writeInt(blockElement.rows());
        output.writeInt(blockElement.cardinality());

        IntIntDoubleFunction elementDumper = new IntIntDoubleFunction() {

            public double apply(int i, int i1, double d) {
                try {
                    output.writeInt(i);
                    output.writeInt(i1);
                    output.writeDouble(d);
                } catch (IOException ex) {
                    Logger.getLogger(BlockMatrixElement.class.getName()).log(Level.SEVERE, null, ex);
                }
                return d;
            }
        };

        blockElement.forEachNonZero(elementDumper);
    }

    public void readFields(DataInput input) throws IOException {
        blockRowColumn.readFields(input);
        int dimension = input.readInt();
        int nnzElements = input.readInt();

        blockElement = new SparseDoubleMatrix2D(dimension, dimension);

        for (int i = 0; i < nnzElements; i++) {
            int row = input.readInt();
            int column = input.readInt();
            double value = input.readDouble();
            blockElement.setQuick(row, column, value);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BlockMatrixElement other = (BlockMatrixElement) obj;
        if (this.blockElement != other.blockElement && (this.blockElement == null || !this.blockElement.equals(other.blockElement))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(blockElement.toArray());
    }

    @Override
    public String toString() {
        return "BlockMatrixElement{" + "blockElement=" + blockElement + ", blockRowColumn=" + blockRowColumn + '}';
    }
}
