package com.nileshc.graphfu.matrix.types;

import cern.colt.function.tdouble.IntIntDoubleFunction;
import cern.colt.matrix.tdouble.impl.SparseDoubleMatrix1D;
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
public class BlockVectorElement implements Writable {

    SparseDoubleMatrix1D vector;

    public BlockVectorElement(int rows) {
        set(new SparseDoubleMatrix1D(rows));
    }

    public BlockVectorElement(SparseDoubleMatrix1D vector) {
        set(vector);
    }

    public BlockVectorElement() {
    }

    public void set(SparseDoubleMatrix1D vector) {
        this.vector = vector;
    }

    public SparseDoubleMatrix1D get() {
        return vector;
    }

    public void write(final DataOutput output) throws IOException {
        int size = (int) vector.size();
        output.writeInt(size);
        output.writeInt(vector.cardinality());

        for (int i = 0; i < size; i++) {
            double value = vector.getQuick(i);
            if (value != 0) {
                output.writeInt(i);
                output.writeDouble(value);
            }
        }
    }

    public void readFields(DataInput input) throws IOException {
        int size = input.readInt();
        int nnzElements = input.readInt();

        vector = new SparseDoubleMatrix1D(size);

        for (int i = 0; i < nnzElements; i++) {
            int row = input.readInt();
            double value = input.readDouble();
            vector.setQuick(row, value);
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
        final BlockVectorElement other = (BlockVectorElement) obj;
        if (this.vector != other.vector && (this.vector == null || !this.vector.equals(other.vector))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(vector.toArray());
    }

    @Override
    public String toString() {
        return "BlockVectorElement{" + "vector=" + vector + '}';
    }
}
