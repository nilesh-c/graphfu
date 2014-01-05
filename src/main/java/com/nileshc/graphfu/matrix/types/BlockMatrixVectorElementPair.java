package com.nileshc.graphfu.matrix.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author nilesh
 */
public class BlockMatrixVectorElementPair implements Writable {

    private BlockMatrixElement blockMatrixElement;
    private BlockVectorElement blockVectorElement;

    public BlockMatrixVectorElementPair() {
        this(new BlockMatrixElement(), new BlockVectorElement());
    }

    public BlockMatrixVectorElementPair(BlockMatrixElement blockMatrixElement, BlockVectorElement blockVectorElement) {
        this.blockMatrixElement = blockMatrixElement;
        this.blockVectorElement = blockVectorElement;
    }

    public BlockMatrixElement getBlockMatrixElement() {
        return blockMatrixElement;
    }

    public void setBlockMatrixElement(BlockMatrixElement blockMatrixElement) {
        this.blockMatrixElement = blockMatrixElement;
    }

    public BlockVectorElement getBlockVectorElement() {
        return blockVectorElement;
    }

    public void setBlockVectorElement(BlockVectorElement blockVectorElement) {
        this.blockVectorElement = blockVectorElement;
    }

    public void write(DataOutput output) throws IOException {
        blockMatrixElement.write(output);
        blockVectorElement.write(output);
    }

    public void readFields(DataInput input) throws IOException {
        blockMatrixElement.readFields(input);
        blockVectorElement.readFields(input);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BlockMatrixVectorElementPair other = (BlockMatrixVectorElementPair) obj;
        if (this.blockVectorElement != other.blockVectorElement && (this.blockVectorElement == null || !this.blockVectorElement.equals(other.blockVectorElement))) {
            return false;
        }
        if (this.blockMatrixElement != other.blockMatrixElement && (this.blockMatrixElement == null || !this.blockMatrixElement.equals(other.blockMatrixElement))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 37 * hash + (this.blockMatrixElement != null ? this.blockMatrixElement.hashCode() : 0)
                + (this.blockVectorElement != null ? this.blockVectorElement.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return "BlockMatrixVectorElementPair{" + "blockMatrixElement=" + blockMatrixElement + ", blockVectorElement=" + blockVectorElement + '}';
    }
}
