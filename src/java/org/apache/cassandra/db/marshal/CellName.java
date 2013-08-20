package org.apache.cassandra.db.marshal;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import java.nio.ByteBuffer;
import java.util.List;


public class CellName implements Comparable<CellName>, IMeasurableMemory {
    public static final CellName EMPTY_CELL_NAME = CellName.wrap(ByteBufferUtil.EMPTY_BYTE_BUFFER);

    //The original unmarshalled data
    public final ByteBuffer bb;
    public final Byte qbit;

    private List<CellName> composites;
    private Object type;
    private AbstractType<?> comparator;


    private CellName(ByteBuffer bb) {
        this.bb = bb;
        this.qbit = null;
    }

    private CellName(ByteBuffer bb, Byte qbit) {
        this.bb = bb;
        this.qbit = qbit;
    }

    public List<CellName> getOrSetCompositeCells(AbstractCompositeType marshaller) {
        if (composites == null)
        {
            composites = marshaller.deconstructCells(bb);
        }

        return composites;
    }

    public <T> T getOrSetType(AbstractType<T> comparator) {

        if (type == null) {
            this.comparator = comparator;
            type = comparator.compose(bb);
        }

        return (T) type;
    }

    public AbstractType<?> getComparator() {
        return comparator;
    }

    public boolean isEmpty() {
        return bb == null || bb.remaining() == 0;
    }

    public static CellName wrap(ByteBuffer bb) {
        return new CellName(bb);
    }

    public static CellName wrap(String in) {
        return new CellName(ByteBufferUtil.bytes(in));
    }

    public static CellName wrap(byte[] in) {
        return new CellName(ByteBuffer.wrap(in));
    }

    public static CellName wrap(long in) {
        return new CellName(ByteBufferUtil.bytes(in));
    }

    public static CellName wrap(int in) {
        return new CellName(ByteBufferUtil.bytes(in));
    }

    public static CellName wrap(ByteBuffer bb, AbstractType<?> comparator, Byte qBit)
    {
        CellName cn = new CellName(bb,qBit);
        cn.getOrSetType(comparator);

        return cn;
    }

    @Override
    public int compareTo(CellName other) {
        return bb.compareTo(other.bb);
    }

    //TODO account for marshalled type
    @Override
    public long memorySize() {
        return ObjectSizes.getReferenceSize() + ObjectSizes.getReferenceSize() +
                ObjectSizes.getSize(bb);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CellName cellName = (CellName) o;

        if (bb != null ? !bb.equals(cellName.bb) : cellName.bb != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return bb != null ? bb.hashCode() : 0;
    }
}
