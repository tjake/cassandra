package org.apache.cassandra.io.sstable.format.test.convert;

import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

/**
 * Created by jake on 8/14/14.
 */
public class TestPrimitiveConverter extends PrimitiveConverter
{

    private final TestGroupConverter parent;
    private final int index;

    public TestPrimitiveConverter(TestGroupConverter parent, int index)
    {
        this.parent = parent;
        this.index = index;
    }

    /**
     * {@inheritDoc}
     * @see parquet.io.api.PrimitiveConverter#addBinary(parquet.io.api.Binary)
     */
    @Override
    public void addBinary(Binary value)
    {
        parent.getCurrentRecord().add(index, value);
    }

    /**
     * {@inheritDoc}
     * @see parquet.io.api.PrimitiveConverter#addBoolean(boolean)
     */
    @Override
    public void addBoolean(boolean value)
    {
        parent.getCurrentRecord().add(index, value);
    }

    /**
     * {@inheritDoc}
     * @see parquet.io.api.PrimitiveConverter#addDouble(double)
     */
    @Override
    public void addDouble(double value)
    {
        parent.getCurrentRecord().add(index, value);
    }

    /**
     * {@inheritDoc}
     * @see parquet.io.api.PrimitiveConverter#addFloat(float)
     */
    @Override
    public void addFloat(float value)
    {
        parent.getCurrentRecord().add(index, value);
    }

    /**
     * {@inheritDoc}
     * @see parquet.io.api.PrimitiveConverter#addInt(int)
     */
    @Override
    public void addInt(int value)
    {
        parent.getCurrentRecord().add(index, value);
    }

    /**
     * {@inheritDoc}
     * @see parquet.io.api.PrimitiveConverter#addLong(long)
     */
    @Override
    public void addLong(long value)
    {
        parent.getCurrentRecord().add(index, value);
    }

}
