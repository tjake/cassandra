package org.apache.cassandra.io.sstable.format.test.convert;


import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.schema.GroupType;
import parquet.schema.Type;

public class TestGroupConverter extends GroupConverter
{

    private final TestGroupConverter parent;
    private final int index;
    protected TestGroup current;
    private Converter[] converters;

    public TestGroupConverter(TestGroupConverter parent, int index, GroupType schema) {
        this.parent = parent;
        this.index = index;

        converters = new Converter[schema.getFieldCount()];

        for (int i = 0; i < converters.length; i++) {
            final Type type = schema.getType(i);
            if (type.isPrimitive()) {
                converters[i] = new TestPrimitiveConverter(this, i);
            } else {
                converters[i] = new TestGroupConverter(this, i, type.asGroupType());
            }

        }
    }

    @Override
    public Converter getConverter(int fieldIndex)
    {
        return converters[fieldIndex];
    }

    @Override
    public void start()
    {
        current = parent.getCurrentRecord().addGroup(index);
    }

    @Override
    public void end()
    {

    }

    public TestGroup getCurrentRecord() {
        return current;
    }
}
