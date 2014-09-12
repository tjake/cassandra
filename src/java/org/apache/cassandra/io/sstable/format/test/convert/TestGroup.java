package org.apache.cassandra.io.sstable.format.test.convert;


import com.google.common.collect.Lists;
import org.apache.cassandra.db.*;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroup;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.Type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Based off some parquet sample, much to be desired.
 * TODO: FIXME
 *
 */
public class TestGroup implements Iterator<Integer>
{
    private final GroupType schema;
    private final Object[] data;

    //Since we have so many optional fields it helps to track only the ones set
    private final List<Integer> setFields;
    private Iterator<Integer> setFieldsIterator = null;
    private int fieldsRead = 0;

    public TestGroup(GroupType schema) {
        this.schema = schema;
        this.data = new Object[schema.getFieldCount()];
        this.setFields = new ArrayList<>(schema.getFieldCount());
    }

    public TestGroup(GroupType schema, OnDiskAtom c)
    {
        this(schema);

        //add the metadata
        add(Schema.TIMESTAMP_FIELD_NAME, c.timestamp());

        //TODO Switch
        if (c instanceof ExpiringCell)
        {
            add(Schema.CELLTYPE_FIELD_NAME, Schema.CellTypes.EXPIRING.ordinal());
            add(Schema.TTL_FIELD_NAME, ((ExpiringCell) c).getTimeToLive());
        }
        else if (c instanceof DeletedCell)
        {
            add(Schema.CELLTYPE_FIELD_NAME, Schema.CellTypes.TOMBSTONE.ordinal());
            add(Schema.DELETION_TIME_FIELD_NAME, c.getLocalDeletionTime());
        }
        else if (c instanceof RangeTombstone)
        {
            add(Schema.CELLTYPE_FIELD_NAME, Schema.CellTypes.RANGETOMBSTONE.ordinal());
            add(Schema.DELETION_TIME_FIELD_NAME, c.getLocalDeletionTime());
        }
        else if (c instanceof CounterCell)
        {
            add(Schema.CELLTYPE_FIELD_NAME, Schema.CellTypes.COUNTER.ordinal());
        }
        else
        {
            assert c instanceof BufferCell || c instanceof NativeCell;

            add(Schema.CELLTYPE_FIELD_NAME, Schema.CellTypes.NORMAL.ordinal());
        }
    }

    @Override
    public String toString() {
        return toString("");
    }

    public String toString(String indent) {
        String result = "";
        int i = 0;
        for (Type field : schema.getFields()) {
            String name = field.getName();
            Object valuesObj = data[i];
            ++i;
            List values = valuesObj instanceof List ? (List) valuesObj : Lists.newArrayList(valuesObj);

            if (values != null) {
                if (values.size() > 0) {
                    for (Object value : values) {
                        result += indent + name;
                        if (value == null) {
                            result += ": NULL\n";
                        } else if (value instanceof Group) {
                            result += "\n" + ((SimpleGroup)value).toString(indent+"  ");
                        } else {
                            result += ": " + value.toString() + "\n";
                        }
                    }
                }
            }
        }
        return result;
    }

    public int getFieldRepetitionCount(String field) {
        return getFieldRepetitionCount(getType().getFieldIndex(field));
    }

    public TestGroup getGroup(String field) {
        return getGroup(getType().getFieldIndex(field), 0);
    }

    public String getString(String field) {
        return getString(getType().getFieldIndex(field), 0);
    }

    public int getInteger(String field) {
        return getInteger(getType().getFieldIndex(field), 0);
    }

    public boolean getBoolean(String field) {
        return getBoolean(getType().getFieldIndex(field), 0);
    }

    public Long getLong(String field) {
        return getLong(getType().getFieldIndex(field), 0);
    }

    public Float getFloat(String field)
    {
        return getFloat(getType().getFieldIndex(field), 0);
    }

    public Double getDouble(String field) {
        return getDouble(getType().getFieldIndex(field), 0);
    }

    public Binary getBinary(String field) {
        return getBinary(getType().getFieldIndex(field), 0);
    }

    public Binary getInt96(String field) {
        return getInt96(getType().getFieldIndex(field), 0);
    }



    public TestGroup getGroup(String field, int index) {
        return getGroup(getType().getFieldIndex(field), index);
    }

    public String getString(String field, int index) {
        return getString(getType().getFieldIndex(field), index);
    }

    public int getInteger(String field, int index) {
        return getInteger(getType().getFieldIndex(field), index);
    }

    public boolean getBoolean(String field, int index) {
        return getBoolean(getType().getFieldIndex(field), index);
    }

    public Long getLong(String field, int index) {
        return getLong(getType().getFieldIndex(field), index);
    }

    public Float getFloat(String field, int index) {
        return getFloat(getType().getFieldIndex(field), index);
    }

    public Double getDouble(String field, int index) {
        return getDouble(getType().getFieldIndex(field), index);
    }

    public Binary getBinary(String field, int index) {
        return getBinary(getType().getFieldIndex(field), index);
    }

    public Binary getInt96(String field, int index) {
        return getInt96(getType().getFieldIndex(field), index);
    }



    public void add(String field, String value) {
        add(getType().getFieldIndex(field), value);
    }

    public void add(String field, Object value) {
        add(getType().getFieldIndex(field), value);
    }


    public TestGroup addGroup(String field) {
        return addGroup(getType().getFieldIndex(field));
    }

    public TestGroup asGroup() {
        return this;
    }



    public TestGroup addGroup(int fieldIndex) {
        Type type = schema.getType(fieldIndex);

        TestGroup g = new TestGroup(type.asGroupType());

        if (!type.isRepetition(Type.Repetition.REPEATED))
        {
            assert data[fieldIndex] == null;
            data[fieldIndex] = g;
        }
        else
        {
            if (data[fieldIndex] == null)
                data[fieldIndex] = Lists.newArrayList(g);
            else
                data[fieldIndex] = g;
        }


        return g;
    }

    public int getFieldsRead()
    {
        return fieldsRead;
    }

    public TestGroup getGroup(int fieldIndex, int index) {
        return (TestGroup)getValue(fieldIndex, index);
    }

    private Object getValue(int fieldIndex, int index) {

        Type type = schema.getType(fieldIndex);
        fieldsRead++;

        if (!type.isRepetition(Type.Repetition.REPEATED))
        {
            assert index == 0;
            return data[fieldIndex];
        }
        else
        {
            try
            {
                return data[fieldIndex] == null ? null : ((List)data[fieldIndex]).get(index);
            } catch (IndexOutOfBoundsException e)
            {
                throw new RuntimeException("not found " + fieldIndex + "(" + schema.getFieldName(fieldIndex) + ") in group:\n" + this);
            }
        }
    }

    public void add(int fieldIndex, Object value) {

        setFields.add(fieldIndex);
        Type type = schema.getType(fieldIndex);

        if (value instanceof String)
            value = Binary.fromString((String)value);

        if (!type.isRepetition(Type.Repetition.REPEATED))
        {
            assert data[fieldIndex] == null;
            data[fieldIndex] = value;
        }
        else
        {
            if (data[fieldIndex] == null)
                data[fieldIndex] = Lists.newArrayList(value);
            else
                ((List)data[fieldIndex]).add(value);
        }

    }

    public int getFieldRepetitionCount(int fieldIndex) {
        Type type = schema.getType(fieldIndex);

        if (!type.isRepetition(Type.Repetition.REPEATED))
        {
            return data[fieldIndex] == null ? 0 : 1;
        }
        else
        {
            return data[fieldIndex] == null ? 0 : ((List)data[fieldIndex]).size();
        }
    }

    public String getValueToString(int fieldIndex, int index) {
        return String.valueOf(getValue(fieldIndex, index));
    }

    public String getString(int fieldIndex) {
        return ((Binary)getValue(fieldIndex, 0)).toStringUsingUTF8();
    }

    public int getInteger(int fieldIndex) {
        return (int)getValue(fieldIndex, 0);
    }

    public boolean getBoolean(int fieldIndex) {
        return (boolean)getValue(fieldIndex, 0);
    }

    public long getLong(int fieldIndex) {
        return (long)getValue(fieldIndex, 0);
    }

    public double getDouble(int fieldIndex)
    {
        return (double)getValue(fieldIndex, 0);
    }

    public Binary getBinary(int fieldIndex) {
        return (Binary)getValue(fieldIndex, 0);
    }

    public Binary getInt96(int fieldIndex) {
        return (Binary)getValue(fieldIndex, 0);
    }

    public String getString(int fieldIndex, int index) {
        return ((Binary)getValue(fieldIndex, index)).toStringUsingUTF8();
    }

    public int getInteger(int fieldIndex, int index) {
        return (int)getValue(fieldIndex, index);
    }

    public boolean getBoolean(int fieldIndex, int index) {
        return (boolean)getValue(fieldIndex, index);
    }

    public long getLong(int fieldIndex, int index) {
        return (long)getValue(fieldIndex, index);
    }

    public double getDouble(int fieldIndex, int index)
    {
        return (double)getValue(fieldIndex, index);
    }

    public float getFloat(int fieldIndex, int index)
    {
        return (float)getValue(fieldIndex, index);
    }

    public Binary getBinary(int fieldIndex, int index) {
        return (Binary)getValue(fieldIndex, index);
    }

    public Binary getInt96(int fieldIndex, int index) {
        return (Binary)getValue(fieldIndex, index);
    }



    public GroupType getType() {
        return schema;
    }

    public void writeValue(int field, int index, RecordConsumer recordConsumer) {

        switch (getType().getType(field).asPrimitiveType().getPrimitiveTypeName())
        {
            case INT64:
                recordConsumer.addLong(getLong(field, index));
                break;
            case INT32:
                recordConsumer.addInteger(getInteger(field, index));
                break;
            case FLOAT:
                recordConsumer.addFloat(getFloat(field, index));
                break;
            case DOUBLE:
                recordConsumer.addDouble(getDouble(field, index));
                break;
            case INT96:
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                recordConsumer.addBinary(getBinary(field, index));
                break;
            case BOOLEAN:
                recordConsumer.addBoolean(getBoolean(field, index));
                break;
            default:
                throw new UnsupportedOperationException(
                        getType().asPrimitiveType().getName() + " not supported for recordConsumer");
        }
    }


    //We want to be able to iterate over the fields
    //In the order they were added.  So this can only
    //safely be called on read after the group is materialized
    void maybeInitIterator()
    {
        if (setFieldsIterator == null)
            setFieldsIterator = setFields.iterator();
    }

    @Override
    public boolean hasNext()
    {
        maybeInitIterator();

        return setFieldsIterator.hasNext();
    }

    @Override
    public Integer next()
    {
        maybeInitIterator();

        return setFieldsIterator.next();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
