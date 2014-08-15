package org.apache.cassandra.io.sstable.format.test.convert;


import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.NativeCell;
import parquet.example.data.Group;
import parquet.example.data.simple.*;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;


public class TestGroup
{
    private final GroupType schema;
    private final List<Object>[] data;
    public final List<Integer> setFields;

    public TestGroup(GroupType schema) {
        this.schema = schema;
        this.data = new List[schema.getFields().size()];
        for (int i = 0; i < schema.getFieldCount(); i++) {
            this.data[i] = new ArrayList<>();
        }

        this.setFields = new ArrayList<>();
    }

    public TestGroup(GroupType schema, Cell c)
    {
        this(schema);

        //add the metadata
        add(Schema.TIMESTAMP_FIELD_NAME, c.timestamp());

        if (c instanceof ExpiringCell)
        {
            add(Schema.CELLTYPE_FIELD_NAME, Schema.CellTypes.EXPIRING.ordinal());
            add(Schema.TTL_FIELD_NAME, ((ExpiringCell) c).getTimeToLive());
        }
        else {
            //Don't support everything yet...
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
            List<Object> values = data[i];
            ++i;
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

    public Binary getBinary(String field, int index) {
        return getBinary(getType().getFieldIndex(field), index);
    }

    public Binary getInt96(String field, int index) {
        return getInt96(getType().getFieldIndex(field), index);
    }

    public void add(String field, int value) {
        add(getType().getFieldIndex(field), value);
    }

    public void add(String field, long value) {
        add(getType().getFieldIndex(field), value);
    }

    public void add(String field, float value) {
        add(getType().getFieldIndex(field), value);
    }

    public void add(String field, double value) {
        add(getType().getFieldIndex(field), value);
    }

    public void add(String field, String value) {
        add(getType().getFieldIndex(field), value);
    }

    public void add(String field, NanoTime value) {
        add(getType().getFieldIndex(field), value);
    }

    public void add(String field, boolean value) {
        add(getType().getFieldIndex(field), value);
    }

    public void add(String field, Binary value) {
        add(getType().getFieldIndex(field), value);
    }

    public TestGroup addGroup(String field) {
        return addGroup(getType().getFieldIndex(field));
    }

    public TestGroup asGroup() {
        return this;
    }

    public TestGroup append(String fieldName, int value) {
        add(fieldName, value);
        return this;
    }

    public TestGroup append(String fieldName, float value) {
        add(fieldName, value);
        return this;
    }

    public TestGroup append(String fieldName, double value) {
        add(fieldName, value);
        return this;
    }

    public TestGroup append(String fieldName, long value) {
        add(fieldName, value);
        return this;
    }

    public TestGroup append(String fieldName, NanoTime value) {
        add(fieldName, value);
        return this;
    }

    public TestGroup append(String fieldName, String value) {
        add(fieldName, Binary.fromString(value));
        return this;
    }

    public TestGroup append(String fieldName, boolean value) {
        add(fieldName, value);
        return this;
    }

    public TestGroup append(String fieldName, Binary value) {
        add(fieldName, value);
        return this;
    }

    public TestGroup addGroup(int fieldIndex) {
        TestGroup g = new TestGroup(schema.getType(fieldIndex).asGroupType());
        data[fieldIndex].add(g);
        return g;
    }

    public TestGroup getGroup(int fieldIndex, int index) {
        return (TestGroup)getValue(fieldIndex, index);
    }

    private Object getValue(int fieldIndex, int index) {
        List<Object> list;
        try {
            list = data[fieldIndex];
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("not found " + fieldIndex + "(" + schema.getFieldName(fieldIndex) + ") in group:\n" + this);
        }
        try {
            return list.get(index);
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("not found " + fieldIndex + "(" + schema.getFieldName(fieldIndex) + ") element number " + index + " in group:\n" + this);
        }
    }

    private void add(int fieldIndex, Primitive value) {

        setFields.add(fieldIndex);

        Type type = schema.getType(fieldIndex);
        List<Object> list = data[fieldIndex];
        if (!type.isRepetition(Type.Repetition.REPEATED)
                && !list.isEmpty()) {
            throw new IllegalStateException("field "+fieldIndex+" (" + type.getName() + ") can not have more than one value: " + list);
        }
        list.add(value);
    }

    public int getFieldRepetitionCount(int fieldIndex) {
        List<Object> list = data[fieldIndex];
        return list == null ? 0 : list.size();
    }

    public String getValueToString(int fieldIndex, int index) {
        return String.valueOf(getValue(fieldIndex, index));
    }

    public String getString(int fieldIndex, int index) {
        return ((BinaryValue)getValue(fieldIndex, index)).getString();
    }

    public int getInteger(int fieldIndex, int index) {
        return ((IntegerValue)getValue(fieldIndex, index)).getInteger();
    }

    public boolean getBoolean(int fieldIndex, int index) {
        return ((BooleanValue)getValue(fieldIndex, index)).getBoolean();
    }

    public long getLong(int fieldIndex, int index) {
        return ((LongValue)getValue(fieldIndex, index)).getLong();
    }

    public Binary getBinary(int fieldIndex, int index) {
        return ((BinaryValue)getValue(fieldIndex, index)).getBinary();
    }

    public NanoTime getTimeNanos(int fieldIndex, int index) {
        return NanoTime.fromInt96((Int96Value)getValue(fieldIndex, index));
    }

    public Binary getInt96(int fieldIndex, int index) {
        return ((Int96Value)getValue(fieldIndex, index)).getInt96();
    }

    public void add(int fieldIndex, int value) {
        add(fieldIndex, new IntegerValue(value));
    }

    public void add(int fieldIndex, long value) {
        add(fieldIndex, new LongValue(value));
    }

    public void add(int fieldIndex, String value) {
        add(fieldIndex, new BinaryValue(Binary.fromString(value)));
    }

    public void add(int fieldIndex, NanoTime value) {
        add(fieldIndex, value.toInt96());
    }

    public void add(int fieldIndex, boolean value) {
        add(fieldIndex, new BooleanValue(value));
    }

    public void add(int fieldIndex, Binary value) {
        switch (getType().getType(fieldIndex).asPrimitiveType().getPrimitiveTypeName()) {
            case BINARY:
                add(fieldIndex, new BinaryValue(value));
                break;
            case INT96:
                add(fieldIndex, new Int96Value(value));
                break;
            default:
                throw new UnsupportedOperationException(
                        getType().asPrimitiveType().getName() + " not supported for Binary");
        }
    }

    public void add(int fieldIndex, float value) {
        add(fieldIndex, new FloatValue(value));
    }

    public void add(int fieldIndex, double value) {
        add(fieldIndex, new DoubleValue(value));
    }

    public GroupType getType() {
        return schema;
    }

    public void writeValue(int field, int index, RecordConsumer recordConsumer) {
        ((Primitive)getValue(field, index)).writeValue(recordConsumer);
    }
}
