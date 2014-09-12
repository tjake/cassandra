package org.apache.cassandra.io.sstable.format.test.convert;

import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.Type;

public class TestGroupWriter
{
    private final RecordConsumer recordConsumer;
    private final GroupType schema;

    public TestGroupWriter(RecordConsumer recordConsumer, GroupType schema)
    {
        this.recordConsumer = recordConsumer;
        this.schema = schema;
    }

    public void write(TestGroup group)
    {
        recordConsumer.startMessage();
        writeGroup(group, schema);
        recordConsumer.endMessage();
    }

    private void writeGroup(TestGroup group, GroupType type)
    {
        int fieldCount = type.getFieldCount();
        for (int field = 0; field < fieldCount; ++field)
        {
            int valueCount = group.getFieldRepetitionCount(field);
            if (valueCount > 0)
            {
                Type fieldType = type.getType(field);
                String fieldName = fieldType.getName();
                recordConsumer.startField(fieldName, field);

                for (int index = 0; index < valueCount; ++index)
                {
                    if (fieldType.isPrimitive())
                    {
                        group.writeValue(field, index, recordConsumer);
                    } else
                    {
                        recordConsumer.startGroup();
                        writeGroup(group.getGroup(field, index), fieldType.asGroupType());
                        recordConsumer.endGroup();
                    }
                }
                recordConsumer.endField(fieldName, field);
            }
        }
    }
}