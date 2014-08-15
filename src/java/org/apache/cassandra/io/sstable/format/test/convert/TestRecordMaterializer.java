package org.apache.cassandra.io.sstable.format.test.convert;

import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;


public class TestRecordMaterializer extends RecordMaterializer<TestGroup>
{
    private final MessageType schema;
    private TestGroupConverter root;

    public TestRecordMaterializer(MessageType schema_) {
        this.schema = schema_;
        this.root = new TestGroupConverter(null, 0, schema) {
            @Override
            public void start() {
                this.current = new TestGroup(schema);
            }

            @Override
            public void end() {
            }
        };
    }

    @Override
    public TestGroup getCurrentRecord() {
        return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }

}

