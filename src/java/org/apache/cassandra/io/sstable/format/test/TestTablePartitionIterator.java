package org.apache.cassandra.io.sstable.format.test;


import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.test.convert.Schema;
import org.apache.cassandra.io.sstable.format.test.convert.TestGroup;
import org.apache.cassandra.io.sstable.format.test.convert.TestGroupConverter;
import org.apache.cassandra.io.sstable.format.test.convert.TestRecordMaterializer;
import org.apache.cassandra.io.util.FileDataInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.example.data.Group;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordReader;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestTablePartitionIterator implements Iterator<OnDiskAtom>
{
    private final FileDataInput in;
    private final ColumnSerializer.Flag flag;
    private final int expireBefore;
    private final CFMetaData cfm;
    private final Version version;
    private final ParquetRowGroupReader reader;
    private final Iterator<PageReadStore> pageStoreIterator;
    private PageReadStore currentPageStore;
    private long totalNumberRead;
    private int numberReadinPageStore;

    private MessageColumnIO messageIO;
    private RecordMaterializer<TestGroup> recordConverter;
    private RecordReader<TestGroup> recordReader;
    private final MessageType schema;


    public TestTablePartitionIterator(FileDataInput in, ColumnSerializer.Flag flag, int expireBefore, CFMetaData cfm, Version version)
    {
        this.in = in;
        this.flag = flag;
        this.expireBefore = expireBefore;
        this.cfm = cfm;
        this.version = version;

        reader = new ParquetRowGroupReader(version, in, false);
        pageStoreIterator = reader.iterator();

        assert pageStoreIterator.hasNext();

        currentPageStore = pageStoreIterator.next();

        schema = TestTableWriter.getParquetSchema(cfm);
        messageIO  = new ColumnIOFactory().getColumnIO(schema);
        recordConverter = new TestRecordMaterializer(schema);
        recordReader = messageIO.getRecordReader(currentPageStore, recordConverter);

        totalNumberRead = 0;
        numberReadinPageStore = 0;
     }

    @Override
    public boolean hasNext()
    {
        return totalNumberRead < reader.getTotalRowCount();
    }

    private ByteBuffer getValue(PrimitiveType type, TestGroup g)
    {
        switch (type.getPrimitiveTypeName())
        {
            case FIXED_LEN_BYTE_ARRAY:
            case BINARY:
                return g.getBinary(type.getName(), 0).toByteBuffer();
            case INT32:
                return Int32Type.instance.decompose(g.getInteger(type.getName(), 0));
            case INT64:
                return LongType.instance.decompose(g.getLong(type.getName(), 0));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public OnDiskAtom next()
    {
        if (!hasNext())
            return null;

        //Maybe iterate to the next row group
        if (numberReadinPageStore >= currentPageStore.getRowCount())
        {
            assert pageStoreIterator.hasNext();

            currentPageStore = pageStoreIterator.next();
            recordReader = messageIO.getRecordReader(currentPageStore, recordConverter);
            numberReadinPageStore = 0;
        }

        TestGroup record = recordReader.read();


        if (cfm.hasStaticColumns() || cfm.clusteringColumns().size() > 0 )
            throw new UnsupportedOperationException();

        //Meta info
        long timestamp = record.getLong(Schema.TIMESTAMP_FIELD_NAME, 0);
        Schema.CellTypes type = Schema.CellTypes.values()[record.getInteger(Schema.CELLTYPE_FIELD_NAME, 0)];

        Cell cell = null;
        switch (type)
        {
            case NORMAL:
                assert record.setFields.size() == 3;
                Type t = schema.getFields().get(record.setFields.get(2));
                assert t.isPrimitive();

                cell = AbstractCell.create(cfm.comparator.makeCellName(t.getName()), getValue((PrimitiveType)t, record), timestamp, -1, cfm);
                break;
            default:
                throw new UnsupportedOperationException();
        }

        totalNumberRead++;
        numberReadinPageStore++;

        return cell;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
