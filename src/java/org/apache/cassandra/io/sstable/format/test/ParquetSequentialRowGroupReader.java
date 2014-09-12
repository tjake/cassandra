package org.apache.cassandra.io.sstable.format.test;

import com.google.common.primitives.Longs;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.test.convert.TDataInputTransport;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.DictionaryPage;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.format.*;
import parquet.io.ParquetDecodingException;
import parquet.org.apache.thrift.TException;
import parquet.org.apache.thrift.protocol.TCompactProtocol;
import parquet.org.apache.thrift.transport.TTransport;
import parquet.schema.PrimitiveType;

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a special version of the RowGroupReader that
 * is used specifically for Streamed data.
 */
public class ParquetSequentialRowGroupReader implements DeletionTimeAwarePageReader
{
    private final DataInput in;

    private SequentialPageReadStore next;
    private boolean finished;
    private DeletionTime deletionTime;

    public ParquetSequentialRowGroupReader(DataInput in)
    {
        this.in = in;
        finished = false;
    }

    @Override
    public boolean hasNext()
    {

        if (next != null)
            return true;

        if (finished)
            return false;

        try
        {
            next = new SequentialPageReadStore();
        } catch (FoundFooterException e)
        {
            finished = true;
            return false;
        }

        return true;
    }

    @Override
    public PageReadStore next()
    {
        if (!hasNext())
            return null;

        SequentialPageReadStore tmp = next;
        next = null;

        return tmp;
    }


    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public DeletionTime getDeletionTime()
    {
        return deletionTime;
    }

    class FoundFooterException extends Exception
    {

    }

    class SequentialPageReadStore implements PageReadStore
    {
        private final Map<ColumnDescriptor, PageReader> rowGroup = new HashMap<>();
        private final long numberOfRows;

        SequentialPageReadStore() throws FoundFooterException
        {
            try
            {
                //Make sure this isnt the start of the footer
                byte[] magic = new byte[ParquetRowGroupReader.START_OF_FOOTER_MAGIC.length];
                assert magic.length == Longs.BYTES;

                in.readFully(magic);

                //No more row groups to read, so read the deletion info and return
                if (Arrays.equals(magic, ParquetRowGroupReader.START_OF_FOOTER_MAGIC))
                {
                    deletionTime = DeletionTime.serializer.deserialize(in);

                    //Skip the footer
                    skipFooter();

                    throw new FoundFooterException();
                }

                ///Read the next Row group
                numberOfRows = LongType.instance.compose(ByteBuffer.wrap(magic));
                int numColumnChunks = in.readInt();

                for (int i = 0; i < numColumnChunks; i++)
                {
                    TestPageReader pageReader = new TestPageReader(in.readInt());
                    rowGroup.put(pageReader.getColumnDescriptor(), pageReader);
                }

            } catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        private void skipFooter() throws IOException
        {
            int numRowGroups = in.readInt();

            try
            {
                TTransport t = new TDataInputTransport(in);
                TCompactProtocol protocol = new TCompactProtocol(t);

                for (int i = 0; i < numRowGroups; i++)
                {
                    RowGroup rg = new RowGroup();
                    rg.read(protocol);
                }
            } catch (TException e)
            {
                throw new ParquetDecodingException(e);
            }

            long offset = in.readLong();
            byte[] magic = new byte[ParquetRowGroupReader.END_OF_FOOTER_MAGIC.length];

            in.readFully(magic);

            assert Arrays.equals(magic, ParquetRowGroupReader.END_OF_FOOTER_MAGIC);
        }

        @Override
        public PageReader getPageReader(ColumnDescriptor descriptor)
        {
            return rowGroup.get(descriptor);
        }

        @Override
        public long getRowCount()
        {
            return numberOfRows;
        }
    }

    public parquet.column.Encoding getEncoding(parquet.format.Encoding encoding)
    {
        return parquet.column.Encoding.valueOf(encoding.name());
    }

    class TestPageReader implements PageReader
    {
        final ColumnMetaData columnMetaData;
        final Page[] pages;
        int currentPage;


        public TestPageReader(int numPages)
        {
            pages = new Page[numPages];

            for (int i = 0; i < numPages; i++)
            {
                pages[i] = readPageInternal();
            }

            columnMetaData = readColumnMetaData();
        }

        @Override
        public DictionaryPage readDictionaryPage()
        {
            return null;
        }

        @Override
        public long getTotalValueCount()
        {
            return columnMetaData.getNum_values();
        }

        @Override
        public Page readPage()
        {
            assert currentPage < pages.length;

            return pages[currentPage++];
        }

        public ColumnDescriptor getColumnDescriptor()
        {
            return new ColumnDescriptor(
                    columnMetaData.getPath_in_schema().toArray(new String[]{}),
                    getType(columnMetaData.getType()), 1, 1);
        }

        private Page readPageInternal()
        {
            try
            {
                //Deserialize page header
                PageHeader header = new PageHeader();

                TTransport t = new TDataInputTransport(in);
                TCompactProtocol protocol = new TCompactProtocol(t);
                header.read(protocol);

                byte[] buffer = new byte[header.getUncompressed_page_size()];
                in.readFully(buffer);
                BytesInput bytes = BytesInput.from(buffer);

                Page p = new Page(bytes, header.data_page_header.num_values, header.uncompressed_page_size, null,
                        getEncoding(header.data_page_header.repetition_level_encoding),
                        getEncoding(header.data_page_header.definition_level_encoding),
                        getEncoding(header.data_page_header.encoding));


                return p;
            } catch (TException | IOException e)
            {
                throw new ParquetDecodingException(e);
            }
        }

        private ColumnMetaData readColumnMetaData()
        {
            try
            {
                ColumnMetaData cmd = new ColumnMetaData();

                TTransport t = new TDataInputTransport(in);
                TCompactProtocol protocol = new TCompactProtocol(t);
                cmd.read(protocol);

                return cmd;
            } catch (TException e)
            {
                throw new ParquetDecodingException(e);
            }
        }

        PrimitiveType.PrimitiveTypeName getType(Type type)
        {
            switch (type)
            {
                case INT64:
                    return PrimitiveType.PrimitiveTypeName.INT64;
                case INT32:
                    return PrimitiveType.PrimitiveTypeName.INT32;
                case BOOLEAN:
                    return PrimitiveType.PrimitiveTypeName.BOOLEAN;
                case BYTE_ARRAY:
                    return PrimitiveType.PrimitiveTypeName.BINARY;
                case FLOAT:
                    return PrimitiveType.PrimitiveTypeName.FLOAT;
                case DOUBLE:
                    return PrimitiveType.PrimitiveTypeName.DOUBLE;
                case INT96:
                    return PrimitiveType.PrimitiveTypeName.INT96;
                case FIXED_LEN_BYTE_ARRAY:
                    return PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
                default:
                    throw new RuntimeException("Unknown primitive type " + type);
            }
        }
    }
}
