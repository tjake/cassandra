package org.apache.cassandra.io.sstable.format.test;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileDataInput;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.DictionaryPage;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.format.ColumnChunk;
import parquet.format.ColumnMetaData;
import parquet.format.PageHeader;
import parquet.format.RowGroup;
import parquet.io.ParquetDecodingException;
import parquet.org.apache.thrift.TException;
import parquet.org.apache.thrift.protocol.TCompactProtocol;
import parquet.org.apache.thrift.protocol.TProtocol;
import parquet.org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class ParquetRowGroupReader implements Iterable<PageReadStore>
{
    private List<PageReadStore> rowGroups = new ArrayList<>();

    public static final byte[] START_OF_FOOTER_MAGIC = "PAR1STAR".getBytes(Charset.forName("ASCII"));

    public static final byte[] END_OF_FOOTER_MAGIC = "PAR1".getBytes(Charset.forName("ASCII"));

    private final Version version;
    private final FileDataInput input;
    private long totalRowCount = 0;
    private final DeletionTime deletionTime;

    public ParquetRowGroupReader(Version version, FileDataInput input)
    {
        assert version != null && input != null;
        this.version = version;
        this.input = input;
        deletionTime = readFooter();
    }


    private DeletionTime readFooter()
    {
        assert rowGroups.isEmpty(); //only call once

        DeletionTime dtime = null;

        try
        {
            int numRowGroups = input.readInt();
            assert numRowGroups > 0;

            TProtocol protocol = new TCompactProtocol(new TDataInputTransport(input));
            for (int i = 0; i < numRowGroups; i++)
            {
                RowGroup rg = new RowGroup();
                rg.read(protocol);

                rowGroups.add(new TestPageStoreReader(rg));
                totalRowCount += rg.getNum_rows();
            }
        }
        catch (IOException e)
        {
            throw new ParquetDecodingException(e);
        }
        catch (TException e)
        {
            throw new ParquetDecodingException(e);
        }

        return dtime;
    }

    public DeletionTime getDeletionTime()
    {
        return deletionTime;
    }

    //Rows across all row groups
    public long getTotalRowCount()
    {
        return totalRowCount;
    }

    public long getNumRowGroups()
    {
        return rowGroups.size();
    }

    @Override
    public Iterator<PageReadStore> iterator()
    {
        return rowGroups.iterator();
    }


    class TestPageStoreReader implements PageReadStore
    {
        final RowGroup rg;

        TestPageStoreReader(RowGroup rg)
        {
            this.rg = rg;
        }

        @Override
        public PageReader getPageReader(ColumnDescriptor descriptor)
        {
            for (ColumnChunk chunk : rg.columns)
            {
                //TODO: avoid iterative search
                if (Arrays.deepEquals(descriptor.getPath(),chunk.getMeta_data().getPath_in_schema().toArray()))
                {
                    try
                    {
                        input.seek(chunk.getMeta_data().getData_page_offset());
                    }
                    catch (IOException e)
                    {
                        throw new ParquetDecodingException(e);
                    }

                    return new TestPageReader(descriptor, chunk.getMeta_data());
                }
            }

            return null;
        }

        @Override
        public long getRowCount()
        {
            return rg.getNum_rows();
        }
    }

    public parquet.column.Encoding getEncoding(parquet.format.Encoding encoding) {
        return parquet.column.Encoding.valueOf(encoding.name());
    }

    class TestPageReader implements PageReader
    {
        final ColumnDescriptor columnDescriptor;
        final ColumnMetaData columnMetaData;

        final long valueCount;
        long valuesRead;

        public TestPageReader(ColumnDescriptor columnDescriptor, ColumnMetaData columnMetaData)
        {
            this.columnDescriptor = columnDescriptor;
            this.columnMetaData = columnMetaData;

            valueCount = columnMetaData.getNum_values();
            valuesRead = 0;
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

            if (valuesRead >= valueCount)
                throw new ParquetDecodingException("No more pages to read");

            try
            {
                //Deserialize page header
                PageHeader header = new PageHeader();

                TTransport in = new TDataInputTransport(input);
                TCompactProtocol protocol = new TCompactProtocol(in);
                header.read(protocol);

                BytesInput bytes = new ByteBufferBytesInput(input.readBytes(header.getUncompressed_page_size()));

                Page p = new Page(bytes, header.data_page_header.num_values, header.uncompressed_page_size, null,
                        getEncoding(header.data_page_header.repetition_level_encoding),
                        getEncoding(header.data_page_header.definition_level_encoding),
                        getEncoding(header.data_page_header.encoding));

                valuesRead += p.getValueCount();

                return p;
            }
            catch (TException | IOException e)
            {
                throw new ParquetDecodingException(e);
            }
        }
    }


}
