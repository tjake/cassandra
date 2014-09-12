package org.apache.cassandra.io.sstable.format.test;

import com.google.common.base.Joiner;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.format.test.convert.ByteBufferBytesInput;
import org.apache.cassandra.io.sstable.format.test.convert.TDataInputTransport;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;
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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;


public class ParquetRowGroupReader implements DeletionTimeAwarePageReader
{
    private List<RowGroup> rowGroups = new ArrayList<>();
    private final Iterator<RowGroup> rowGroupIterator;

    public static final byte[] START_OF_FOOTER_MAGIC = "PAR1STAR".getBytes(Charset.forName("ASCII"));
    public static final byte[] END_OF_FOOTER_MAGIC = "PAR1".getBytes(Charset.forName("ASCII"));

    private final FileDataInput input;
    private long totalRowCount = 0;
    private final DeletionTime deletionTime;

    public ParquetRowGroupReader(FileDataInput input)
    {
        assert input != null;

        this.input = input;
        deletionTime = readFooter();
        rowGroupIterator = rowGroups.iterator();
    }


    private DeletionTime readFooter()
    {
        assert rowGroups.isEmpty(); //only call once

        DeletionTime dtime = null;

        try
        {
            int numRowGroups = input.readInt();
            if (numRowGroups > 0)
            {
                TProtocol protocol = new TCompactProtocol(new TDataInputTransport(input));
                for (int i = 0; i < numRowGroups; i++)
                {
                    RowGroup rg = new RowGroup();
                    rg.read(protocol);

                    rowGroups.add(rg);
                    totalRowCount += rg.getNum_rows();
                }
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
    public boolean hasNext()
    {
        return rowGroupIterator.hasNext();
    }

    @Override
    public PageReadStore next()
    {
        assert hasNext();
        return new TestPageStoreReader(rowGroupIterator.next(), false);
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    class TestPageStoreReader implements PageReadStore
    {
        final RowGroup rg;
        Map<String, TestPageReader> pathLookup;

        TestPageStoreReader(RowGroup rg, boolean lazy)
        {
            this.rg = rg;
            pathLookup = new HashMap<>(rg.getColumnsSize());

            for (ColumnChunk cm : rg.getColumns())
            {
                pathLookup.put(Joiner.on(",").join(cm.getMeta_data().getPath_in_schema()), new TestPageReader(cm.getMeta_data(), lazy));
            }
        }

        @Override
        public PageReader getPageReader(ColumnDescriptor descriptor)
        {
            TestPageReader pageReader = pathLookup.remove(Joiner.on(",").join(descriptor.getPath()));

            assert pageReader != null;

            return  pageReader;
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
        final ColumnMetaData columnMetaData;

        final long valueCount;
        long valuesRead;
        long lastOffset;

        final boolean lazy;
        final List<Page> pages;
        final Iterator<Page> pageIterator;
        final TCompactProtocol protocol;

        public TestPageReader(ColumnMetaData columnMetaData, boolean lazy)
        {
            this.columnMetaData = columnMetaData;
            this.lazy = lazy;
            this.protocol = new TCompactProtocol(new TDataInputTransport(input));

            valueCount = columnMetaData.getNum_values();
            valuesRead = 0;
            lastOffset = columnMetaData.getData_page_offset();

            if (lazy)
            {
                pages = null;
                pageIterator = null;
            }
            else
            {
                pages = loadAllPages();
                pageIterator = pages.iterator();
            }
        }

        @Override
        public DictionaryPage readDictionaryPage()
        {
            return null;
        }

        @Override
        public long getTotalValueCount()
        {
            return valueCount;
        }


        private List<Page> loadAllPages()
        {
            List<Page> pageList = new ArrayList<>();
            while (valuesRead < valueCount)
            {
               pageList.add(readPageInternal());
            }

            return pageList;
        }

        private Page readPageInternal()
        {
            if (valuesRead >= valueCount)
                throw new ParquetDecodingException("No more pages to read");

            try
            {
                //Seek back to where we left off if needed
                if (lastOffset != input.getFilePointer())
                    input.seek(lastOffset);

                //Deserialize page header
                PageHeader header = new PageHeader();
                header.read(protocol);

                int size = header.getUncompressed_page_size();
                int compressedSize = header.getCompressed_page_size();

                BytesInput bytes;

                if (compressedSize != size)
                {
                    bytes = BytesInput.from(ParquetPageWriter.decompressor.decompress(ByteBufferUtil.getArray(input.readBytes(compressedSize)), size));
                }
                else
                {
                    bytes = new ByteBufferBytesInput(input.readBytes(size));
                }

                Page p = new Page(bytes, header.data_page_header.num_values, size, null,
                        getEncoding(header.data_page_header.repetition_level_encoding),
                        getEncoding(header.data_page_header.definition_level_encoding),
                        getEncoding(header.data_page_header.encoding));

                valuesRead += p.getValueCount();

                lastOffset = input.getFilePointer();

                return p;
            }
            catch (TException | IOException e)
            {
                throw new ParquetDecodingException(e);
            }

        }

        @Override
        public Page readPage()
        {
            if (!lazy)
                return pageIterator.next();

            return readPageInternal();
        }
    }


}
