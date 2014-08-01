package org.apache.cassandra.io.sstable.format.test;


import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.commons.lang.StringUtils;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.column.UnknownColumnException;
import parquet.column.page.*;
import parquet.column.statistics.Statistics;
import parquet.format.DataPageHeader;
import parquet.format.PageHeader;
import parquet.format.PageType;
import parquet.format.Util;
import parquet.io.ParquetDecodingException;
import parquet.io.ParquetEncodingException;
import parquet.org.apache.thrift.TException;
import parquet.org.apache.thrift.TSerializer;
import parquet.org.apache.thrift.protocol.TCompactProtocol;
import parquet.schema.MessageType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class TestPageStore implements PageWriteStore, PageReadStore
{

    private Map<ColumnDescriptor, TestPageWriter> pageWriters = new HashMap<>();
    private TSerializer ser = new TSerializer(new TCompactProtocol.Factory());

    private final Version version;
    private final DataInput input;

    public TestPageStore(Version version, DataInput input)
    {
        assert version != null && input != null;
        this.version = version;
        this.input = input;
    }

    public TestPageStore()
    {
        this.version = null;
        this.input = null;
    }

    @Override
    public PageReader getPageReader(ColumnDescriptor columnDescriptor)
    {
        if (input == null)
        {
            TestPageWriter pageWriter = pageWriters.get(columnDescriptor);
            if (pageWriter == null)
            {
                throw new UnknownColumnException(columnDescriptor);
            }
            List<Page> pages = new ArrayList<>(pageWriter.getPages());
            return new TestPageReader(pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage());
        }

        return new TestPageReader2(columnDescriptor);
    }

    @Override
    public long getRowCount()
    {
        return 0;
    }

    @Override
    public PageWriter getPageWriter(ColumnDescriptor columnDescriptor)
    {
        TestPageWriter pageWriter = pageWriters.get(columnDescriptor);
        if (pageWriter == null) {
            pageWriter = new TestPageWriter();
            pageWriters.put(columnDescriptor, pageWriter);
        }
        return pageWriter;
    }

    public parquet.column.Encoding getEncoding(parquet.format.Encoding encoding) {
        return parquet.column.Encoding.valueOf(encoding.name());
    }

    public parquet.format.Encoding getEncoding(parquet.column.Encoding encoding) {
        return parquet.format.Encoding.valueOf(encoding.name());
    }



    public void write(DataOutputPlus out) throws IOException
    {
        //Write the number of columns in this row group
        out.writeInt(pageWriters.size());

        for (Map.Entry<ColumnDescriptor,TestPageWriter> entry : pageWriters.entrySet())
        {
            //Write out the column def and pages
            out.writeUTF(StringUtils.join(entry.getKey().getPath(), ","));
            out.writeInt(entry.getValue().getPages().size());

            //Write each page
            for (Page p : entry.getValue().getPages())
            {
                //Write the header
                PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, p.getUncompressedSize(), p.getUncompressedSize());

                pageHeader.data_page_header = new DataPageHeader(
                        p.getValueCount(),
                        getEncoding(p.getValueEncoding()),
                        getEncoding(p.getDlEncoding()),
                        getEncoding(p.getRlEncoding()));

                try
                {
                    out.write(ser.serialize(pageHeader));
                }
                catch (TException e)
                {
                    throw new IOException(e);
                }

                //Write encoded page
                out.write(p.getBytes().toByteArray());
            }
        }

        //Clear for next row group
        pageWriters.clear();
    }

    class TestPageWriter implements PageWriter
    {

        private final List<Page> pages = new ArrayList<>();
        private DictionaryPage dictionaryPage;
        private long memSize = 0;
        private long totalValueCount = 0;

        @Override
        public void writePage(BytesInput bytesInput, int valueCount, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) throws IOException
        {
            if (valueCount == 0) {
                throw new ParquetEncodingException("illegal page of 0 values");
            }
            memSize += bytesInput.size();
            pages.add(new Page(BytesInput.copy(bytesInput), valueCount, (int)bytesInput.size(), rlEncoding, dlEncoding, valuesEncoding));
            totalValueCount += valueCount;
        }

        @Override
        public void writePage(BytesInput bytesInput, int valueCount, Statistics statistics, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) throws IOException
        {
            if (valueCount == 0) {
                throw new ParquetEncodingException("illegal page of 0 values");
            }
            memSize += bytesInput.size();
            pages.add(new Page(BytesInput.copy(bytesInput), valueCount, (int)bytesInput.size(), statistics, rlEncoding, dlEncoding, valuesEncoding));
            totalValueCount += valueCount;
        }

        @Override
        public long getMemSize()
        {
            return memSize;
        }

        @Override
        public long allocatedSize()
        {
            return memSize;
        }

        @Override
        public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException
        {
            if (this.dictionaryPage != null) {
                throw new ParquetEncodingException("Only one dictionary page per block");
            }
            this.memSize += dictionaryPage.getBytes().size();
            this.dictionaryPage = dictionaryPage.copy();
        }

        public long getTotalValueCount() {
            return totalValueCount;
        }

        public List<Page> getPages() {
            return pages;
        }

        public DictionaryPage getDictionaryPage()
        {
            return dictionaryPage;
        }

        @Override
        public String memUsageString(String prefix)
        {
            return String.format("%s %,d bytes", prefix, memSize);
        }
    }

    class TestPageReader2 implements PageReader
    {
        final ColumnDescriptor columnDescriptor;

        public TestPageReader2(ColumnDescriptor columnDescriptor)
        {
            this.columnDescriptor = columnDescriptor;
        }

        @Override
        public DictionaryPage readDictionaryPage()
        {
            return null;
        }

        @Override
        public long getTotalValueCount()
        {
            return 0;
        }

        @Override
        public Page readPage()
        {
            try
            {
                int columns = input.readInt();

                for (int i = 0; i < columns; i++) {

                    String[] path = input.readUTF().split(",");
                    //if (!Arrays.deepEquals(path, columnDescriptor.getPath()))
                     return null;


                }

            }
            catch (IOException e)
            {
                throw new ParquetDecodingException(e);
            }

            return null;
        }
    }

    class TestPageReader implements PageReader
    {

        private final long totalValueCount;
        private final Iterator<Page> pages;
        private final DictionaryPage dictionaryPage;

        public TestPageReader(long totalValueCount, Iterator<Page> pages, DictionaryPage dictionaryPage) {
            super();
            this.totalValueCount = totalValueCount;
            this.pages = pages;
            this.dictionaryPage = dictionaryPage;
        }

        @Override
        public long getTotalValueCount() {
            return totalValueCount;
        }

        @Override
        public Page readPage() {
            if (pages.hasNext()) {
                Page next = pages.next();

                return next;
            } else {
                throw new ParquetDecodingException("after last page");
            }
        }

        @Override
        public DictionaryPage readDictionaryPage() {
            return dictionaryPage;
        }
    }
}
