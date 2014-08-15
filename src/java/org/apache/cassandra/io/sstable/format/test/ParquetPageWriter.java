package org.apache.cassandra.io.sstable.format.test;


import com.google.common.collect.Lists;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.util.SequentialWriter;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.column.page.*;
import parquet.column.statistics.Statistics;
import parquet.format.*;
import parquet.io.ParquetEncodingException;
import parquet.org.apache.thrift.TException;
import parquet.org.apache.thrift.TSerializer;
import parquet.org.apache.thrift.protocol.TCompactProtocol;
import parquet.schema.PrimitiveType;

import java.io.IOException;
import java.util.*;

public class ParquetPageWriter implements PageWriteStore
{
    private Map<ColumnDescriptor, TestPageWriter> pageWriters = new HashMap<>();
    private List<RowGroup> rowGroups = new ArrayList<>();

    private TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
    private long totalRowCount = 0;


    @Override
    public PageWriter getPageWriter(ColumnDescriptor columnDescriptor)
    {
        TestPageWriter pageWriter = pageWriters.get(columnDescriptor);
        if (pageWriter == null)
        {
            pageWriter = new TestPageWriter();
            pageWriters.put(columnDescriptor, pageWriter);
        }
        return pageWriter;
    }


    Type getType(PrimitiveType.PrimitiveTypeName type) {
        switch (type) {
            case INT64:
                return Type.INT64;
            case INT32:
                return Type.INT32;
            case BOOLEAN:
                return Type.BOOLEAN;
            case BINARY:
                return Type.BYTE_ARRAY;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case INT96:
                return Type.INT96;
            case FIXED_LEN_BYTE_ARRAY:
                return Type.FIXED_LEN_BYTE_ARRAY;
            default:
                throw new RuntimeException("Unknown primitive type " + type);
        }
    }

    public parquet.column.Encoding getEncoding(parquet.format.Encoding encoding)
    {
        return parquet.column.Encoding.valueOf(encoding.name());
    }

    public parquet.format.Encoding getEncoding(parquet.column.Encoding encoding)
    {
        return parquet.format.Encoding.valueOf(encoding.name());
    }

    public void writeFooter(SequentialWriter out, DeletionTime deletionTime) throws IOException
    {
        //We don't write the actual parquet footer since we don't need the
        //schema written for every partition

        assert !rowGroups.isEmpty();

        out.write(ParquetRowGroupReader.START_OF_FOOTER_MAGIC);

        long footerOffset = out.getFilePointer();

        //Write the row level deletion info
        DeletionTime.serializer.serialize(deletionTime, out.stream);


        out.stream.writeInt(rowGroups.size());

        for (RowGroup rg : rowGroups)
        {
            try
            {
                //Fixme: use IOStreamTransport
                byte[] serialized = ser.serialize(rg);
                out.write(serialized);
            } catch (TException e)
            {
                throw new IOException(e);
            }
        }

        long len = out.getFilePointer() - footerOffset;
        out.stream.writeLong(len);
        out.write(ParquetRowGroupReader.END_OF_FOOTER_MAGIC);
    }

    /**
     * Writes the pages buffered in memory to disk, and appends the meta info
     * to the RowGroup list
     *
     * @param out
     * @throws IOException
     */
    public void write(SequentialWriter out, long numRows) throws IOException
    {
        if (pageWriters.isEmpty())
            return;

        //We need to write some preamble so we can support
        //things like streaming.  Most reads will go through
        //the footer

        //Hint how many rows will be in this row group
        out.stream.writeLong(numRows);

        //Hint how many column chunks we will be writing
        out.stream.writeInt(pageWriters.size());

        RowGroup rowGroup = new RowGroup();

        for (Map.Entry<ColumnDescriptor, TestPageWriter> entry : pageWriters.entrySet())
        {
            ColumnDescriptor desc = entry.getKey();

            Set<parquet.format.Encoding> encodings = new HashSet<>(3);
            long numValues = 0;
            long totalUncompressedSize = 0;
            long dataPageOffset = -1L;

            //Write the number of pages in this column chunk
            out.stream.writeInt(entry.getValue().getPages().size());

            //Write each page
            for (Page p : entry.getValue().getPages())
            {
                //First data page offset
                if (dataPageOffset == -1L)
                    dataPageOffset = out.getFilePointer();

                //Write the header
                PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, p.getUncompressedSize(), p.getUncompressedSize());

                pageHeader.setData_page_header(
                        new DataPageHeader(
                                p.getValueCount(),
                                getEncoding(p.getValueEncoding()),
                                getEncoding(p.getDlEncoding()),
                                getEncoding(p.getRlEncoding())));

                //Info needed for column chunk meta
                encodings.add(pageHeader.data_page_header.encoding);
                encodings.add(pageHeader.data_page_header.definition_level_encoding);
                encodings.add(pageHeader.data_page_header.repetition_level_encoding);
                numValues += p.getValueCount();
                totalUncompressedSize += p.getUncompressedSize();


                try
                {
                    out.write(ser.serialize(pageHeader));
                } catch (TException e)
                {
                    throw new IOException(e);
                }


                //Write encoded page
                p.getBytes().writeAllTo(out);
            }

            ColumnMetaData cm = new ColumnMetaData(getType(desc.getType()), Lists.newArrayList(encodings), Arrays.asList(desc.getPath()),
                    CompressionCodec.UNCOMPRESSED, numValues, totalUncompressedSize, totalUncompressedSize, dataPageOffset);

            //Write the column metadata
            try
            {
                out.write(ser.serialize(cm));
            }
            catch (TException e)
            {
                throw new IOException(e);
            }

            ColumnChunk chunk = new ColumnChunk(out.getFilePointer());
            chunk.setMeta_data(cm);


            rowGroup.addToColumns(chunk);
        }

        rowGroup.setNum_rows(numRows);

        //Clear for next row group
        pageWriters.clear();

        //append to rowgroup list
        rowGroups.add(rowGroup);
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
            throw new UnsupportedOperationException();
        }

        @Override
        public void writePage(BytesInput bytesInput, int valueCount, Statistics statistics, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) throws IOException
        {
            if (valueCount == 0)
            {
                throw new ParquetEncodingException("illegal page of 0 values");
            }
            memSize += bytesInput.size();

            pages.add(new Page(BytesInput.copy(bytesInput), valueCount, (int) bytesInput.size(), statistics, rlEncoding, dlEncoding, valuesEncoding));
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
            if (this.dictionaryPage != null)
            {
                throw new ParquetEncodingException("Only one dictionary page per block");
            }
            this.memSize += dictionaryPage.getBytes().size();
            this.dictionaryPage = dictionaryPage.copy();
        }

        public long getTotalValueCount()
        {
            return totalValueCount;
        }

        public List<Page> getPages()
        {
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

}
