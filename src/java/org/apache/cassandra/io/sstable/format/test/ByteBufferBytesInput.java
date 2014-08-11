package org.apache.cassandra.io.sstable.format.test;

import org.apache.cassandra.utils.ByteBufferUtil;
import parquet.bytes.BytesInput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;


public class ByteBufferBytesInput extends BytesInput
{
    final ByteBuffer buf;

    public ByteBufferBytesInput(ByteBuffer buf)
    {
        this.buf = buf;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size()
    {
        return buf.remaining();
    }


    @Override
    public byte[] toByteArray() throws IOException
    {
        return ByteBufferUtil.getArray(buf);
    }
}
