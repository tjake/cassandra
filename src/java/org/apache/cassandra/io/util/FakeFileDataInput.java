package org.apache.cassandra.io.util;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by jake on 8/11/14.
 */
public class FakeFileDataInput implements FileDataInput
{

    private final DataInput in;

    public FakeFileDataInput(DataInput in)
    {
        this.in = in;
    }

    @Override
    public String getPath()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEOF() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long bytesRemaining() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long pos) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileMark mark()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset(FileMark mark) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long bytesPastMark(FileMark mark)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFilePointer()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer readBytes(int length) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        in.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        in.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        return in.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        return in.readBoolean();
    }

    @Override
    public byte readByte() throws IOException
    {
        return in.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        return in.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException
    {
        return in.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        return in.readUnsignedByte();
    }

    @Override
    public char readChar() throws IOException
    {
        return in.readChar();
    }

    @Override
    public int readInt() throws IOException
    {
        return in.readInt();
    }

    @Override
    public long readLong() throws IOException
    {
        return in.readLong();
    }

    @Override
    public float readFloat() throws IOException
    {
        return in.readFloat();
    }

    @Override
    public double readDouble() throws IOException
    {
        return in.readDouble();
    }

    @Override
    public String readLine() throws IOException
    {
        return in.readLine();
    }

    @Override
    public String readUTF() throws IOException
    {
        return in.readUTF();
    }
}
