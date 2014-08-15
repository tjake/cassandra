package org.apache.cassandra.io.sstable.format.test;

import org.apache.cassandra.io.util.FileDataInput;
import parquet.org.apache.thrift.transport.TTransport;
import parquet.org.apache.thrift.transport.TTransportException;

import java.io.DataInput;
import java.io.IOException;


public class TDataInputTransport extends TTransport
{

    final DataInput fin;

    public TDataInputTransport(DataInput fin)
    {
        this.fin = fin;
    }

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void open() throws TTransportException
    {

    }

    @Override
    public void close()
    {

    }

    @Override
    public int read(byte[] bytes, int off, int len) throws TTransportException
    {
        try
        {
            fin.readFully(bytes, off, len);
            return len;
        } catch (IOException e)
        {
            throw new TTransportException(e);
        }
    }

    @Override
    public void write(byte[] bytes, int i, int i2) throws TTransportException
    {
        throw new UnsupportedOperationException();
    }
}
