package org.apache.cassandra.io.sstable.format.test;


import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.format.Version;
import parquet.column.ColumnDescriptor;
import parquet.column.page.Page;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestTablePartitionIterator implements Iterator<OnDiskAtom>
{
    private final DataInput in;
    private final ColumnSerializer.Flag flag;
    private final int expireBefore;
    private final CFMetaData cfm;
    private final Version version;

    public TestTablePartitionIterator(DataInput in, ColumnSerializer.Flag flag, int expireBefore, CFMetaData cfm, Version version)
    {
        this.in = in;
        this.flag = flag;
        this.expireBefore = expireBefore;
        this.cfm = cfm;
        this.version = version;

     }


    private void maybeLoadRowGroup()
    {

    }

    @Override
    public boolean hasNext()
    {


    }

    @Override
    public OnDiskAtom next()
    {
        if (!hasNext())
            return null;


        return null;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
