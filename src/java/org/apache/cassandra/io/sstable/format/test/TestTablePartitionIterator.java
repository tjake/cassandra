package org.apache.cassandra.io.sstable.format.test;


import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;

import java.io.IOException;

public class TestTablePartitionIterator implements OnDiskAtomIterator
{


    @Override
    public ColumnFamily getColumnFamily()
    {
        return null;
    }

    @Override
    public DecoratedKey getKey()
    {
        return null;
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public boolean hasNext()
    {
        return false;
    }

    @Override
    public OnDiskAtom next()
    {
        return null;
    }

    @Override
    public void remove()
    {

    }
}
