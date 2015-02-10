package org.apache.cassandra.db.index.global;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.CompositeType;

import java.nio.ByteBuffer;

public class GlobalIndexSelectorOnPartitionKey extends GlobalIndexSelector
{
    ColumnFamilyStore baseCfs;

    public GlobalIndexSelectorOnPartitionKey(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition)
    {
        super(columnDefinition);
        this.baseCfs = baseCfs;
    }

    public boolean canGenerateTombstones()
    {
        return false;
    }

    public boolean selects(CellName cellName)
    {
        return false;
    }

    public ByteBuffer value(CellName cellName, ByteBuffer key, ColumnFamily cf) {
        throw new AssertionError("PartitionKey cannot produce value from a CellName");
    }

    public boolean isPrimaryKey()
    {
        return true;
    }

    public ByteBuffer value(ByteBuffer key)
    {
        if (columnDefinition.isOnAllComponents())
            return key;

        CompositeType keyComparator = (CompositeType)baseCfs.metadata.getKeyValidator();
        ByteBuffer[] components = keyComparator.split(key);
        return components[columnDefinition.position()];
    }
}
