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
    ColumnDefinition columnDef;

    public GlobalIndexSelectorOnPartitionKey(ColumnFamilyStore baseCfs, ColumnDefinition cfDef)
    {
        this.baseCfs = baseCfs;
        this.columnDef = cfDef;
    }

    public boolean canGenerateTombstones()
    {
        return false;
    }

    public boolean selects(CellName cellName)
    {
        return true;
    }

    @Override
    public ByteBuffer value(CellName cellName, ByteBuffer key, ColumnFamily cf) {
        if (columnDef.isOnAllComponents())
            return key;
        CompositeType keyComparator = (CompositeType)baseCfs.metadata.getKeyValidator();
        ByteBuffer[] components = keyComparator.split(key);
        return components[columnDef.position()];
    }
}
