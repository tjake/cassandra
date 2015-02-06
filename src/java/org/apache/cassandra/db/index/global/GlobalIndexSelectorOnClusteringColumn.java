package org.apache.cassandra.db.index.global;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.CellName;

import java.nio.ByteBuffer;

public class GlobalIndexSelectorOnClusteringColumn extends GlobalIndexSelector
{
    private ColumnDefinition columnDefinition;

    public GlobalIndexSelectorOnClusteringColumn(ColumnDefinition columnDefinition)
    {
        this.columnDefinition = columnDefinition;
    }

    public boolean canGenerateTombstones()
    {
        return false;
    }

    public boolean selects(CellName cellName)
    {
        return true;
    }

    public ByteBuffer value(CellName cellName, ByteBuffer key, ColumnFamily cf) {
        return cellName.get(columnDefinition.position());
    }

}
