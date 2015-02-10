package org.apache.cassandra.db.index.global;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;

public class GlobalIndexSelectorOnRegularColumn extends GlobalIndexSelector
{
    private final ColumnFamilyStore baseCfs;

    public GlobalIndexSelectorOnRegularColumn(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition)
    {
        super(columnDefinition);
        this.baseCfs = baseCfs;
    }

    public boolean canGenerateTombstones()
    {
        return true;
    }

    public boolean selects(CellName name)
    {
        AbstractType<?> comp = baseCfs.metadata.getColumnDefinitionComparator(columnDefinition);
        return name.size() > columnDefinition.position()
                && comp.compare(name.get(columnDefinition.position()), columnDefinition.name.bytes) == 0;
    }

    public ByteBuffer value(CellName cellName, ByteBuffer key, ColumnFamily cf) {
        return cf.getColumn(cellName).value();
    }

}
