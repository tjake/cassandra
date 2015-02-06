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
    private final ColumnDefinition columnDef;

    public GlobalIndexSelectorOnRegularColumn(ColumnFamilyStore baseCfs, ColumnDefinition columnDef)
    {
        this.baseCfs = baseCfs;
        this.columnDef = columnDef;
    }

    public boolean canGenerateTombstones()
    {
        return true;
    }

    public boolean selects(CellName name)
    {
        AbstractType<?> comp = baseCfs.metadata.getColumnDefinitionComparator(columnDef);
        return name.size() > columnDef.position()
                && comp.compare(name.get(columnDef.position()), columnDef.name.bytes) == 0;
    }

    public ByteBuffer value(CellName cellName, ByteBuffer key, ColumnFamily cf) {
        return cf.getColumn(cellName).value();
    }

}
