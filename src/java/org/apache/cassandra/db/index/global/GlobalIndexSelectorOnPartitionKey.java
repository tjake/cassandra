package org.apache.cassandra.db.index.global;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.composites.CellName;

public class GlobalIndexSelectorOnPartitionKey extends GlobalIndexSelector
{
    public GlobalIndexSelectorOnPartitionKey(ColumnDefinition cfDef)
    {
    }

    public boolean canGenerateTombstones()
    {
        return false;
    }

    public boolean selects(CellName cellName)
    {
        return false;
    }

}
