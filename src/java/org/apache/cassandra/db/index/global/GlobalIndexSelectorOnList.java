package org.apache.cassandra.db.index.global;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.composites.CellName;

public class GlobalIndexSelectorOnList extends GlobalIndexSelector
{
    private ColumnDefinition definition;

    public GlobalIndexSelectorOnList(ColumnDefinition definition)
    {
        this.definition = definition;
    }

    @Override
    public boolean selects(CellName cellName)
    {
        return false;
    }

    @Override
    public CellName cellName(CellName cellName)
    {
        return null;
    }
}
