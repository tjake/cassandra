package org.apache.cassandra.db.index.global;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.composites.CellName;

public class GlobalIndexSelectorOnMap extends GlobalIndexSelector
{
    public GlobalIndexSelectorOnMap(ColumnDefinition cfDef)
    {
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
