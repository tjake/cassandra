package org.apache.cassandra.db.index.global;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.composites.CellName;

/**
 * Created by carl on 2/4/15.
 */
public class GlobalIndexSelectorOnSet extends GlobalIndexSelector
{
    public GlobalIndexSelectorOnSet(ColumnDefinition cfDef)
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
