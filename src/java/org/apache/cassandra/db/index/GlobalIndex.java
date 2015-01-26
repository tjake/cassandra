package org.apache.cassandra.db.index;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.composites.CellName;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

public class GlobalIndex
{
    private CellName indexOn;
    private Collection<CellName> denormalize;

    public Collection<? extends Mutation> createMutations(ByteBuffer key, ColumnFamily cf)
    {
        return Collections.emptyList();
    }
}
