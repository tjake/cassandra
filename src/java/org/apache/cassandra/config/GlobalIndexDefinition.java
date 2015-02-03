package org.apache.cassandra.config;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.index.GlobalIndex;

public class GlobalIndexDefinition
{
    public String indexName;
    public ColumnIdentifier target;
    public Collection<ColumnIdentifier> denormalized;

    public GlobalIndexDefinition(String indexName, ColumnIdentifier target, Collection<ColumnIdentifier> denormalized)
    {
        this.indexName = indexName;
        this.target = target;
        this.denormalized = denormalized;
    }

    public GlobalIndex resolve(CFMetaData cfm)
    {
        ColumnDefinition targetCd = cfm.getColumnDefinition(target);
        Collection<ColumnDefinition> denormalizedCds = new ArrayList<>();
        for (ColumnIdentifier cd: denormalized)
            denormalizedCds.add(cfm.getColumnDefinition(cd));
        return new GlobalIndex(indexName, targetCd, denormalizedCds, Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfName));
    }
}
