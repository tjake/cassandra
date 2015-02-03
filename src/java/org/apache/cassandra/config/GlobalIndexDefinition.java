package org.apache.cassandra.config;

import java.util.Collection;

import org.apache.cassandra.cql3.ColumnIdentifier;

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
}
