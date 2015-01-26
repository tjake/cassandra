package org.apache.cassandra.config;

import java.util.Collection;

public class GlobalIndexDefinition
{
    public String indexedColumn;
    public Collection<String> denormalizedColumns;
}
