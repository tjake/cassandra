package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;

import java.util.Set;


public class TableReader extends SSTable
{
    protected TableReader(Descriptor descriptor, CFMetaData metadata, IPartitioner partitioner)
    {
        super(descriptor, metadata, partitioner);
    }

    protected TableReader(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner)
    {
        super(descriptor, components, metadata, partitioner);
    }
}
