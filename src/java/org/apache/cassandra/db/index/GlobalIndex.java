package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.service.StorageProxy;

public class GlobalIndex
{
    private ColumnDefinition target;
    private Collection<ColumnDefinition> denormalized;

    private String indexName;
    private ColumnFamilyStore baseCfs;
    private ColumnFamilyStore indexCfs;

    public GlobalIndex(String indexName, ColumnDefinition target, Collection<ColumnDefinition> denormalized, ColumnFamilyStore baseCfs)
    {
        this.indexName = indexName;
        this.target = target;
        this.denormalized = denormalized;
        this.baseCfs = baseCfs;

        createIndexCfs();
    }

    private void createIndexCfs()
    {
        assert baseCfs != null && target != null;

        CellNameType indexComparator = SecondaryIndex.getIndexComparator(baseCfs.metadata, target);
        CFMetaData indexedCfMetadata = CFMetaData.newIndexMetadata(baseCfs.metadata, target, indexComparator);
        indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace,
                indexedCfMetadata.cfName,
                new LocalPartitioner(target.type),
                indexedCfMetadata);
    }

    /**
     * Check to see if any value that is part of the index is updated. If so, we possibly need to mutate the index.
     *
     * @param cf Column family to check for indexed values with
     * @return True if any of the indexed or denormalized values are contained in the column family.
     */
    private boolean containsIndex(ColumnFamily cf)
    {
        // If we are denormalizing all of the columns, then any non-empty column family will need to be indexed
        if (denormalized == null && !cf.isEmpty())
            return true;

        for (CellName cellName : cf.getColumnNames())
        {
            AbstractType<?> indexComparator = cf.metadata().getColumnDefinitionComparator(target);
            if (indexComparator.compare(target.name.bytes, cellName.toByteBuffer()) == 0)
                return true;
            for (ColumnDefinition column : denormalized)
            {
                AbstractType<?> denormalizedComparator = cf.metadata().getColumnDefinitionComparator(column);
                if (denormalizedComparator.compare(column.name.bytes, cellName.toByteBuffer()) == 0)
                    return true;
            }
        }
        return false;
    }

    // If the indexed value is updated, and was previously set, then we need to tombstone the old value
    private Collection<Mutation> createTombstones(ByteBuffer key, ColumnFamily cf, List<Row> previousResults)
    {
        boolean modifiesTarget = false;
        AbstractType<?> indexComparator = cf.metadata().getColumnDefinitionComparator(target);
        for (CellName cellName : cf.getColumnNames())
        {
            if (indexComparator.compare(target.name.bytes, cellName.toByteBuffer()) == 0)
            {
                modifiesTarget = true;
                break;
            }
        }

        if (!modifiesTarget)
        {
            Collections.emptyList();
        }

        List<Mutation> tombstones = new ArrayList<>();
        for (Row row: previousResults)
        {
            for (CellName cellName: row.cf.getColumnNames())
            {
                if (indexComparator.compare(target.name.bytes, cellName.toByteBuffer()) == 0)
                {
                    Mutation mutation = new Mutation(cf.metadata().ksName, key);
                    ColumnFamily tombstoneCf = mutation.addOrGet(cf.metadata());
                    tombstoneCf.addTombstone(cellName, 0, cf.maxTimestamp());
                    tombstones.add(mutation);
                }
            }
        }
        return tombstones;
    }

    private Collection<Mutation> createInserts(ByteBuffer key, ColumnFamily cf)
    {
        return Collections.emptyList();
    }

    public Collection<Mutation> createMutations(ByteBuffer key, ColumnFamily cf, ConsistencyLevel consistency)
    {
        if (!containsIndex(cf))
        {
            return null;
        }

        // Need to execute a read first (this is *not* a local read; it should be done at the same consistency as write
        List<Row> results = StorageProxy.read(Lists.<ReadCommand>newArrayList(new SliceFromReadCommand(cf.metadata().ksName, key, cf.metadata().cfName, cf.maxTimestamp(), new IdentityQueryFilter())), consistency);

        Collection<Mutation> mutations = null;
        Collection<Mutation> tombstones = createTombstones(key, cf, results);
        if (tombstones != null && !tombstones.isEmpty())
        {
            if (mutations == null) mutations = new ArrayList<>();
            mutations.addAll(tombstones);
        }

        Collection<Mutation> inserts = createInserts(key, cf);

        if (inserts != null && !inserts.isEmpty())
        {
            if (mutations == null) mutations = new ArrayList<>();
            mutations.addAll(inserts);
        }

        return mutations;
    }
}