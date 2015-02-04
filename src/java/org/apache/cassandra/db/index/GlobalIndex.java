package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CompoundDenseCellNameType;
import org.apache.cassandra.db.index.composites.CompositesIndex;
import org.apache.cassandra.db.index.global.GlobalIndexSelector;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.service.StorageProxy;

public class GlobalIndex
{
    private ColumnDefinition target;
    private GlobalIndexSelector targetSelector;
    private Collection<ColumnDefinition> denormalized;
    private Collection<GlobalIndexSelector> denormalizedSelectors;

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
        createSelectors();
    }

    private void createSelectors()
    {
        targetSelector = GlobalIndexSelector.create(baseCfs, target);
        denormalizedSelectors = new ArrayList<>(denormalized.size());
        for (ColumnDefinition column: denormalized)
        {
            denormalizedSelectors.add(GlobalIndexSelector.create(baseCfs, column));
        }
    }

    private void createIndexCfs()
    {
        assert baseCfs != null;
        assert target != null;

        CFMetaData indexedCfMetadata = CFMetaData.newGlobalIndexMetaData();

        int index = 0;

        indexedCfMetadata.addColumnDefinition(ColumnDefinition.partitionKeyDef(indexedCfMetadata, target.name.bytes, target.type, index++));
        for (ColumnDefinition partitionColumn: baseCfs.metadata.partitionKeyColumns())
            indexedCfMetadata.addColumnDefinition(ColumnDefinition.clusteringKeyDef(indexedCfMetadata, partitionColumn.name.bytes, partitionColumn.type, index++));

        for (ColumnDefinition clusteringColumn: baseCfs.metadata.clusteringColumns())
            indexedCfMetadata.addColumnDefinition(ColumnDefinition.clusteringKeyDef(indexedCfMetadata, clusteringColumn.name.bytes, clusteringColumn.type, index++));

        for (ColumnDefinition regularColumn: baseCfs.metadata.regularColumns())
            indexedCfMetadata.addColumnDefinition(ColumnDefinition.regularDef(indexedCfMetadata, regularColumn.name.bytes, regularColumn.type, index++));

        for (ColumnDefinition staticColumn: baseCfs.metadata.staticColumns())
            indexedCfMetadata.addColumnDefinition(ColumnDefinition.staticDef(indexedCfMetadata, staticColumn.name.bytes, staticColumn.type, index++));

        CellNameType indexComparator = CompositesIndex.getIndexComparator(baseCfs.metadata, target);

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
    private boolean modifiesIndexedColumn(ColumnFamily cf)
    {
        // If we are denormalizing all of the columns, then any non-empty column family will need to be indexed
        if (denormalized.isEmpty() && !cf.isEmpty())
            return true;

        for (CellName cellName : cf.getColumnNames())
        {
            if (targetSelector.selects(cellName))
                return true;
            for (GlobalIndexSelector column: denormalizedSelectors)
            {
                if (column.selects(cellName))
                    return true;
            }
        }
        return false;
    }

    private boolean modifiesTarget(ColumnFamily cf)
    {
        for (CellName cellName: cf.getColumnNames())
        {
            if (targetSelector.selects(cellName))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * @param key Partition key which is being modified
     * @param cf Column family being modified
     */
    private Collection<Mutation> createTombstones(ByteBuffer key, ColumnFamily cf, List<Row> previousResults)
    {
        if (!targetSelector.canGenerateTombstones())
            return Collections.emptyList();

        // If there are no previous results, then throw an exception
        if (previousResults.isEmpty())
            return Collections.emptyList();

        if (!modifiesTarget(cf))
            return Collections.emptyList();

        List<Mutation> tombstones = new ArrayList<>();
        for (Row row: previousResults)
        {
            ColumnFamily rowCf = row.cf;
            for (CellName cellName: rowCf.getColumnNames())
            {
                if (targetSelector.selects(cellName))
                {
                    ByteBuffer oldPartitionKey = rowCf.getColumn(cellName).value();
                    ByteBuffer oldColumn = key;
                    Mutation mutation = new Mutation(cf.metadata().ksName, oldPartitionKey);
                    ColumnFamily tombstoneCf = mutation.addOrGet(indexCfs.metadata);
                    ColumnIdentifier partition = new ColumnIdentifier("partition", false);
                    CellNameType type = new CompoundDenseCellNameType(indexCfs.metadata.getColumnDefinition(partition).type.getComponents());
                    tombstoneCf.addTombstone(type.makeCellName(oldColumn), 0, cf.maxTimestamp());
                    tombstones.add(mutation);
                }
            }
        }
        return tombstones;
    }

    private Collection<Mutation> createInserts(ByteBuffer key, ColumnFamily cf)
    {
        // The transformation is:
        // Indexed Data Column -> Index Partition Key
        // Partition Key -> Index Cluster Column
        // Denormalized Columns -> Value

        return Collections.emptyList();
    }

    public Collection<Mutation> createMutations(ByteBuffer key, ColumnFamily cf, ConsistencyLevel consistency)
    {
        if (!modifiesIndexedColumn(cf))
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