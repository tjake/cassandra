package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.index.composites.CompositesIndex;
import org.apache.cassandra.db.index.global.GlobalIndexSelector;
import org.apache.cassandra.service.StorageProxy;

public class GlobalIndex
{
    private ColumnDefinition target;
    private Collection<ColumnDefinition> denormalized;

    private GlobalIndexSelector targetSelector;
    private List<GlobalIndexSelector> clusteringSelectors;
    private List<GlobalIndexSelector> regularSelectors;
    private List<GlobalIndexSelector> staticSelectors;

    private String indexName;
    private ColumnFamilyStore baseCfs;
    private ColumnFamilyStore indexCfs;

    public GlobalIndex(String indexName, ColumnDefinition target, Collection<ColumnDefinition> denormalized, ColumnFamilyStore baseCfs)
    {
        this.indexName = indexName;
        this.target = target;
        this.denormalized = denormalized;
        this.baseCfs = baseCfs;

        clusteringSelectors = new ArrayList<>();
        regularSelectors = new ArrayList<>();
        staticSelectors = new ArrayList<>();

        createIndexCfsAndSelectors();
    }

    private void createIndexCfsAndSelectors()
    {
        assert baseCfs != null;
        assert target != null;

        CellNameType indexComparator = CompositesIndex.getIndexComparator(baseCfs.metadata, target);
        CFMetaData indexedCfMetadata = CFMetaData.newGlobalIndexMetadata(baseCfs.metadata, target, indexComparator);

        indexedCfMetadata.addColumnDefinition(ColumnDefinition.partitionKeyDef(indexedCfMetadata, target.name.bytes, target.type, target.position()));
        targetSelector = GlobalIndexSelector.create(baseCfs, target);

        // All partition and clustering columns are included in the index, whether they are specified in the denormalized columns or not
        for (ColumnDefinition column: baseCfs.metadata.partitionKeyColumns())
        {
            if (column != target)
            {
                Integer position = null;
                if (!column.isOnAllComponents())
                    position = column.position();
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.clusteringKeyDef(indexedCfMetadata, column.name.bytes, column.type, position));
                clusteringSelectors.add(GlobalIndexSelector.create(baseCfs, column));
            }
        }

        for (ColumnDefinition column: baseCfs.metadata.clusteringColumns())
        {
            if (column != target)
            {
                Integer position = null;
                if (!column.isOnAllComponents())
                    position = column.position();
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.clusteringKeyDef(indexedCfMetadata, column.name.bytes, column.type, position));
                clusteringSelectors.add(GlobalIndexSelector.create(baseCfs, column));
            }
        }

        for (ColumnDefinition column: baseCfs.metadata.regularColumns())
        {
            if (column != target && denormalized.contains(column))
            {
                Integer position = null;
                if (!column.isOnAllComponents())
                    position = column.position();
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.regularDef(indexedCfMetadata, column.name.bytes, column.type, position));
                regularSelectors.add(GlobalIndexSelector.create(baseCfs, column));
            }
        }

        for (ColumnDefinition column: baseCfs.metadata.staticColumns())
        {
            if (column != target && denormalized.contains(column))
            {
                Integer position = null;
                if (!column.isOnAllComponents())
                    position = column.position();
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.staticDef(indexedCfMetadata, column.name.bytes, column.type, position));
                staticSelectors.add(GlobalIndexSelector.create(baseCfs, column));
            }
        }

        indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace,
                                                             indexedCfMetadata.cfName,
                                                             DatabaseDescriptor.getPartitioner(),
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
            for (GlobalIndexSelector column: Iterables.concat(clusteringSelectors, regularSelectors, staticSelectors))
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
     * @param cf Column family being modified
     * @param previousResults Current values that are stored for the specified partition key
     */
    private Collection<Mutation> createTombstones(ByteBuffer key, ColumnFamily cf, List<Row> previousResults)
    {
        if (!targetSelector.canGenerateTombstones())
            return Collections.emptyList();

        if (previousResults.isEmpty())
            return Collections.emptyList();

        if (!modifiesTarget(cf))
            return Collections.emptyList();

        List<Mutation> tombstones = new ArrayList<>();
        for (Row row: previousResults)
        {
            ColumnFamily rowCf = row.cf;
            GlobalIndexSelector.Holder holder = new GlobalIndexSelector.Holder(targetSelector, clusteringSelectors, regularSelectors, staticSelectors);
            for (CellName cellName: rowCf.getColumnNames())
            {
                holder.update(cellName, key, rowCf);
            }

            Mutation mutation = holder.getTombstoneMutation(indexCfs, rowCf.maxTimestamp());
            if (mutation != null)
                tombstones.add(mutation);
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