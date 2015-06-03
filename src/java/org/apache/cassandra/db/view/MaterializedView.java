/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.view;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.MaterializedViewDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.CFRowAdder;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.CompoundSparseCellNameType;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MaterializedView
{
    private static class RowAdder extends CFRowAdder
    {
        RowAdder(ColumnFamily cf, Composite prefix, long timestamp, int ttl)
        {
            super(cf, prefix, timestamp, ttl, true);
        }
    }

    private final MaterializedViewDefinition definition;
    public final String name;

    private MaterializedViewSelector targetSelector;
    private final List<MaterializedViewSelector> clusteringSelectors;
    private final List<MaterializedViewSelector> includedSelectors;

    ColumnFamilyStore baseCfs;
    public ColumnFamilyStore viewCfs;
    MaterializedViewBuilder builder;

    public MaterializedView(MaterializedViewDefinition definition,
                            ColumnFamilyStore baseCfs)
    {
        this.definition = definition;
        this.name = this.definition.viewName;

        this.baseCfs = baseCfs;

        clusteringSelectors = new ArrayList<>();
        includedSelectors = new ArrayList<>();
    }

    private synchronized void createViewCfsAndSelectors()
    {
        if (viewCfs != null)
            return;

        assert baseCfs != null;

        CFMetaData viewCfm = getCFMetaData(definition, baseCfs.metadata);
        targetSelector = MaterializedViewSelector.create(baseCfs, definition.target);

        ColumnIdentifier target = definition.target;

        // All partition and clustering columns are included in the view, whether they are specified in the included columns or not
        for (ColumnDefinition definition: baseCfs.metadata.partitionKeyColumns())
        {
            ColumnIdentifier column = definition.name;
            if (column != target)
                clusteringSelectors.add(MaterializedViewSelector.create(baseCfs, column));
        }

        for (ColumnDefinition definition: baseCfs.metadata.clusteringColumns())
        {
            ColumnIdentifier column = definition.name;
            if (column != target)
                clusteringSelectors.add(MaterializedViewSelector.create(baseCfs, column));
        }

        if (definition.included.isEmpty())
        {
            for (ColumnDefinition definition: baseCfs.metadata.regularColumns())
            {
                ColumnIdentifier column = definition.name;
                if (column != target)
                    includedSelectors.add(MaterializedViewSelector.create(baseCfs, column));
            }
        }
        else
        {
            for (ColumnIdentifier column : definition.included)
                includedSelectors.add(MaterializedViewSelector.create(baseCfs, column));
        }

        viewCfs = Schema.instance.getColumnFamilyStoreInstance(viewCfm.cfId);
    }

    /**
     * Check to see if any value that is part of the view is updated. If so, we possibly need to mutate the view.
     *
     * @param cf Column family to check for selected values with
     * @return True if any of the selected values are contained in the column family.
     */
    public boolean cfModifiesSelectedColumn(ColumnFamily cf)
    {
        createViewCfsAndSelectors();

        // If we are including all of the columns, then any non-empty column family will need to be selected
        if (definition.included.isEmpty())
            return true;

        if (!cf.deletionInfo().isLive())
            return true;

        for (CellName cellName : cf.getColumnNames())
        {
            if (targetSelector.selects(cellName))
                return true;
            for (MaterializedViewSelector column: Iterables.concat(clusteringSelectors, includedSelectors))
            {
                if (column.selects(cellName))
                    return true;
            }
        }
        return false;
    }

    private Mutation createTombstone(MutationUnit mutationUnit, ByteBuffer partitionKey, long timestamp)
    {
        // Need to generate a tombstone in this case; there will be only one element because we do not allow Collections
        // for keys of a materialized view.
        Mutation mutation = new Mutation(viewCfs.metadata.ksName, partitionKey);
        ColumnFamily viewCf = mutation.addOrGet(viewCfs.metadata);
        CellNameType cellNameType = viewCfs.getComparator();
        if (cellNameType.isCompound())
        {
            Composite cellName = mutationUnit.viewComposite(cellNameType, clusteringSelectors, MutationUnit.earliest);
            RangeTombstone rt = new RangeTombstone(cellName.start(), cellName.end(), timestamp, Integer.MAX_VALUE);
            viewCf.addAtom(rt);
        }
        else
        {
            assert clusteringSelectors.size() == 1;
            ByteBuffer column = mutationUnit.clusteringValue(clusteringSelectors.get(0), MutationUnit.earliest);
            assert column != null : "Clustering Columns should never be null in a mutation";
            CellName cellName = cellNameType.cellFromByteBuffer(column);
            viewCf.addTombstone(cellName, 0, timestamp);
        }

        return mutation;
    }

    private Collection<Mutation> createTombstonesForUpdates(MutationUnit mutationUnit, long timestamp)
    {
        // Primary Key and Clustering columns do not generate tombstones
        if (!targetSelector.isBasePrimaryKey())
            return null;

        // Target must be modified in order for a tombstone to be created
        if (mutationUnit.clusteringValue(targetSelector, MutationUnit.oldValueIfUpdated) == null)
            return null;

        Mutation mutation = createTombstone(mutationUnit, mutationUnit.clusteringValue(targetSelector, MutationUnit.oldValueIfUpdated), timestamp);
        if (mutation != null)
            return Collections.singleton(mutation);
        return null;
    }

    private Collection<Mutation> createMutationsForInserts(MutationUnit mutationUnit, long timestamp, boolean tombstonesGenerated)
    {
        ByteBuffer partitionKey = mutationUnit.clusteringValue(targetSelector, MutationUnit.latest);
        if (partitionKey == null)
        {
            // Not having a partition key means we aren't updating anything
            return null;
        }

        MutationUnit.Resolver resolver = tombstonesGenerated ? MutationUnit.latest : MutationUnit.newValueIfUpdated;

        Mutation mutation = new Mutation(viewCfs.metadata.ksName, partitionKey);
        ColumnFamily viewCf = mutation.addOrGet(viewCfs.metadata);

        Composite composite = mutationUnit.viewComposite(viewCfs.metadata.comparator,
                                                         clusteringSelectors,
                                                         resolver);

        CFRowAdder rowAdder = new RowAdder(viewCf, composite, timestamp, mutationUnit.ttl);

        for (MaterializedViewSelector selector: includedSelectors)
        {
            Collection<Cell> cells = mutationUnit.values(selector, resolver);

            for (Cell cell : cells)
            {
                if (cell.name().isCollectionCell())
                    rowAdder.addCollectionEntry(selector.columnDefinition.name.toString(), cell.name().collectionElement(), cell.isLive() ? cell.value() : null);
                else
                    rowAdder.add(selector.columnDefinition.name.toString(), cell.isLive() ? cell.value() : null);
            }
        }

        return Collections.singleton(mutation);
    }

    private Mutation getCollectionRangeTombstone(ByteBuffer key, RangeTombstone tombstone, ConsistencyLevel cl)
    {
        CFMetaData metadata = baseCfs.metadata;
        int clusteringSize = metadata.clusteringColumns().size();

        //In the case of a tombstoned collection we have a shortcut.
        //Since collections can't be part of clustering we can simply tombstone the whole thing
        if (tombstone.min.size() > clusteringSize && tombstone.max.size() > clusteringSize &&
            ByteBufferUtil.compareUnsigned(tombstone.min.get(clusteringSize),
                                           tombstone.max.get(clusteringSize)) == 0)
        {
            ColumnDefinition collectionDef = metadata.getColumnDefinition(tombstone.min.get(clusteringSize));

            if (collectionDef == null || !collectionDef.type.isCollection())
                return null;

            Map<ColumnIdentifier, ByteBuffer> clusterings = new HashMap<>(clusteringSize);
            for (int i = 0; i < clusteringSize; i++)
            {
                //we can't handle RT across clustering keys
                assert ByteBufferUtil.compareUnsigned(tombstone.min.get(i), tombstone.max.get(i)) == 0;

                ColumnDefinition cdef = metadata.clusteringColumns().get(i);
                clusterings.put(cdef.name, tombstone.min.get(i));
            }


            boolean queryNeeded = false;
            for (MaterializedViewSelector selector : clusteringSelectors)
            {
                ColumnDefinition definition = selector.columnDefinition;
                if (baseCfs.metadata.getColumnDefinition(definition.name).isPartitionKey())
                    continue;

                queryNeeded = queryNeeded || !clusterings.containsKey(definition.name);
            }

            MutationUnit mu = new MutationUnit(baseCfs, key, clusterings);

            //Incase we need a diff partition key
            queryNeeded = queryNeeded || !clusterings.containsKey(targetSelector.columnDefinition.name);

            //Fetch missing info
            if (queryNeeded)
                query(key, DeletionInfo.live(), new MutationUnit.Set(mu, clusteringSelectors), cl);


            //Build modified RT mutation
            ByteBuffer targetKey = mu.clusteringValue(targetSelector, MutationUnit.earliest);
            if (targetKey == null)
                return null; //Fixme: need a empty mutation type

            Mutation mutation = new Mutation(metadata.ksName, targetKey);

            CBuilder builder = viewCfs.getComparator().prefixBuilder();

            for (MaterializedViewSelector selector : clusteringSelectors)
                builder.add(mu.clusteringValue(selector, MutationUnit.earliest));

            Composite range = viewCfs.getComparator().create(builder.build(), collectionDef);

            mutation.addOrGet(viewCfs.getColumnFamilyName())
                    .addAtom(new RangeTombstone(range.start(),
                                                range.end(),
                                                tombstone.timestamp(), tombstone.getLocalDeletionTime()));

            return mutation;
        }

        return null;
    }



    private Collection<Mutation> createForDeletionInfo(ByteBuffer key, ColumnFamily columnFamily, ConsistencyLevel consistency)
    {
        DeletionInfo deletionInfo = columnFamily.deletionInfo();
        if (deletionInfo.hasRanges() || deletionInfo.getTopLevelDeletion().markedForDeleteAt != Long.MIN_VALUE)
        {

            MutationUnit.Set mutationUnits = new MutationUnit.Set(baseCfs, clusteringSelectors);
            List<Mutation> mutations = new ArrayList<>();

            IDiskAtomFilter filter;
            long timestamp;
            if (deletionInfo.hasRanges())
            {
                ColumnSlice[] slices = new ColumnSlice[deletionInfo.rangeCount()];
                Iterator<RangeTombstone> tombstones = deletionInfo.rangeIterator();
                int i = 0;
                timestamp = Long.MIN_VALUE;
                while (tombstones.hasNext())
                {
                    RangeTombstone tombstone = tombstones.next();

                    Mutation m = getCollectionRangeTombstone(key, tombstone, consistency);
                    if (m != null)
                    {
                        mutations.add(m);
                    }
                    else
                    {
                        slices[i++] = new ColumnSlice(tombstone.min, tombstone.max);
                        timestamp = Math.max(timestamp, tombstone.timestamp());
                    }
                }

                if (i == 0)
                    return mutations;

                filter = new SliceQueryFilter(slices, false, 100);
            }
            else
            {
                filter = new IdentityQueryFilter();
                timestamp = deletionInfo.getTopLevelDeletion().markedForDeleteAt;
            }

            List<Row> rows = StorageProxy.read(Collections.singletonList(ReadCommand.create(baseCfs.metadata.ksName,
                                                                                            key,
                                                                                            baseCfs.metadata.cfName,
                                                                                            timestamp,
                                                                                            filter)), consistency);


            for (Row row : rows)
            {
                ColumnFamily cf = row.cf;

                if (cf == null)
                    continue;

                for (Cell cell : cf.getSortedColumns())
                    mutationUnits.addUnit(key, cell, false);
            }

            for (MutationUnit mutationUnit : mutationUnits)
            {
                ByteBuffer value = mutationUnit.clusteringValue(targetSelector, MutationUnit.earliest);
                if (value != null)
                {
                    Mutation mutation = createTombstone(mutationUnit, value, timestamp);
                    if (mutation != null)
                        mutations.add(mutation);
                }
            }

            if (!mutations.isEmpty())
                return mutations;
        }

        return null;
    }

    private void query(ByteBuffer key, DeletionInfo deletionInfo, MutationUnit.Set mutationUnits, ConsistencyLevel consistency)
    {
        ColumnSlice[] slices = new ColumnSlice[mutationUnits.size()];
        Iterator<MutationUnit> mutationUnitIterator = mutationUnits.iterator();
        for (int i = 0; i < slices.length; i++)
            slices[i] = mutationUnitIterator.next().getBaseColumnSlice();

        SliceQueryFilter queryFilter = new SliceQueryFilter(slices, false, 100);

        List<Row> rows = StorageProxy.read(Collections.singletonList(ReadCommand.create(baseCfs.metadata.ksName,
                                                                                        key,
                                                                                        baseCfs.metadata.cfName,
                                                                                        Long.MAX_VALUE,
                                                                                        queryFilter)), consistency);

        for (Row row: rows)
        {
            ColumnFamily cf = row.cf;

            if (cf == null)
                continue;

            for (Cell cell : cf.getSortedColumns())
            {
                if (deletionInfo.isDeleted(cell))
                    continue;

                mutationUnits.addUnit(key, cell, false);
            }
        }
    }

    private MutationUnit.Set separateMutationUnits(ByteBuffer key, ColumnFamily cf)
    {
        MutationUnit.Set mutationUnits = new MutationUnit.Set(baseCfs, clusteringSelectors);

        // For each cell name, we need to grab the clustering columns
        for (Cell cell: cf.getSortedColumns())
        {
            mutationUnits.addUnit(key, cell, true);
        }

        return mutationUnits;
    }

    public Collection<Mutation> createMutations(ByteBuffer key, ColumnFamily cf, ConsistencyLevel consistency, boolean isBuilding)
    {
        createViewCfsAndSelectors();

        if (cf.deletionInfo().isLive() && !cfModifiesSelectedColumn(cf))
        {
            return null;
        }

        MutationUnit.Set mutationUnits = separateMutationUnits(key, cf);

        // If we are building the view, we do not want to add old values; they will always be the same
        if (!isBuilding)
            query(key, cf.deletionInfo(), mutationUnits, consistency);

        Collection<Mutation> mutations = null;

        for (MutationUnit mutationUnit: mutationUnits)
        {
            Collection<Mutation> tombstones = null;
            if (!isBuilding)
            {
                tombstones = createTombstonesForUpdates(mutationUnit, cf.maxTimestamp());
                if (tombstones != null && !tombstones.isEmpty())
                {
                    if (mutations == null) mutations = new LinkedList<>();
                    mutations.addAll(tombstones);
                }
            }

            Collection<Mutation> inserts = createMutationsForInserts(mutationUnit, cf.maxTimestamp(), tombstones != null && !tombstones.isEmpty());
            if (inserts != null && !inserts.isEmpty())
            {
                if (mutations == null) mutations = new LinkedList<>();
                mutations.addAll(inserts);
            }
        }

        if (!isBuilding)
        {
            Collection<Mutation> deletion = createForDeletionInfo(key, cf, consistency);
            if (deletion != null && !deletion.isEmpty())
            {
                if (mutations == null) mutations = new LinkedList<>();
                mutations.addAll(deletion);
            }
        }

        return mutations;
    }

    public synchronized void build()
    {
        if (this.builder != null)
        {
            this.builder.stop();
            this.builder = null;
        }

        createViewCfsAndSelectors();

        this.builder = new MaterializedViewBuilder(baseCfs, this);
        CompactionManager.instance.submitMaterializedViewBuilder(builder);
    }

    public void reload()
    {
        createViewCfsAndSelectors();

        build();
    }

    public static CFMetaData getCFMetaData(MaterializedViewDefinition definition,
                                           CFMetaData baseCf)
    {
        Collection<ColumnDefinition> included = new ArrayList<>();
        for(ColumnIdentifier identifier: definition.included)
        {
            ColumnDefinition cfDef = baseCf.getColumnDefinition(identifier);
            assert cfDef != null;
            included.add(cfDef);
        }
        ColumnDefinition target = baseCf.getColumnDefinition(definition.target);

        UUID cfId = Schema.instance.getId(baseCf.ksName, definition.viewName);
        if (cfId != null)
            return Schema.instance.getCFMetaData(cfId);

        CellNameType comparator = getViewComparator(baseCf, definition.clusteringColumns, definition.included);
        CFMetaData viewCfm = CFMetaData.createMaterializedViewMetadata(definition.viewName, baseCf, target, comparator);

        viewCfm.addColumnDefinition(ColumnDefinition.partitionKeyDef(viewCfm, target.name.bytes, target.type, null));

        boolean includeAll = included.isEmpty();
        Integer position = 0;
        for (ColumnIdentifier ident: definition.clusteringColumns)
        {
            ColumnDefinition column = baseCf.getColumnDefinition(ident);
            viewCfm.addColumnDefinition(ColumnDefinition.clusteringKeyDef(viewCfm, column.name.bytes, column.type, position++));
        }

        Integer componentIndex = comparator.isCompound() ? comparator.clusteringPrefixSize() : null;
        for (ColumnDefinition column: baseCf.regularColumns())
        {
            if (column != target && (includeAll || included.contains(column)))
            {
                viewCfm.addColumnDefinition(ColumnDefinition.regularDef(viewCfm, column.name.bytes, column.type, componentIndex));
            }
        }

        for (ColumnDefinition column: baseCf.staticColumns())
        {
            if (column != target && (includeAll || included.contains(column)))
            {
                viewCfm.addColumnDefinition(ColumnDefinition.staticDef(viewCfm, column.name.bytes, column.type, componentIndex));
            }
        }

        return viewCfm;
    }

    public static CellNameType getViewComparator(CFMetaData baseCFMD, List<ColumnIdentifier> clusteringColumns, Collection<ColumnIdentifier> included)
    {
        List<AbstractType<?>> types = new ArrayList<>();
        for (ColumnIdentifier clusteringColumn: clusteringColumns)
        {
            types.add(baseCFMD.getColumnDefinition(clusteringColumn).type);
        }

        Map<ByteBuffer, CollectionType> ctct = null;
        if (included.isEmpty())
        {
            for (ColumnDefinition def: baseCFMD.allColumns())
            {
                if (def.type.isCollection())
                {
                    if (ctct == null)
                        ctct = new HashMap<>();
                    ctct.put(def.name.bytes, (CollectionType) def.type);
                }
            }
        }
        else
        {
            for (ColumnIdentifier identifier : included)
            {
                ColumnDefinition def = baseCFMD.getColumnDefinition(identifier);
                if (def.type.isCollection())
                {
                    if (ctct == null)
                        ctct = new HashMap<>();
                    ctct.put(identifier.bytes, (CollectionType) def.type);
                }
            }
        }

        return ctct != null
               ? new CompoundSparseCellNameType.WithCollection(types, ColumnToCollectionType.getInstance(ctct))
               : new CompoundSparseCellNameType(types);
    }
}
