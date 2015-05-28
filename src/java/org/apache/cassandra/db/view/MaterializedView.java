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
        public RowAdder(ColumnFamily cf, Composite prefix, long timestamp, int ttl)
        {
            super(cf, prefix, timestamp, ttl, true);
        }
    }

    private final MaterializedViewDefinition definition;
    public final String name;

    private ColumnDefinition target;
    private List<ColumnDefinition> clusteringKeys;
    private List<ColumnDefinition> regularColumns;
    private List<ColumnDefinition> staticColumns;

    private MaterializedViewSelector targetSelector;
    private List<MaterializedViewSelector> clusteringSelectors;
    private List<MaterializedViewSelector> regularSelectors;
    private List<MaterializedViewSelector> staticSelectors;

    ColumnFamilyStore baseCfs;
    public ColumnFamilyStore viewCfs;
    MaterializedViewBuilder builder;

    public MaterializedView(MaterializedViewDefinition definition,
                            ColumnDefinition target,
                            ColumnFamilyStore baseCfs)
    {
        this.definition = definition;
        this.name = this.definition.viewName;

        this.target = target;
        this.baseCfs = baseCfs;

        clusteringSelectors = new ArrayList<>();
        regularSelectors = new ArrayList<>();
        staticSelectors = new ArrayList<>();

        clusteringKeys = new ArrayList<>();
        regularColumns = new ArrayList<>();
        staticColumns = new ArrayList<>();
    }

    private synchronized void createViewCfsAndSelectors()
    {
        if (viewCfs != null)
            return;

        assert baseCfs != null;
        assert target != null;

        CFMetaData viewCfm = getCFMetaData(definition, baseCfs.metadata);
        targetSelector = MaterializedViewSelector.create(baseCfs, target);

        // All partition and clustering columns are included in the view, whether they are specified in the included columns or not
        for (ColumnDefinition column: baseCfs.metadata.partitionKeyColumns())
        {
            if (column != target)
            {
                clusteringSelectors.add(MaterializedViewSelector.create(baseCfs, column));
                clusteringKeys.add(column);
            }
        }

        for (ColumnDefinition column: baseCfs.metadata.clusteringColumns())
        {
            if (column != target)
            {
                clusteringSelectors.add(MaterializedViewSelector.create(baseCfs, column));
                clusteringKeys.add(column);
            }
        }

        if (definition.included.isEmpty())
        {
            for (ColumnDefinition column: baseCfs.metadata.regularColumns())
            {
                if (column != target)
                {
                    regularSelectors.add(MaterializedViewSelector.create(baseCfs, column));
                    regularColumns.add(column);
                }
            }

            for (ColumnDefinition column: baseCfs.metadata.staticColumns())
            {
                if (column != target)
                {
                    staticSelectors.add(MaterializedViewSelector.create(baseCfs, column));
                    staticColumns.add(column);
                }
            }
        }
        else
        {
            for (ColumnIdentifier identifier : definition.included)
            {
                ColumnDefinition column = baseCfs.metadata.getColumnDefinition(identifier);
                if (column.isStatic())
                {
                    staticSelectors.add(MaterializedViewSelector.create(baseCfs, column));
                    staticColumns.add(column);
                }
                else
                {
                    regularSelectors.add(MaterializedViewSelector.create(baseCfs, column));
                    regularColumns.add(column);
                }
            }
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
            for (MaterializedViewSelector column: Iterables.concat(clusteringSelectors, regularSelectors, staticSelectors))
            {
                if (column.selects(cellName))
                    return true;
            }
        }
        return false;
    }

    private boolean modifiesTarget(MutationUnit mutationUnit)
    {
        // these will always be specified
        if (target.isPrimaryKeyColumn() || target.isClusteringColumn())
            return true;

        // Otherwise, see if the *new* values are updated
        return !mutationUnit.values(target.name, MutationUnit.oldValueIfUpdated).isEmpty();
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
            CBuilder builder = cellNameType.prefixBuilder();
            for (ColumnDefinition definition: clusteringKeys)
            {
                ByteBuffer column = mutationUnit.clusteringValue(definition, MutationUnit.earliest);
                assert column != null : "Clustering Columns should never be null in a mutation";
                builder = builder.add(column);
            }
            Composite cellName = builder.build();
            RangeTombstone rt = new RangeTombstone(cellName.start(), cellName.end(), timestamp, Integer.MAX_VALUE);
            viewCf.addAtom(rt);
        }
        else
        {
            assert clusteringKeys.size() == 1;
            ByteBuffer column = mutationUnit.clusteringValue(clusteringKeys.get(0), MutationUnit.earliest);
            assert column != null : "Clustering Columns should never be null in a mutation";
            CellName cellName = cellNameType.cellFromByteBuffer(column);
            viewCf.addTombstone(cellName, 0, timestamp);
        }

        return mutation;
    }

    private Collection<Mutation> createTombstonesForUpdates(MutationUnit mutationUnit, long timestamp)
    {
        // Primary Key and Clustering columns do not generate tombstones
        if (!targetSelector.canGenerateTombstones())
            return null;

        // Make sure that target is actually modified
        if (!modifiesTarget(mutationUnit))
            return null;

        Mutation mutation = createTombstone(mutationUnit, mutationUnit.clusteringValue(target, MutationUnit.oldValueIfUpdated), timestamp);
        if (mutation != null)
            return Collections.singleton(mutation);
        return null;
    }

    private Collection<Mutation> createMutationsForInserts(MutationUnit mutationUnit, long timestamp, boolean tombstonesGenerated)
    {
        ByteBuffer partitionKey = mutationUnit.clusteringValue(target, MutationUnit.latest);
        MutationUnit.Resolver resolver = tombstonesGenerated ? MutationUnit.latest : MutationUnit.newValueIfUpdated;

        if (partitionKey == null)
        {
            // Not having a partition key means we aren't updating anything
            return null;
        }

        ByteBuffer[] clusteringColumns = new ByteBuffer[clusteringKeys.size()];

        for (int i = 0; i < clusteringColumns.length; i++)
        {
            ByteBuffer column = mutationUnit.clusteringValue(clusteringKeys.get(i), MutationUnit.latest);
            assert column != null : "Clustering Columns should never be null in a mutation";
            clusteringColumns[i] = column;
        }

        Mutation mutation = new Mutation(viewCfs.metadata.ksName, partitionKey);
        ColumnFamily viewCf = mutation.addOrGet(viewCfs.metadata);
        CellNameType cellNameType = viewCfs.getComparator();
        Composite composite;
        if (cellNameType.isCompound())
        {
            CBuilder builder = cellNameType.prefixBuilder();
            for (ByteBuffer prefix : clusteringColumns)
                builder = builder.add(prefix);
            composite = builder.build();
        }
        else
        {
            assert clusteringColumns.length == 1;
            composite = cellNameType.make(clusteringColumns[0]);
        }
        CFRowAdder rowAdder = new RowAdder(viewCf, composite, timestamp, mutationUnit.ttl);

        for (ColumnDefinition def : regularColumns)
        {
            Collection<Cell> cells = mutationUnit.values(def.name, resolver);

            for (Cell cell : cells)
            {
                if (cell.name().isCollectionCell())
                    rowAdder.addCollectionEntry(def.name.toString(), cell.name().collectionElement(), cell.isLive() ? cell.value() : null);
                else
                    rowAdder.add(def.name.toString(), cell.isLive() ? cell.value() : null);
            }
        }

        for (int i = 0; i < staticColumns.size(); i++)
        {
            ColumnDefinition def = this.staticColumns.get(i);
            Collection<Cell> cells = mutationUnit.values(def.name, resolver);

            for (Cell cell: cells)
            {
                if (cell.name().isCollectionCell())
                    rowAdder.addCollectionEntry(def.name.toString(), cell.name().collectionElement(), cell.value());
                else
                    rowAdder.add(def.name.toString(), cell.value());
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


            List<ColumnDefinition> columnsNeeded = new ArrayList<>(clusteringSize);
            for (ColumnDefinition cdef : clusteringKeys)
            {
                if (cdef.isPartitionKey())
                    continue;

                ByteBuffer b = clusterings.get(cdef.name);
                if (b == null)
                    columnsNeeded.add(cdef);
            }

            MutationUnit mu = new MutationUnit(baseCfs, key, clusterings);

            //Incase we need a diff partition key
            if (clusterings.get(target) == null)
                columnsNeeded.add(target);

            //Fetch missing info
            if (!columnsNeeded.isEmpty())
                query(key, DeletionInfo.live(), new MutationUnit.Set(mu), cl);


            //Build modified RT mutation
            ByteBuffer targetKey = mu.clusteringValue(target, MutationUnit.earliest);
            if (targetKey == null)
                return null; //Fixme: need a empty mutation type

            Mutation mutation = new Mutation(metadata.ksName, targetKey);

            CBuilder builder = viewCfs.getComparator().prefixBuilder();

            for (int i = 0; i < clusteringKeys.size(); i++)
            {
                ColumnDefinition cdef = clusteringKeys.get(i);
                builder.add(mu.clusteringValue(cdef, MutationUnit.earliest));
            }

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

            MutationUnit.Set mutationUnits = new MutationUnit.Set(baseCfs);
            List<Mutation> mutations = new ArrayList<>();

            IDiskAtomFilter filter;
            long timestamp;
            if (deletionInfo.hasRanges())
            {
                List<ColumnSlice> slices = new ArrayList<>(deletionInfo.rangeCount());
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
                        slices.add(new ColumnSlice(tombstone.min, tombstone.max));
                        timestamp = Math.max(timestamp, tombstone.timestamp());
                    }
                }

                if (slices.isEmpty())
                    return mutations;

                filter = new SliceQueryFilter(slices.toArray(new ColumnSlice[]{}), false, 100);
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
                {
                    Map<ColumnIdentifier, ByteBuffer> clusteringColumns = new HashMap<>();
                    for (ColumnDefinition cdef : cf.metadata().clusteringColumns())
                    {
                        clusteringColumns.put(cdef.name, cell.name().get(cdef.position()));
                    }

                    mutationUnits.addUnit(key, clusteringColumns, cell, false);
                }
            }

            for (MutationUnit mutationUnit : mutationUnits)
            {
                ByteBuffer value = mutationUnit.clusteringValue(target, MutationUnit.earliest);
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

                Map<ColumnIdentifier, ByteBuffer> clusteringColumns = new HashMap<>();
                for (ColumnDefinition cdef : cf.metadata().clusteringColumns())
                {
                    clusteringColumns.put(cdef.name, cell.name().get(cdef.position()));
                }

                mutationUnits.addUnit(key, clusteringColumns, cell, false);
            }
        }
    }

    private MutationUnit.Set separateMutationUnits(ByteBuffer key, ColumnFamily cf)
    {
        MutationUnit.Set mutationUnits = new MutationUnit.Set(baseCfs);

        // For each cell name, we need to grab the clustering columns
        for (Cell cell: cf.getSortedColumns())
        {
            Map<ColumnIdentifier, ByteBuffer> clusteringColumns = new HashMap<>();
            for (ColumnDefinition cdef : cf.metadata().clusteringColumns())
            {
                clusteringColumns.put(cdef.name, cell.name().get(cdef.position()));
            }

            mutationUnits.addUnit(key, clusteringColumns, cell, true);
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
