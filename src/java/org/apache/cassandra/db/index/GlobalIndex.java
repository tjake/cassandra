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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.GlobalIndexDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.global.GlobalIndexBuilder;
import org.apache.cassandra.db.index.global.GlobalIndexSelector;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.service.StorageProxy;

public class GlobalIndex implements Index
{
    private static class MutationUnit
    {
        private final ColumnFamilyStore baseCfs;
        private final ByteBuffer partitionKey;
        private final Map<ColumnIdentifier, ByteBuffer> clusteringColumns;
        private Map<ColumnIdentifier, ByteBuffer> oldRegularColumnValues = new HashMap<>();
        private Map<ColumnIdentifier, ByteBuffer> newRegularColumnValues = new HashMap<>();
        private int ttl = 0;

        public MutationUnit(ColumnFamilyStore baseCfs, ByteBuffer key, Map<ColumnIdentifier, ByteBuffer> clusteringColumns)
        {
            this.baseCfs = baseCfs;
            this.partitionKey = key;
            this.clusteringColumns = clusteringColumns;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MutationUnit that = (MutationUnit) o;

            if (!clusteringColumns.equals(that.clusteringColumns)) return false;
            if (!partitionKey.equals(that.partitionKey)) return false;

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = partitionKey.hashCode();
            result = 31 * result + clusteringColumns.hashCode();
            return result;
        }

        public void addNewColumnValue(ColumnIdentifier columnIdentifier, ByteBuffer value)
        {
            newRegularColumnValues.put(columnIdentifier, value);
        }

        public void addOldColumnValue(ColumnIdentifier columnIdentifier, ByteBuffer value)
        {
            oldRegularColumnValues.put(columnIdentifier, value);
        }

        public ByteBuffer oldValueIfUpdated(ColumnIdentifier columnIdentifier)
        {
            if (!newRegularColumnValues.containsKey(columnIdentifier))
                return null;
            if (!oldRegularColumnValues.containsKey(columnIdentifier))
                return null;

            ByteBuffer newValue = newRegularColumnValues.get(columnIdentifier);
            ByteBuffer oldValue = oldRegularColumnValues.get(columnIdentifier);

            return newValue.compareTo(oldValue) != 0 ? oldValue : null;
        }

        public ByteBuffer newValueIfUpdated(ColumnIdentifier columnIdentifier)
        {
            if (!newRegularColumnValues.containsKey(columnIdentifier))
                return null;
            if (!oldRegularColumnValues.containsKey(columnIdentifier))
                return newRegularColumnValues.get(columnIdentifier);

            ByteBuffer newValue = newRegularColumnValues.get(columnIdentifier);
            ByteBuffer oldValue = oldRegularColumnValues.get(columnIdentifier);

            return newValue.compareTo(oldValue) != 0 ? newValue : null;
        }

        public ByteBuffer value(ColumnDefinition definition)
        {
            if (definition.isPartitionKey())
            {
                if (definition.isOnAllComponents())
                    return partitionKey;
                else
                {
                    CompositeType keyComparator = (CompositeType) baseCfs.metadata.getKeyValidator();
                    ByteBuffer[] components = keyComparator.split(partitionKey);
                    return components[definition.position()];
                }
            }
            else
            {
                ColumnIdentifier columnIdentifier = definition.name;

                if (clusteringColumns.containsKey(columnIdentifier))
                    return clusteringColumns.get(columnIdentifier);

                if (newRegularColumnValues.containsKey(columnIdentifier))
                    return newRegularColumnValues.get(columnIdentifier);

                if (oldRegularColumnValues.containsKey(columnIdentifier))
                    return oldRegularColumnValues.get(columnIdentifier);
            }

            return null;
        }
    }

    private static class RowAdder extends CFRowAdder
    {
        public RowAdder(ColumnFamily cf, Composite prefix, long timestamp, int ttl)
        {
            super(cf, prefix, timestamp, ttl, true);
        }
    }

    private final GlobalIndexDefinition definition;

    private ColumnDefinition target;
    private boolean includeAll;
    private Collection<ColumnDefinition> included;
    private List<ColumnDefinition> clusteringKeys;
    private List<ColumnDefinition> regularColumns;
    private List<ColumnDefinition> staticColumns;

    private GlobalIndexSelector targetSelector;
    private List<GlobalIndexSelector> clusteringSelectors;
    private List<GlobalIndexSelector> regularSelectors;
    private List<GlobalIndexSelector> staticSelectors;

    ColumnFamilyStore baseCfs;
    public ColumnFamilyStore indexCfs;
    GlobalIndexBuilder builder;
    public final String indexName;

    public GlobalIndex(GlobalIndexDefinition definition,
                       ColumnDefinition target,
                       Collection<ColumnDefinition> included,
                       ColumnFamilyStore baseCfs)
    {
        this.definition = definition;
        this.indexName = definition.indexName;

        this.target = target;
        this.included = included;
        this.includeAll = included.isEmpty();
        this.baseCfs = baseCfs;

        clusteringSelectors = new ArrayList<>();
        regularSelectors = new ArrayList<>();
        staticSelectors = new ArrayList<>();

        clusteringKeys = new ArrayList<>();
        regularColumns = new ArrayList<>();
        staticColumns = new ArrayList<>();
    }

    private void createIndexCfsAndSelectors()
    {
        if (indexCfs != null)
            return;

        assert baseCfs != null;
        assert target != null;

        CFMetaData indexedCfMetadata = getCFMetaData(definition, baseCfs.metadata);
        targetSelector = GlobalIndexSelector.create(baseCfs, target);

        // All partition and clustering columns are included in the index, whether they are specified in the included columns or not
        for (ColumnDefinition column: baseCfs.metadata.partitionKeyColumns())
        {
            if (column != target)
            {
                clusteringSelectors.add(GlobalIndexSelector.create(baseCfs, column));
                clusteringKeys.add(column);
            }
        }

        for (ColumnDefinition column: baseCfs.metadata.clusteringColumns())
        {
            if (column != target)
            {
                clusteringSelectors.add(GlobalIndexSelector.create(baseCfs, column));
                clusteringKeys.add(column);
            }
        }

        for (ColumnDefinition column: baseCfs.metadata.regularColumns())
        {
            if (column != target && (includeAll || included.contains(column)))
            {
                regularSelectors.add(GlobalIndexSelector.create(baseCfs, column));
                regularColumns.add(column);
            }
        }

        for (ColumnDefinition column: baseCfs.metadata.staticColumns())
        {
            if (column != target && (includeAll || included.contains(column)))
            {
                staticSelectors.add(GlobalIndexSelector.create(baseCfs, column));
                staticColumns.add(column);
            }
        }

        indexCfs = Schema.instance.getColumnFamilyStoreInstance(indexedCfMetadata.cfId);
    }

    /**
     * Check to see if any value that is part of the index is updated. If so, we possibly need to mutate the index.
     *
     * @param cf Column family to check for indexed values with
     * @return True if any of the indexed or included values are contained in the column family.
     */
    public boolean cfModifiesIndexedColumn(ColumnFamily cf)
    {
        // If we are including all of the columns, then any non-empty column family will need to be indexed
        if (includeAll)
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

    private boolean modifiesTarget(MutationUnit mutationUnit)
    {
        // these will always be specified
        if (target.isPrimaryKeyColumn() || target.isClusteringColumn())
            return true;

        // Otherwise, see if the *new* values are updated
        if (mutationUnit.oldValueIfUpdated(target.name) != null)
            return true;

        return false;
    }

    private Mutation createTombstone(MutationUnit mutationUnit, long timestamp)
    {
        // Need to generate a tombstone in this case
        ByteBuffer partitionKey = mutationUnit.oldValueIfUpdated(target.name);

        Mutation mutation = new Mutation(indexCfs.metadata.ksName, partitionKey);
        ColumnFamily indexCf = mutation.addOrGet(indexCfs.metadata);
        CellNameType cellNameType = indexCfs.getComparator();
        if (cellNameType.isCompound())
        {
            CBuilder builder = cellNameType.prefixBuilder();
            for (ColumnDefinition definition: clusteringKeys)
                builder = builder.add(mutationUnit.value(definition));
            Composite cellName = builder.build();
            RangeTombstone rt = new RangeTombstone(cellName.start(), cellName.end(), timestamp, Integer.MAX_VALUE);
            indexCf.addAtom(rt);
        }
        else
        {
            assert clusteringKeys.size() == 1;
            CellName cellName = cellNameType.cellFromByteBuffer(mutationUnit.value(clusteringKeys.get(0)));
            indexCf.addTombstone(cellName, 0, timestamp);
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

        return Collections.singleton(createTombstone(mutationUnit, timestamp));
    }

    private Collection<Mutation> createMutationsForInserts(MutationUnit mutationUnit, long timestamp, boolean tombstonesGenerated)
    {
        ByteBuffer partitionKey = mutationUnit.value(target);
        if (partitionKey == null)
        {
            // Not having a partition key means we aren't updating anything
            return null;
        }

        ByteBuffer[] clusteringColumns = new ByteBuffer[clusteringKeys.size()];
        ByteBuffer[] regularColumns = new ByteBuffer[this.regularColumns.size()];
        ByteBuffer[] staticColumns = new ByteBuffer[this.staticColumns.size()];

        for (int i = 0; i < clusteringColumns.length; i++)
        {
            clusteringColumns[i] = mutationUnit.value(clusteringKeys.get(i));
        }

        for (int i = 0; i < regularColumns.length; i++)
        {
            regularColumns[i] = tombstonesGenerated
                                 ? mutationUnit.value(this.regularColumns.get(i))
                                 : mutationUnit.newValueIfUpdated(this.regularColumns.get(i).name);
        }

        for (int i = 0; i < staticColumns.length; i++)
        {
            staticColumns[i] = tombstonesGenerated
                                ? mutationUnit.value(this.staticColumns.get(i))
                                : mutationUnit.newValueIfUpdated(this.staticColumns.get(i).name);
        }

        Mutation mutation = new Mutation(indexCfs.metadata.ksName, partitionKey);
        ColumnFamily indexCf = mutation.addOrGet(indexCfs.metadata);
        CellNameType cellNameType = indexCfs.getComparator();
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

        CFRowAdder rowAdder = new RowAdder(indexCf, composite, timestamp, mutationUnit.ttl);
        for (int i = 0; i < regularColumns.length; i++)
        {
            if (regularColumns[i] != null)
                rowAdder.add(regularSelectors.get(i).columnDefinition.name.toString(), regularColumns[i]);
        }
        for (int i = 0; i < staticColumns.length; i++)
        {
            if (staticColumns[i] != null)
                rowAdder.add(staticSelectors.get(i).columnDefinition.name.toString(), staticColumns[i]);
        }
        return Collections.singleton(mutation);
    }

    private Collection<Mutation> createForDeletionInfo(ByteBuffer key, ColumnFamily columnFamily, ConsistencyLevel consistency)
    {
        DeletionInfo deletionInfo = columnFamily.deletionInfo();
        if (deletionInfo.hasRanges() || deletionInfo.getTopLevelDeletion().markedForDeleteAt != Long.MIN_VALUE)
        {
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
                    slices[i++] = new ColumnSlice(tombstone.min, tombstone.max);
                    timestamp = Math.max(timestamp, tombstone.timestamp());
                }
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

            Map<MutationUnit, MutationUnit> mutationUnits = new HashMap<>();
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

                    MutationUnit mutationUnit = new MutationUnit(baseCfs, key, clusteringColumns);
                    if (mutationUnits.containsKey(mutationUnit))
                    {
                        MutationUnit previous = mutationUnits.get(mutationUnit);
                        previous.addOldColumnValue(cell.name().cql3ColumnName(cf.metadata()), cell.value());
                    }
                    else
                    {
                        mutationUnits.put(mutationUnit, mutationUnit);
                    }
                }
            }

            if (!mutationUnits.isEmpty())
            {
                List<Mutation> mutations = new ArrayList<>();
                for (MutationUnit mutationUnit : mutationUnits.values())
                {
                    mutations.add(createTombstone(mutationUnit, timestamp));
                }
                return mutations;
            }
        }
        return null;
    }

    private ColumnSlice getColumnSlice(MutationUnit mutationUnit)
    {
        CBuilder builder = baseCfs.getComparator().prefixBuilder();
        CFMetaData cfmd = baseCfs.metadata;
        ByteBuffer[] buffers = new ByteBuffer[mutationUnit.clusteringColumns.size()];
        for (Map.Entry<ColumnIdentifier, ByteBuffer> buffer: mutationUnit.clusteringColumns.entrySet())
        {
            buffers[cfmd.getColumnDefinition(buffer.getKey()).position()] = buffer.getValue();
        }

        for (ByteBuffer byteBuffer: buffers)
            builder = builder.add(byteBuffer);

        Composite built = builder.build();
        return new ColumnSlice(built.start(), built.end());
    }

    private void query(ByteBuffer key, Collection<MutationUnit> mutationUnits, ConsistencyLevel consistency, long timestamp)
    {
        ColumnSlice[] slices = new ColumnSlice[mutationUnits.size()];
        Iterator<MutationUnit> mutationUnitIterator = mutationUnits.iterator();
        Map<MutationUnit, MutationUnit> mapMutationUnits = new HashMap<>();
        for (int i = 0; i < slices.length; i++)
        {
            MutationUnit next = mutationUnitIterator.next();
            slices[i] = getColumnSlice(next);
            mapMutationUnits.put(next, next);
        }

        SliceQueryFilter queryFilter = new SliceQueryFilter(slices, false, 100);

        List<Row> rows = StorageProxy.read(Collections.singletonList(ReadCommand.create(baseCfs.metadata.ksName,
                                                                                        key,
                                                                                        baseCfs.metadata.cfName,
                                                                                        timestamp,
                                                                                        queryFilter)), consistency);

        for (Row row: rows)
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

                MutationUnit mutationUnit = new MutationUnit(baseCfs, key, clusteringColumns);
                if (mapMutationUnits.containsKey(mutationUnit))
                {
                    MutationUnit previous = mapMutationUnits.get(mutationUnit);
                    previous.addOldColumnValue(cell.name().cql3ColumnName(cf.metadata()), cell.value());
                }
                else
                {
                    mapMutationUnits.put(mutationUnit, mutationUnit);
                }
            }
        }
    }

    private Collection<MutationUnit> separateMutationUnits(ByteBuffer key, ColumnFamily cf)
    {
        Map<MutationUnit, MutationUnit> mutationUnits = new HashMap<>();
        // For each cell name, we need to grab the clustering columns
        for (Cell cell: cf.getSortedColumns())
        {
            Map<ColumnIdentifier, ByteBuffer> clusteringColumns = new HashMap<>();
            for (ColumnDefinition cdef : cf.metadata().clusteringColumns())
            {
                clusteringColumns.put(cdef.name, cell.name().get(cdef.position()));
            }

            MutationUnit mutationUnit = new MutationUnit(baseCfs, key, clusteringColumns);
            if (cell instanceof ExpiringCell)
            {
                mutationUnit.ttl = ((ExpiringCell)cell).getTimeToLive();
            }

            if (mutationUnits.containsKey(mutationUnit))
            {
                mutationUnit = mutationUnits.get(mutationUnit);
            }
            else
            {
                mutationUnits.put(mutationUnit, mutationUnit);
            }
            mutationUnit.addNewColumnValue(cell.name().cql3ColumnName(cf.metadata()), cell.value());
        }

        return mutationUnits.values();
    }

    public Collection<Mutation> createMutations(ByteBuffer key, ColumnFamily cf, ConsistencyLevel consistency, boolean isBuilding)
    {
        createIndexCfsAndSelectors();

        if (!cf.deletionInfo().hasRanges() && cf.deletionInfo().getTopLevelDeletion().markedForDeleteAt == Long.MIN_VALUE && !cfModifiesIndexedColumn(cf))
        {
            return null;
        }

        Collection<MutationUnit> mutationUnits = separateMutationUnits(key, cf);

        // If we are building the index, we do not want to add old values; they will always be the same
        if (!isBuilding)
            query(key, mutationUnits, consistency, cf.maxTimestamp());

        Collection<Mutation> mutations = null;

        for (MutationUnit mutationUnit: mutationUnits)
        {
            Collection<Mutation> tombstones = null;
            if (!isBuilding)
            {
                tombstones = createTombstonesForUpdates(mutationUnit, cf.maxTimestamp());
                if (tombstones != null && !tombstones.isEmpty())
                {
                    if (mutations == null) mutations = new ArrayList<>();
                    mutations.addAll(tombstones);
                }
            }

            Collection<Mutation> inserts = createMutationsForInserts(mutationUnit, cf.maxTimestamp(), tombstones != null && !tombstones.isEmpty());
            if (inserts != null && !inserts.isEmpty())
            {
                if (mutations == null) mutations = new ArrayList<>();
                mutations.addAll(inserts);
            }
        }

        if (!isBuilding)
        {
            Collection<Mutation> deletion = createForDeletionInfo(key, cf, consistency);
            if (deletion != null && !deletion.isEmpty())
            {
                if (mutations == null) mutations = new ArrayList<>();
                mutations.addAll(deletion);
            }
        }

        return mutations;
    }

    public boolean supportsOperator(Operator operator)
    {
        return operator == Operator.EQ;
    }

    public synchronized void build()
    {
        if (this.builder != null)
        {
            this.builder.stop();
            this.builder = null;
        }

        createIndexCfsAndSelectors();

        this.builder = new GlobalIndexBuilder(baseCfs, this);
        ScheduledExecutors.optionalTasks.execute(builder);
    }

    public void reload()
    {
        createIndexCfsAndSelectors();

        build();
    }

    public static CFMetaData getCFMetaData(GlobalIndexDefinition definition,
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

        String name = definition.getCfName();
        UUID cfId = Schema.instance.getId(baseCf.ksName, name);
        if (cfId != null)
            return Schema.instance.getCFMetaData(cfId);

        CellNameType comparator = getIndexComparator(baseCf, target);
        CFMetaData indexedCfMetadata = CFMetaData.createGlobalIndexMetadata(name, baseCf, target, comparator);

        indexedCfMetadata.addColumnDefinition(ColumnDefinition.partitionKeyDef(indexedCfMetadata, target.name.bytes, target.type, null));

        boolean includeAll = included.isEmpty();
        Integer position = 0;
        // All partition and clustering columns are included in the index, whether they are specified in the included columns or not
        for (ColumnDefinition column: baseCf.partitionKeyColumns())
        {
            if (column != target)
            {
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.clusteringKeyDef(indexedCfMetadata, column.name.bytes, column.type, position++));
            }
        }

        for (ColumnDefinition column: baseCf.clusteringColumns())
        {
            if (column != target)
            {
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.clusteringKeyDef(indexedCfMetadata, column.name.bytes, column.type, position++));
            }
        }

        Integer componentIndex = comparator.isCompound() ? comparator.clusteringPrefixSize() : null;
        for (ColumnDefinition column: baseCf.regularColumns())
        {
            if (column != target && (includeAll || included.contains(column)))
            {
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.regularDef(indexedCfMetadata, column.name.bytes, column.type, componentIndex));
            }
        }

        for (ColumnDefinition column: baseCf.staticColumns())
        {
            if (column != target && (includeAll || included.contains(column)))
            {
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.staticDef(indexedCfMetadata, column.name.bytes, column.type, componentIndex));
            }
        }

        return indexedCfMetadata;
    }

    public static CellNameType getIndexComparator(CFMetaData baseCFMD, ColumnDefinition target)
    {
        List<AbstractType<?>> types = new ArrayList<>();
        // All partition and clustering columns are included in the index, whether they are specified in the included columns or not
        for (ColumnDefinition column: baseCFMD.partitionKeyColumns())
        {
            if (column != target)
            {
                types.add(column.type);
            }
        }

        for (ColumnDefinition column: baseCFMD.clusteringColumns())
        {
            if (column != target)
            {
                types.add(column.type);
            }
        }
        return new CompoundSparseCellNameType(types);
    }
}
