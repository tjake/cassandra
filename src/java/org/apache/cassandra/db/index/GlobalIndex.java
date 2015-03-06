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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.global.GlobalIndexSelector;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;

public class GlobalIndex
{
    private static class MutationUnit
    {
        private final ColumnFamilyStore baseCfs;
        private final ColumnFamilyStore indexCfs;
        private final ByteBuffer partitionKey;
        private final Map<ColumnIdentifier, ByteBuffer> clusteringColumns;
        private Map<ColumnIdentifier, ByteBuffer> oldRegularColumnValues = new HashMap<>();
        private Map<ColumnIdentifier, ByteBuffer> newRegularColumnValues = new HashMap<>();
        private int ttl = 0;

        public MutationUnit(ColumnFamilyStore baseCfs, ColumnFamilyStore indexCfs, ByteBuffer key, Map<ColumnIdentifier, ByteBuffer> clusteringColumns)
        {
            this.baseCfs = baseCfs;
            this.indexCfs = indexCfs;
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

    private ColumnDefinition target;
    private Collection<ColumnDefinition> denormalized;
    private List<ColumnDefinition> clusteringKeys;
    private List<ColumnDefinition> regularColumns;
    private List<ColumnDefinition> staticColumns;

    private GlobalIndexSelector targetSelector;
    private List<GlobalIndexSelector> clusteringSelectors;
    private List<GlobalIndexSelector> regularSelectors;
    private List<GlobalIndexSelector> staticSelectors;

    private ColumnFamilyStore baseCfs;
    private ColumnFamilyStore indexCfs;

    public GlobalIndex(ColumnDefinition target, Collection<ColumnDefinition> denormalized, ColumnFamilyStore baseCfs)
    {
        this.target = target;
        this.denormalized = denormalized;
        this.baseCfs = baseCfs;

        clusteringSelectors = new ArrayList<>();
        regularSelectors = new ArrayList<>();
        staticSelectors = new ArrayList<>();

        clusteringKeys = new ArrayList<>();
        regularColumns = new ArrayList<>();
        staticColumns = new ArrayList<>();

        createIndexCfsAndSelectors();
    }

    private void createIndexCfsAndSelectors()
    {
        assert baseCfs != null;
        assert target != null;

        CFMetaData indexedCfMetadata = getCFMetaData(baseCfs.metadata, target, denormalized);
        targetSelector = GlobalIndexSelector.create(baseCfs, target);

        // All partition and clustering columns are included in the index, whether they are specified in the denormalized columns or not
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
            if (column != target && denormalized.contains(column))
            {
                regularSelectors.add(GlobalIndexSelector.create(baseCfs, column));
                regularColumns.add(column);
            }
        }

        for (ColumnDefinition column: baseCfs.metadata.staticColumns())
        {
            if (column != target && denormalized.contains(column))
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

    private Collection<Mutation> createTombstonesForUpdates(MutationUnit mutationUnit, long timestamp)
    {
        // Primary Key and Clustering columns do not generate tombstones
        if (!targetSelector.canGenerateTombstones())
            return null;

        // Make sure that target is actually modified
        if (!modifiesTarget(mutationUnit))
            return null;

        // Need to generate a tombstone in this case
        ByteBuffer partitionKey = mutationUnit.oldValueIfUpdated(target.name);

        Mutation mutation = new Mutation(indexCfs.metadata.ksName, partitionKey);
        ColumnFamily indexCf = mutation.addOrGet(indexCfs.metadata);
        CellNameType cellNameType = indexCfs.getComparator();
        CellName cellName;
        if (cellNameType.isCompound())
        {
            CBuilder builder = cellNameType.prefixBuilder();
            for (ColumnDefinition definition: clusteringKeys)
                builder = builder.add(mutationUnit.value(definition));
            cellName = cellNameType.rowMarker(builder.build());
        }
        else
        {
            assert clusteringKeys.size() == 1;
            cellName = cellNameType.cellFromByteBuffer(mutationUnit.value(clusteringKeys.get(0)));
        }
        indexCf.addTombstone(cellName, 0, timestamp);
        return Collections.singleton(mutation);
    }

    private Collection<Mutation> createMutationsForInserts(MutationUnit mutationUnit, long timestamp, boolean tombstonesGenerated)
    {
        ByteBuffer partitionKey = mutationUnit.value(target);
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

                    MutationUnit mutationUnit = new MutationUnit(baseCfs, indexCfs, key, clusteringColumns);
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
                    Mutation mutation = new Mutation(indexCfs.metadata.ksName, mutationUnit.value(target));
                    ColumnFamily indexCf = mutation.addOrGet(indexCfs.metadata);
                    CellNameType cellNameType = indexCfs.getComparator();
                    CellName cellName;
                    if (cellNameType.isCompound())
                    {
                        CBuilder builder = cellNameType.prefixBuilder();
                        for (ColumnDefinition definition : clusteringKeys)
                            builder = builder.add(mutationUnit.value(definition));
                        cellName = cellNameType.rowMarker(builder.build());
                    }
                    else
                    {
                        assert clusteringKeys.size() == 1;
                        cellName = cellNameType.cellFromByteBuffer(mutationUnit.value(clusteringKeys.get(0)));
                    }
                    indexCf.addTombstone(cellName, 0, timestamp);
                    mutations.add(mutation);
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

                MutationUnit mutationUnit = new MutationUnit(baseCfs, indexCfs, key, clusteringColumns);
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

            MutationUnit mutationUnit = new MutationUnit(baseCfs, indexCfs, key, clusteringColumns);
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

    public Collection<Mutation> createMutations(ByteBuffer key, ColumnFamily cf, ConsistencyLevel consistency)
    {
        if (!cf.deletionInfo().hasRanges() && cf.deletionInfo().getTopLevelDeletion().markedForDeleteAt == Long.MIN_VALUE && !modifiesIndexedColumn(cf))
        {
            return null;
        }

        Collection<MutationUnit> mutationUnits = separateMutationUnits(key, cf);

        query(key, mutationUnits, consistency, cf.maxTimestamp());

        Collection<Mutation> mutations = null;

        for (MutationUnit mutationUnit: mutationUnits)
        {
            Collection<Mutation> tombstones = createTombstonesForUpdates(mutationUnit, cf.maxTimestamp());
            if (tombstones != null && !tombstones.isEmpty())
            {
                if (mutations == null) mutations = new ArrayList<>();
                mutations.addAll(tombstones);
            }

            Collection<Mutation> inserts = createMutationsForInserts(mutationUnit, cf.maxTimestamp(), tombstones != null && !tombstones.isEmpty());
            if (inserts != null && !inserts.isEmpty())
            {
                if (mutations == null) mutations = new ArrayList<>();
                mutations.addAll(inserts);
            }
        }

        Collection<Mutation> deletion = createForDeletionInfo(key, cf, consistency);
        if (deletion != null && !deletion.isEmpty())
        {
            if (mutations == null) mutations = new ArrayList<>();
            mutations.addAll(deletion);
        }

        return mutations;
    }

    public static CFMetaData getCFMetaData(CFMetaData baseCFMD, ColumnDefinition target, Collection<ColumnDefinition> denormalized)
    {
        String name = baseCFMD.cfName + "_" + ByteBufferUtil.bytesToHex(target.name.bytes);
        UUID cfId = Schema.instance.getId(baseCFMD.ksName, name);
        if (cfId != null)
            return Schema.instance.getCFMetaData(cfId);

        CellNameType comparator = getIndexComparator(baseCFMD, target);
        CFMetaData indexedCfMetadata = CFMetaData.createGlobalIndexMetadata(name, baseCFMD, target, comparator);

        indexedCfMetadata.addColumnDefinition(ColumnDefinition.partitionKeyDef(indexedCfMetadata, target.name.bytes, target.type, null));

        Integer position = 0;
        // All partition and clustering columns are included in the index, whether they are specified in the denormalized columns or not
        for (ColumnDefinition column: baseCFMD.partitionKeyColumns())
        {
            if (column != target)
            {
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.clusteringKeyDef(indexedCfMetadata, column.name.bytes, column.type, position++));
            }
        }

        for (ColumnDefinition column: baseCFMD.clusteringColumns())
        {
            if (column != target)
            {
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.clusteringKeyDef(indexedCfMetadata, column.name.bytes, column.type, position++));
            }
        }

        Integer componentIndex = comparator.isCompound() ? comparator.clusteringPrefixSize() : null;
        for (ColumnDefinition column: baseCFMD.regularColumns())
        {
            if (column != target && denormalized.contains(column))
            {
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.regularDef(indexedCfMetadata, column.name.bytes, column.type, componentIndex));
            }
        }

        for (ColumnDefinition column: baseCFMD.staticColumns())
        {
            if (column != target && denormalized.contains(column))
            {
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.staticDef(indexedCfMetadata, column.name.bytes, column.type, componentIndex));
            }
        }

        return indexedCfMetadata;
    }

    public static CellNameType getIndexComparator(CFMetaData baseCFMD, ColumnDefinition target)
    {
        List<AbstractType<?>> types = new ArrayList<>();
        // All partition and clustering columns are included in the index, whether they are specified in the denormalized columns or not
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
