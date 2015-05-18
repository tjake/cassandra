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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.GlobalIndexDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.CFRowAdder;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.ExpiringCell;
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
import org.apache.cassandra.db.index.global.GlobalIndexBuilder;
import org.apache.cassandra.db.index.global.GlobalIndexSelector;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.service.StorageProxy;

public class GlobalIndex implements Index
{
    // This is a class that allows comparisons based on partition key and clustering columns, and resolves existing and
    // new mutation values
    private static class MutationUnit
    {
        private final ColumnFamilyStore baseCfs;
        private final ByteBuffer partitionKey;
        private final Map<ColumnIdentifier, ByteBuffer> clusteringColumns;
        private final Map<ColumnIdentifier, Map<CellName, SortedMap<Long, Cell>>> columnValues = new HashMap<>();
        private int ttl = 0;

        MutationUnit(ColumnFamilyStore baseCfs, ByteBuffer key, Map<ColumnIdentifier, ByteBuffer> clusteringColumns)
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

        public void addColumnValue(Cell cell)
        {
            CellName cellName = cell.name();
            ColumnIdentifier identifier = cellName.cql3ColumnName(baseCfs.metadata);
            if (!columnValues.containsKey(identifier))
                columnValues.put(identifier, new HashMap<CellName, SortedMap<Long, Cell>>());
            Map<CellName, SortedMap<Long, Cell>> innerMap = columnValues.get(identifier);
            if (!innerMap.containsKey(cellName))
                innerMap.put(cellName, new TreeMap<Long, Cell>());
            innerMap.get(cellName).put(cell.timestamp(), cell);
        }

        public Collection<Cell> oldValueIfUpdated(ColumnIdentifier columnIdentifier)
        {
            if (!columnValues.containsKey(columnIdentifier))
                return Collections.emptyList();

            Map<CellName, SortedMap<Long, Cell>> cellNames = columnValues.get(columnIdentifier);
            List<Cell> values = new ArrayList<>();
            for (SortedMap<Long, Cell> cells: cellNames.values())
            {
                if (cells.size() < 2)
                    continue;

                Cell initial = cells.get(cells.firstKey());
                Cell value = initial;
                for (Cell cell: cells.values())
                    value = value.reconcile(cell);

                if (initial.value().compareTo(value.value()) != 0)
                    values.add(initial);
            }
            return values;
        }

        public Collection<Cell> newValueIfUpdated(ColumnIdentifier columnIdentifier)
        {
            if (!columnValues.containsKey(columnIdentifier))
                return Collections.emptyList();

            Map<CellName, SortedMap<Long, Cell>> cellNames = columnValues.get(columnIdentifier);
            List<Cell> values = new ArrayList<>();
            for (SortedMap<Long, Cell> cells: cellNames.values())
            {
                Cell initial = cells.get(cells.firstKey());
                Cell value = initial;
                for (Cell cell: cells.values())
                    value = value.reconcile(cell);

                if (cells.size() == 1 || initial.value().compareTo(value.value()) != 0)
                    values.add(initial);
            }
            return values;
        }

        public ByteBuffer primaryKeyValue(ColumnDefinition definition)
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
            }
            return null;
        }

        public Collection<Cell> value(ColumnIdentifier identifier)
        {
            if (!columnValues.containsKey(identifier))
                return Collections.emptyList();

            Map<CellName, SortedMap<Long, Cell>> cellNames = columnValues.get(identifier);
            List<Cell> values = new ArrayList<>();
            for (SortedMap<Long, Cell> cells: cellNames.values())
            {
                Cell value = cells.get(cells.firstKey());
                for (Cell cell: cells.values())
                    value = value.reconcile(cell);

                values.add(value);
            }
            return values;
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

    private synchronized void createIndexCfsAndSelectors()
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
        createIndexCfsAndSelectors();

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
        return !mutationUnit.oldValueIfUpdated(target.name).isEmpty();
    }

    private Mutation createTombstone(MutationUnit mutationUnit, long timestamp)
    {
        // Need to generate a tombstone in this case; there will be only one element because we do not allow Collections
        // for keys of a global index.
        Collection<Cell> oldValue = mutationUnit.oldValueIfUpdated(target.name);
        if (oldValue.isEmpty())
            return null;

        Cell partitionKey = Iterables.getOnlyElement(oldValue);

        Mutation mutation = new Mutation(indexCfs.metadata.ksName, partitionKey.value());
        ColumnFamily indexCf = mutation.addOrGet(indexCfs.metadata);
        CellNameType cellNameType = indexCfs.getComparator();
        if (cellNameType.isCompound())
        {
            CBuilder builder = cellNameType.prefixBuilder();
            for (ColumnDefinition definition: clusteringKeys)
                builder = builder.add(mutationUnit.primaryKeyValue(definition));
            Composite cellName = builder.build();
            RangeTombstone rt = new RangeTombstone(cellName.start(), cellName.end(), timestamp, Integer.MAX_VALUE);
            indexCf.addAtom(rt);
        }
        else
        {
            assert clusteringKeys.size() == 1;
            CellName cellName = cellNameType.cellFromByteBuffer(mutationUnit.primaryKeyValue(clusteringKeys.get(0)));
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

        Mutation mutation = createTombstone(mutationUnit, timestamp);
        if (mutation != null)
            return Collections.singleton(mutation);
        return null;
    }

    private Collection<Mutation> createMutationsForInserts(MutationUnit mutationUnit, long timestamp, boolean tombstonesGenerated)
    {
        ByteBuffer partitionKey = target.isPrimaryKeyColumn()
                                  ? mutationUnit.primaryKeyValue(target)
                                  : Iterables.getOnlyElement(mutationUnit.value(target.name)).value();

        if (partitionKey == null)
        {
            // Not having a partition key means we aren't updating anything
            return null;
        }

        ByteBuffer[] clusteringColumns = new ByteBuffer[clusteringKeys.size()];

        for (int i = 0; i < clusteringColumns.length; i++)
        {
            clusteringColumns[i] = mutationUnit.primaryKeyValue(clusteringKeys.get(i));
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

        for (int i = 0; i < regularColumns.size(); i++)
        {
            ColumnDefinition def = this.regularColumns.get(i);
            Collection<Cell> cells = tombstonesGenerated
                                     ? mutationUnit.value(def.name)
                                     : mutationUnit.newValueIfUpdated(def.name);

            for (Cell cell: cells)
            {
                if (cell.name().isCollectionCell())
                    rowAdder.addCollectionEntry(def.name.toString(), cell.name().collectionElement(), cell.value());
                else
                    rowAdder.add(def.name.toString(), cell.value());
            }
        }

        for (int i = 0; i < staticColumns.size(); i++)
        {
            ColumnDefinition def = this.staticColumns.get(i);
            Collection<Cell> cells = tombstonesGenerated
                                     ? mutationUnit.value(def.name)
                                     : mutationUnit.newValueIfUpdated(def.name);

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
                        previous.addColumnValue(cell);
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
                    Mutation mutation = createTombstone(mutationUnit, timestamp);
                    if (mutation != null)
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

    private void query(ByteBuffer key, Collection<MutationUnit> mutationUnits, ConsistencyLevel consistency)
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
                                                                                        Long.MAX_VALUE,
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
                    previous.addColumnValue(cell);
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
            mutationUnit.addColumnValue(cell);
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
            query(key, mutationUnits, consistency);

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
        CompactionManager.instance.submitGlobalIndexBuilder(builder);
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

        CellNameType comparator = getIndexComparator(baseCf, target, definition.included);
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

    public static CellNameType getIndexComparator(CFMetaData baseCFMD, ColumnDefinition target, Collection<ColumnIdentifier> included)
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
