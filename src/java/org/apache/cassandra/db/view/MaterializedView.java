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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.MaterializedViewDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.CFProperties;
import org.apache.cassandra.db.AbstractReadCommandBuilder.SinglePartitionSliceBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadOrderGroup;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.partitions.AbstractPartitionData;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class MaterializedView
{
    private final MaterializedViewDefinition definition;
    public final String name;

    final ColumnFamilyStore baseCfs;
    public final ColumnFamilyStore viewCfs;
    public final List<ColumnDefinition> primaryKeyDefs;
    public final List<ColumnDefinition> targetDefs;
    public final boolean targetHasAllPrimaryKeyColumns;
    MaterializedViewBuilder builder;

    public MaterializedView(MaterializedViewDefinition definition,
                            ColumnFamilyStore baseCfs)
    {
        this.definition = definition;
        this.baseCfs = baseCfs;

        name = definition.viewName;

        primaryKeyDefs = new ArrayList<>(definition.partitionColumns.size());
        targetDefs = new ArrayList<>(definition.partitionColumns.size()
                                     + definition.clusteringColumns.size());
        boolean nonPrimaryKeyCol = false;
        for (ColumnIdentifier identifier : definition.partitionColumns)
        {
            ColumnDefinition cdef = baseCfs.metadata.getColumnDefinition(identifier);
            assert cdef != null;
            primaryKeyDefs.add(cdef);
            targetDefs.add(cdef);

            if(!cdef.isPrimaryKeyColumn())
                nonPrimaryKeyCol = true;
        }

        for (ColumnIdentifier identifier: definition.clusteringColumns)
        {
            ColumnDefinition cdef = baseCfs.metadata.getColumnDefinition(identifier);
            assert cdef != null;
            targetDefs.add(cdef);

            if(!cdef.isPrimaryKeyColumn())
                nonPrimaryKeyCol = true;
        }

        targetHasAllPrimaryKeyColumns = !nonPrimaryKeyCol;
        CFMetaData viewCfm = Schema.instance.getCFMetaData(baseCfs.metadata.ksName, definition.viewName);
        viewCfs = Schema.instance.getColumnFamilyStoreInstance(viewCfm.cfId);
    }

    /**
     * Check to see if any value that is part of the view is updated. If so, we possibly need to mutate the view.
     *
     * @param upd Column family to check for selected values with
     * @return True if any of the selected values are contained in the column family.
     */
    public boolean cfModifiesSelectedColumn(AbstractPartitionData upd)
    {
        // If we are including all of the columns, then any non-empty column family will need to be selected
        if (definition.included.isEmpty())
            return true;

        // If there are range tombstones, tombstones will also need to be generated for the materialized view
        if (!upd.deletionInfo().isLive())
            return true;

        Iterator<Row> rowIterator = upd.iterator();

        while (rowIterator.hasNext())
        {
            Row row = rowIterator.next();

            Iterator<Cell> cellIterator = row.iterator();
            while (cellIterator.hasNext())
            {
                if (viewCfs.metadata.getColumnDefinition(cellIterator.next().column().name) != null)
                    return true;
            }
        }

        return false;
    }

    /**
     * Creates a Mutation containing a range tombstone for a base key and LiveRowState.
     */
    private Mutation createTombstone(LiveRowState liveRowState,
                                     DecoratedKey partitionKey,
                                     long timestamp,
                                     LiveRowState.Resolver resolver)
    {
        int numViewClustering = viewCfs.metadata.clusteringColumns().size();

        Object[] viewClusteringValues = new Object[numViewClustering];

        for (int i = 0; i < numViewClustering; i++)
        {
            ColumnDefinition definition = viewCfs.metadata.clusteringColumns().get(i);
            viewClusteringValues[i] = liveRowState.clusteringValue(definition, resolver);
        }

        return RowUpdateBuilder.deleteRow(viewCfs.metadata, timestamp, partitionKey, viewClusteringValues);
    }

    /**
     * Creates a Mutation containing a complex tombstone for a base key, a LiveRowState, and a collection identifier.
     */
    private Mutation createComplexTombstone(LiveRowState liveRowState,
                                            DecoratedKey partitionKey,
                                            ColumnDefinition deletedColumn,
                                            long timestamp,
                                            LiveRowState.Resolver resolver)
    {
        int numViewClustering = viewCfs.metadata.clusteringColumns().size();

        Object[] viewClusteringValues = new Object[numViewClustering];

        for (int i = 0; i < numViewClustering; i++)
        {
            ColumnDefinition definition = viewCfs.metadata.clusteringColumns().get(i);
            viewClusteringValues[i] = liveRowState.clusteringValue(definition, resolver);
        }

        return new RowUpdateBuilder(viewCfs.metadata, timestamp, partitionKey)
               .clustering(viewClusteringValues)
               .resetCollection(deletedColumn)
               .build();
    }

    /**
     * Creates the DecoratedKey for the view's partition key given a LiveRowState.
     * @return View's DecoratedKey or null, if one of hte view's primary key components has an invalid resolution from
     *         the LiveRowState and its Resolver
     */
    private DecoratedKey targetPartitionKey(LiveRowState liveRowState, LiveRowState.Resolver resolver)
    {
        Object[] partitionKey = new Object[primaryKeyDefs.size()];

        for (int i = 0; i < partitionKey.length; i++)
        {
            ByteBuffer value = liveRowState.clusteringValue(primaryKeyDefs.get(i), resolver);

            if (value == null)
                return null;

            partitionKey[i] = value;
        }

        return viewCfs
               .partitioner
               .decorateKey(CFMetaData
                            .serializePartitionKey(viewCfs
                                                   .metadata
                                                   .getKeyValidatorAsClusteringComparator()
                                                   .make(partitionKey)));
    }


    private Mutation createPartitionTombstonesForUpdates(LiveRowState liveRowState, long timestamp)
    {
        // Primary Key and Clustering columns do not generate tombstones
        if (targetHasAllPrimaryKeyColumns)
            return null;

        boolean hasUpdate = false;
        for (ColumnDefinition target : targetDefs)
        {
            if (!target.isPrimaryKeyColumn() && liveRowState.clusteringValue(target, LiveRowState.oldValueIfUpdated) != null)
                hasUpdate = true;
        }

        if (!hasUpdate)
            return null;

        LiveRowState.Resolver resolver = LiveRowState.earliest;
        Mutation mutation = createTombstone(liveRowState, targetPartitionKey(liveRowState, resolver), timestamp, resolver);
        if (mutation != null)
            return mutation;

        return null;
    }

    private Mutation createMutationsForInserts(LiveRowState liveRowState, long timestamp, boolean tombstonesGenerated)
    {
        DecoratedKey partitionKey = targetPartitionKey(liveRowState, LiveRowState.latest);
        if (partitionKey == null)
        {
            // Not having a partition key means we aren't updating anything
            return null;
        }

        LiveRowState.Resolver resolver = tombstonesGenerated
                                         ? LiveRowState.latest
                                         : LiveRowState.newValueIfUpdated;

        RowUpdateBuilder builder = new RowUpdateBuilder(viewCfs.metadata, timestamp, liveRowState.ttl, partitionKey);
        int nowInSec = FBUtilities.nowInSeconds();

        Object[] clustering = new Object[viewCfs.metadata.clusteringColumns().size()];
        for (int i = 0; i < clustering.length; i++)
        {
            clustering[i] = liveRowState.clusteringValue(viewCfs.metadata.clusteringColumns().get(i), resolver);
        }
        builder.clustering(clustering);

        for (ColumnDefinition columnDefinition : viewCfs.metadata.allColumns())
        {
            if (columnDefinition.isPrimaryKeyColumn())
                continue;

            for (Cell cell : liveRowState.values(columnDefinition, resolver, timestamp))
            {
                if (columnDefinition.isComplex())
                {
                    if (cell.isTombstone())
                        builder.addComplex(columnDefinition, cell.path(), ByteBufferUtil.EMPTY_BYTE_BUFFER, cell.livenessInfo());
                    else
                        builder.addComplex(columnDefinition, cell.path(), cell.isLive(nowInSec) ? cell.value() : null, cell.livenessInfo());
                }
                else
                {
                    builder.add(columnDefinition, cell.isLive(nowInSec) ? cell.value() : null, cell.livenessInfo());
                }
            }
        }

        return builder.build();
    }


    private Collection<Mutation> createForDeletionInfo(LiveRowState.Set mutationUnits, ByteBuffer key, AbstractPartitionData upd, ConsistencyLevel consistency)
    {
        final LiveRowState.Resolver resolver = LiveRowState.earliest;

        DeletionInfo deletionInfo = upd.deletionInfo();


        List<Mutation> mutations = new ArrayList<>();

        if (baseCfs.metadata.hasComplexColumns())
        {
            Iterator<Row> rowIterator = upd.iterator();

            while (rowIterator.hasNext())
            {
                Row row = rowIterator.next();

                if (!row.hasComplexDeletion())
                    continue;

                LiveRowState liveRowState = mutationUnits.getExistingUnit(key, row);

                assert liveRowState != null;

                for (ColumnDefinition definition : baseCfs.metadata.allColumns())
                {
                    if (definition.isComplex())
                    {
                        DeletionTime time = row.getDeletion(definition);
                        if (!time.isLive())
                        {
                            DecoratedKey targetKey = targetPartitionKey(liveRowState, resolver);
                            if (targetKey != null)
                                mutations.add(createComplexTombstone(liveRowState, targetKey, definition, upd.maxTimestamp(), resolver));
                        }
                    }
                }
            }
        }


        if (deletionInfo.hasRanges() || deletionInfo.getPartitionDeletion().markedForDeleteAt() != Long.MIN_VALUE)
        {
            ReadCommand command;
            DecoratedKey dk = baseCfs.partitioner.decorateKey(key);

            long timestamp;
            if (deletionInfo.hasRanges())
            {
                SinglePartitionSliceBuilder builder = new SinglePartitionSliceBuilder(baseCfs, dk);
                Iterator<RangeTombstone> tombstones = deletionInfo.rangeIterator(false);
                timestamp = Long.MIN_VALUE;
                while (tombstones.hasNext())
                {
                    RangeTombstone tombstone = tombstones.next();

                    builder.addSlice(tombstone.deletedSlice());
                    timestamp = Math.max(timestamp, tombstone.deletionTime().markedForDeleteAt());
                }

                if (!mutations.isEmpty())
                    return mutations;

                command = builder.build();
            }
            else
            {
                timestamp = deletionInfo.getPartitionDeletion().markedForDeleteAt();
                command = SinglePartitionReadCommand.fullPartitionRead(baseCfs.metadata, FBUtilities.nowInSeconds(), dk);
            }

            QueryPager pager = command.getPager(null);

            while (!pager.isExhausted())
            {
                try (ReadOrderGroup orderGroup = pager.startOrderGroup();
                     PartitionIterator iter = pager.fetchPageInternal(128, orderGroup))
                {
                    if (!iter.hasNext())
                        break;

                    try (RowIterator rowIterator = iter.next())
                    {
                        while (rowIterator.hasNext())
                        {
                            Row row = rowIterator.next();
                            mutationUnits.addUnit(key, row, false);
                        }
                    }
                }
            }
            
            for (LiveRowState liveRowState : mutationUnits)
            {
                DecoratedKey value = targetPartitionKey(liveRowState, resolver);
                if (value != null)
                {
                    Mutation mutation = createTombstone(liveRowState, value, timestamp, resolver);
                    if (mutation != null)
                        mutations.add(mutation);
                }
            }
        }

        return !mutations.isEmpty() ? mutations : null;
    }

    private void query(ByteBuffer key, LiveRowState.Set mutationUnits)
    {
        SinglePartitionSliceBuilder builder = new SinglePartitionSliceBuilder(baseCfs, baseCfs.partitioner.decorateKey(key));

        for (LiveRowState liveRowState : mutationUnits)
            builder.addSlice(liveRowState.baseSlice());

        QueryPager pager = builder.build().getPager(null);

        while (!pager.isExhausted())
        {
            try (ReadOrderGroup orderGroup = pager.startOrderGroup();
                 PartitionIterator iter = pager.fetchPageInternal(128, orderGroup))
            {
                while (iter.hasNext())
                {
                    RowIterator rows = iter.next();
                    while (rows.hasNext())
                    {
                        mutationUnits.addUnit(key, rows.next(), false);
                    }
                }
            }
        }
    }

    private LiveRowState.Set separateMutationUnits(ByteBuffer key, AbstractPartitionData upd)
    {
        LiveRowState.Set mutationUnits = new LiveRowState.Set(baseCfs);

        // For each cell name, we need to grab the clustering columns
        Iterator<Row> rows = upd.iterator();
        while (rows.hasNext())
        {
            mutationUnits.addUnit(key, rows.next(), true);
        }

        return mutationUnits;
    }

    public Collection<Mutation> createMutations(ByteBuffer key, AbstractPartitionData upd, ConsistencyLevel consistency, boolean isBuilding)
    {
        if (!cfModifiesSelectedColumn(upd))
        {
            return null;
        }

        LiveRowState.Set mutationUnits = separateMutationUnits(key, upd);

        // If we are building the view, we do not want to add old values; they will always be the same
        if (!isBuilding)
            query(key, mutationUnits);

        Collection<Mutation> mutations = null;

        for (LiveRowState liveRowState : mutationUnits)
        {
            boolean tombstonesInserted = false;

            if (!isBuilding)
            {
                Mutation partitionTombstone = createPartitionTombstonesForUpdates(liveRowState, upd.maxTimestamp());
                tombstonesInserted = partitionTombstone != null;
                if (tombstonesInserted)
                {
                    if (mutations == null) mutations = new LinkedList<>();
                    mutations.add(partitionTombstone);
                }
            }

            Mutation insert = createMutationsForInserts(liveRowState, upd.maxTimestamp(), tombstonesInserted);
            if (insert != null)
            {
                if (mutations == null) mutations = new LinkedList<>();
                mutations.add(insert);
            }
        }

        if (!isBuilding)
        {
            Collection<Mutation> deletion = createForDeletionInfo(mutationUnits, key, upd, consistency);
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

        this.builder = new MaterializedViewBuilder(baseCfs, this);
        CompactionManager.instance.submitMaterializedViewBuilder(builder);
    }

    public void reload()
    {
        build();
    }

    /**
     * @return CFMetaData which represents the definition given
     */
    public static CFMetaData getCFMetaData(MaterializedViewDefinition definition,
                                           CFMetaData baseCf,
                                           CFProperties properties)
    {
        CFMetaData.Builder viewBuilder = CFMetaData.Builder
                                         .create(baseCf.ksName, definition.viewName);

        ColumnDefinition nonPkTarget = null;

        for (ColumnIdentifier targetIdentifier : definition.partitionColumns)
        {
            ColumnDefinition target = baseCf.getColumnDefinition(targetIdentifier);
            if (!target.isPartitionKey())
                nonPkTarget = target;

            viewBuilder.addPartitionKey(target.name, properties.getReversableType(targetIdentifier, target.type));
        }

        Collection<ColumnDefinition> included = new ArrayList<>();
        for(ColumnIdentifier identifier : definition.included)
        {
            ColumnDefinition cfDef = baseCf.getColumnDefinition(identifier);
            assert cfDef != null;
            included.add(cfDef);
        }

        boolean includeAll = included.isEmpty();

        for (ColumnIdentifier ident : definition.clusteringColumns)
        {
            ColumnDefinition column = baseCf.getColumnDefinition(ident);
            viewBuilder.addClusteringColumn(ident, properties.getReversableType(ident, column.type));
        }

        for (ColumnDefinition column : baseCf.partitionColumns().regulars.columns)
        {
            if (column != nonPkTarget && (includeAll || included.contains(column)))
            {
                viewBuilder.addRegularColumn(column.name, column.type);
            }
        }

        for (ColumnDefinition column : baseCf.partitionColumns().statics.columns)
        {
            if (column != nonPkTarget && (includeAll || included.contains(column)))
            {
                viewBuilder.addStaticColumn(column.name, column.type);
            }
        }

        //Add any extra clustering columns
        for (ColumnDefinition column : Iterables.concat(baseCf.partitionKeyColumns(), baseCf.clusteringColumns()))
        {
            if ( (!definition.partitionColumns.contains(column.name) && !definition.clusteringColumns.contains(column.name)) &&
                 (includeAll || included.contains(column)) )
            {
                viewBuilder.addRegularColumn(column.name, column.type);
            }
        }

        CFMetaData cfm = viewBuilder.build();
        properties.properties.applyToCFMetadata(cfm);

        return cfm;
    }
}
