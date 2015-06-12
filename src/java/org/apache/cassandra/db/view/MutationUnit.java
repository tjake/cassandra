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
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Conflicts;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

// This is a class that allows comparisons based on partition key and clustering columns, and resolves existing and
// new mutation values
public class MutationUnit
{
    public static final Resolver oldValueIfUpdated = new Resolver()
    {
        public MUCell resolve(Iterable<MUCell> cells)
        {
            Iterator<MUCell> iterator = cells.iterator();
            if (!iterator.hasNext())
                return null;

            MUCell initial = iterator.next();
            if (initial.isNew || !iterator.hasNext())
                return null;

            MUCell value = initial;
            while (iterator.hasNext())
                value = value.reconcile(iterator.next());

            return ByteBufferUtil.compareUnsigned(initial.value, value.value) != 0 ? initial : null;
        }
    };

    public static final Resolver newValueIfUpdated = new Resolver()
    {
        public MUCell resolve(Iterable<MUCell> cells)
        {
            Iterator<MUCell> iterator = cells.iterator();
            if (!iterator.hasNext())
                return null;
            MUCell initial = iterator.next();
            if (!iterator.hasNext())
                return initial;

            MUCell value = initial;
            while (iterator.hasNext())
                value = value.reconcile(iterator.next());

            return value.isNew && ByteBufferUtil.compareUnsigned(initial.value, value.value) != 0
                   ? value
                   : null;
        }
    };

    public static final Resolver earliest = new Resolver()
    {
        public MUCell resolve(Iterable<MUCell> cells)
        {
            Iterator<MUCell> iterator = cells.iterator();
            if (!iterator.hasNext())
                return null;
            return iterator.next();
        }
    };

    public static final Resolver latest = new Resolver()
    {
        public MUCell resolve(Iterable<MUCell> cells)
        {
            Iterator<MUCell> iterator = cells.iterator();
            if (!iterator.hasNext())
                return null;
            MUCell value = iterator.next();
            while (iterator.hasNext())
                value = value.reconcile(iterator.next());

            return value;
        }
    };

    public Clustering viewClustering(ClusteringComparator comparator, List<ColumnDefinition> clusteringColumns, Resolver resolver)
    {
        Object[] clusterings = new Object[clusteringColumns.size()];

        for (int i = 0; i < clusterings.length; i++)
        {
            clusterings[i] = clusteringValue(clusteringColumns.get(i), resolver);
        }

        return comparator.make(clusterings);
    }

    public Clustering baseClustering(Resolver resolver)
    {
        Object[] clusterings = new Object[baseCfs.metadata.clusteringColumns().size()];

        for (int i = 0; i < clusterings.length; i++)
        {
            clusterings[i] = clusteringValue(baseCfs.metadata.clusteringColumns().get(i), resolver);
        }

        return baseCfs.getComparator().make(clusterings);
    }

    private interface PrivateResolver
    {
        MUCell resolve(Iterable<MUCell> cells);
    }

    private static class MUCell
    {
        public final ByteBuffer value;
        private final LivenessInfo liveness;
        public final boolean isNew;
        private MUCell(Cell cell, boolean isNew)
        {
            this.value = cell.value();
            this.liveness = cell.livenessInfo();
            this.isNew = isNew;
        }

        public MUCell reconcile(MUCell cell)
        {
            int now = FBUtilities.nowInSeconds();
            Conflicts.Resolution resolution = Conflicts.resolveRegular(cell.liveness.timestamp(),
                                     cell.liveness.isLive(now),
                                     cell.liveness.localDeletionTime(),
                                     cell.value,
                                     this.liveness.timestamp(),
                                     this.liveness.isLive(now),
                                     this.liveness.localDeletionTime(),
                                     this.value);
            assert resolution != Conflicts.Resolution.MERGE;
            if (resolution == Conflicts.Resolution.LEFT_WINS)
                return cell;
            return this;
        }
    }

    public interface Resolver extends PrivateResolver
    {
    }

    final ColumnFamilyStore baseCfs;
    private final ByteBuffer partitionKey;
    private final Map<ColumnIdentifier, ByteBuffer> clusteringColumns;
    private final Map<ColumnIdentifier, Map<CellPath, SortedMap<Long, MUCell>>> columnValues = new HashMap<>();
    public int ttl;

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

    public void addColumnValue(Clustering clustering, Cell cell, boolean isNew)
    {
        ColumnIdentifier identifier = cell.column().name;

        if (!columnValues.containsKey(identifier))
            columnValues.put(identifier, new HashMap<>());

        Map<CellPath, SortedMap<Long, MUCell>> innerMap = columnValues.get(identifier);

        CellPath path = cell.path();
        if (!innerMap.containsKey(path))
            innerMap.put(path, new TreeMap<>());

        if (cell.isExpiring())
            ttl = Math.max(cell.livenessInfo().ttl(), ttl);

        innerMap.get(path).put(cell.livenessInfo().timestamp(), new MUCell(cell, isNew));
    }

    // The Definition here is actually the *base table* definition
    public ByteBuffer clusteringValue(ColumnDefinition definition, Resolver resolver)
    {
        ColumnDefinition baseDefinition = definition.cfName.equals(baseCfs.name)
                                          ? definition
                                          : baseCfs.metadata.getColumnDefinition(definition.name);

        if (baseDefinition.isPartitionKey())
        {
            if (baseDefinition.isOnAllComponents())
                return partitionKey;
            else
            {
                CompositeType keyComparator = (CompositeType) baseCfs.metadata.getKeyValidator();
                ByteBuffer[] components = keyComparator.split(partitionKey);
                return components[baseDefinition.position()];
            }
        }
        else
        {
            ColumnIdentifier columnIdentifier = baseDefinition.name;

            if (clusteringColumns.containsKey(columnIdentifier))
                return clusteringColumns.get(columnIdentifier);

            Collection<Cell> val = values(definition, resolver);
            if (val != null && val.size() == 1)
                return Iterables.getOnlyElement(val).value();
        }
        return null;
    }

    public Collection<Cell> values(ColumnDefinition definition, Resolver resolver)
    {
        Map<CellPath, SortedMap<Long, MUCell>> innerMap = columnValues.get(definition.name);
        if (innerMap == null)
            return Collections.emptyList();

        Collection<Cell> value = new ArrayList<>();
        for (Map.Entry<CellPath, SortedMap<Long, MUCell>> pathAndCells: innerMap.entrySet())
        {
            MUCell cell = resolver.resolve(pathAndCells.getValue().values());
            if (cell != null)
                value.add(new Cell()
                {
                    public ColumnDefinition column()
                    {
                        return definition;
                    }

                    public boolean isCounterCell()
                    {
                        return false;
                    }

                    public ByteBuffer value()
                    {
                        return cell.value;
                    }

                    public LivenessInfo livenessInfo()
                    {
                        return cell.liveness;
                    }

                    public boolean isTombstone()
                    {
                        return false;
                    }

                    public boolean isExpiring()
                    {
                        return false;
                    }

                    public boolean isLive(int nowInSec)
                    {
                        return cell.liveness.isLive(nowInSec);
                    }

                    public CellPath path()
                    {
                        return pathAndCells.getKey();
                    }

                    public void writeTo(Row.Writer writer)
                    {

                    }

                    public void digest(MessageDigest digest)
                    {

                    }

                    public void validate()
                    {

                    }

                    public int dataSize()
                    {
                        return cell.value.remaining();
                    }

                    public Cell takeAlias()
                    {
                        return null;
                    }
                });
        }
        return value;
    }

    public Slice baseSlice()
    {
        CFMetaData metadata = baseCfs.metadata;
        CBuilder builder = CBuilder.create(metadata.comparator);

        ByteBuffer[] buffers = new ByteBuffer[clusteringColumns.size()];
        for (Map.Entry<ColumnIdentifier, ByteBuffer> buffer : clusteringColumns.entrySet())
            buffers[metadata.getColumnDefinition(buffer.getKey()).position()] = buffer.getValue();

        for (ByteBuffer byteBuffer : buffers)
            builder = builder.add(byteBuffer);

        return builder.buildSlice();
    }

    static class Set implements Iterable<MutationUnit>
    {
        private final ColumnFamilyStore baseCfs;
        private final Map<MutationUnit, MutationUnit> mutationUnits = new HashMap<>();

        Set(ColumnFamilyStore baseCfs)
        {
            this.baseCfs = baseCfs;
        }

        Set(MutationUnit single)
        {
            this(single.baseCfs);
            mutationUnits.put(single, single);
        }

        public Iterator<MutationUnit> iterator()
        {
            return mutationUnits.values().iterator();
        }

        public void addUnit(ByteBuffer key, Clustering clustering, Cell cell, boolean isNew)
        {
            Map<ColumnIdentifier, ByteBuffer> clusteringColumns = new HashMap<>();
            for (ColumnDefinition columnDefinition: baseCfs.metadata.clusteringColumns())
            {
                clusteringColumns.put(columnDefinition.name, cell.value());
            }

            MutationUnit mutationUnit = new MutationUnit(baseCfs, key, clusteringColumns);
            if (mutationUnits.containsKey(mutationUnit))
            {
                mutationUnit = mutationUnits.get(mutationUnit);
            }
            else
            {
                mutationUnits.put(mutationUnit, mutationUnit);
            }

            mutationUnit.addColumnValue(clustering, cell, isNew);
        }

        public int size()
        {
            return mutationUnits.size();
        }
    }
}
