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
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Conflicts;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

// This is a class that allows comparisons based on partition key and clustering columns, and resolves existing and
// new mutation values
public class TemporalRow
{
    public interface Resolver
    {
        TemporalCell resolve(Iterable<TemporalCell> cells);
    }

    public static final Resolver oldValueIfUpdated = new Resolver()
    {
        public TemporalCell resolve(Iterable<TemporalCell> cells)
        {
            Iterator<TemporalCell> iterator = cells.iterator();
            if (!iterator.hasNext())
                return null;

            TemporalCell initial = iterator.next();
            if (initial.isNew || !iterator.hasNext())
                return null;

            TemporalCell value = initial;
            while (iterator.hasNext())
                value = value.reconcile(iterator.next());

            return ByteBufferUtil.compareUnsigned(initial.value, value.value) != 0 ? initial : null;
        }
    };

    public static final Resolver newValueIfUpdated = new Resolver()
    {
        public TemporalCell resolve(Iterable<TemporalCell> cells)
        {
            Iterator<TemporalCell> iterator = cells.iterator();
            if (!iterator.hasNext())
                return null;
            TemporalCell initial = iterator.next();
            if (!iterator.hasNext())
                return initial;

            TemporalCell value = initial;
            while (iterator.hasNext())
                value = value.reconcile(iterator.next());

            return value.isNew ? value : null;
        }
    };

    public static final Resolver earliest = new Resolver()
    {
        public TemporalCell resolve(Iterable<TemporalCell> cells)
        {
            Iterator<TemporalCell> iterator = cells.iterator();
            if (!iterator.hasNext())
                return null;
            return iterator.next();
        }
    };

    public static final Resolver latest = new Resolver()
    {
        public TemporalCell resolve(Iterable<TemporalCell> cells)
        {
            Iterator<TemporalCell> iterator = cells.iterator();
            if (!iterator.hasNext())
                return null;
            TemporalCell value = iterator.next();
            while (iterator.hasNext())
                value = value.reconcile(iterator.next());

            return value;
        }
    };

    private static class TemporalCell
    {
        public final ByteBuffer value;
        private final LivenessInfo liveness;
        public final boolean isNew;

        private TemporalCell(ByteBuffer value, LivenessInfo liveness, boolean isNew)
        {
            this.value = value;
            this.liveness = liveness;
            this.isNew = isNew;
        }

        public TemporalCell reconcile(TemporalCell that)
        {
            int now = FBUtilities.nowInSeconds();
            Conflicts.Resolution resolution = Conflicts.resolveRegular(that.liveness.timestamp(),
                                                                       that.liveness.isLive(now),
                                                                       that.liveness.localDeletionTime(),
                                                                       that.value,
                                                                       this.liveness.timestamp(),
                                                                       this.liveness.isLive(now),
                                                                       this.liveness.localDeletionTime(),
                                                                       this.value);
            assert resolution != Conflicts.Resolution.MERGE;
            if (resolution == Conflicts.Resolution.LEFT_WINS)
                return that;
            return this;
        }
    }

    final ColumnFamilyStore baseCfs;
    private final ByteBuffer basePartitionKey;
    public final Map<ColumnIdentifier, ByteBuffer> clusteringColumns;
    private final Map<ColumnIdentifier, Map<CellPath, SortedMap<Long, TemporalCell>>> columnValues = new HashMap<>();
    public int ttl;

    TemporalRow(ColumnFamilyStore baseCfs, ByteBuffer key, Row row, boolean isNew)
    {
        this.baseCfs = baseCfs;
        this.basePartitionKey = key;
        clusteringColumns = new HashMap<>();

        List<ColumnDefinition> clusteringDefs = baseCfs.metadata.clusteringColumns();
        for (int i = 0; i < clusteringDefs.size(); i++)
        {
            ColumnDefinition cdef = clusteringDefs.get(i);
            clusteringColumns.put(cdef.name, row.clustering().get(i));

            addColumnValue(cdef.name, null, row.primaryKeyLivenessInfo(), row.clustering().get(i), isNew);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TemporalRow that = (TemporalRow) o;

        if (!clusteringColumns.equals(that.clusteringColumns)) return false;
        if (!basePartitionKey.equals(that.basePartitionKey)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = basePartitionKey.hashCode();
        result = 31 * result + clusteringColumns.hashCode();
        return result;
    }

    public void addColumnValue(ColumnIdentifier identifier, CellPath cellPath, LivenessInfo liveness, ByteBuffer value,  boolean isNew)
    {
        if (!columnValues.containsKey(identifier))
            columnValues.put(identifier, new HashMap<>());

        Map<CellPath, SortedMap<Long, TemporalCell>> innerMap = columnValues.get(identifier);

        if (!innerMap.containsKey(cellPath))
            innerMap.put(cellPath, new TreeMap<>());

        if (liveness.hasTTL())
            ttl = Math.max(liveness.ttl(), ttl);

        innerMap.get(cellPath).put(liveness.timestamp(), new TemporalCell(value, liveness, isNew));
    }

    public void addColumnValue(org.apache.cassandra.db.rows.Cell cell, boolean isNew)
    {
        addColumnValue(cell.column().name, cell.path(), cell.livenessInfo(), cell.value(), isNew);
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
                return basePartitionKey;
            else
            {
                CompositeType keyComparator = (CompositeType) baseCfs.metadata.getKeyValidator();
                ByteBuffer[] components = keyComparator.split(basePartitionKey);
                return components[baseDefinition.position()];
            }
        }
        else
        {
            ColumnIdentifier columnIdentifier = baseDefinition.name;

            if (clusteringColumns.containsKey(columnIdentifier))
                return clusteringColumns.get(columnIdentifier);

            Collection<org.apache.cassandra.db.rows.Cell> val = values(definition, resolver, 1L);
            if (val != null && val.size() == 1)
                return Iterables.getOnlyElement(val).value();
        }
        return null;
    }

    public Collection<org.apache.cassandra.db.rows.Cell> values(ColumnDefinition definition, Resolver resolver, final long newTimeStamp)
    {
        Map<CellPath, SortedMap<Long, TemporalCell>> innerMap = columnValues.get(definition.name);
        if (innerMap == null)
        {

            return Collections.emptyList();
        }

        Collection<org.apache.cassandra.db.rows.Cell> value = new ArrayList<>();
        for (Map.Entry<CellPath, SortedMap<Long, TemporalCell>> pathAndCells : innerMap.entrySet())
        {
            TemporalCell cell = resolver.resolve(pathAndCells.getValue().values());


            if (cell != null)
            {
                final LivenessInfo liveness = cell.isNew ? cell.liveness.withUpdatedTimestamp(newTimeStamp)
                                                         : cell.liveness.withUpdatedTimestamp(newTimeStamp - 1);

                value.add(new org.apache.cassandra.db.rows.Cell()
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
                        return liveness;
                    }

                    public boolean isTombstone()
                    {
                        return livenessInfo().hasLocalDeletionTime() && !livenessInfo().hasTTL();
                    }

                    public boolean isExpiring()
                    {
                        return livenessInfo().hasTTL();
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

                    public org.apache.cassandra.db.rows.Cell takeAlias()
                    {
                        return this;
                    }
                });
            }
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

    static class Set implements Iterable<TemporalRow>
    {
        private final ColumnFamilyStore baseCfs;
        private final ByteBuffer key;
        public final DecoratedKey dk;
        private final Map<Clustering, TemporalRow> clusteringToRow;

        Set(ColumnFamilyStore baseCfs, ByteBuffer key)
        {
            this.baseCfs = baseCfs;
            this.key = key;
            this.dk = baseCfs.partitioner.decorateKey(key);
            this.clusteringToRow = new HashMap<>();
        }

        public Iterator<TemporalRow> iterator()
        {
            return clusteringToRow.values().iterator();
        }

        public TemporalRow getExistingUnit(Row row)
        {
            return clusteringToRow.get(row.clustering());
        }

        public void addRow(Row row, boolean isNew)
        {
            TemporalRow temporalRow = clusteringToRow.get(row.clustering());
            if (temporalRow == null)
            {
                temporalRow = new TemporalRow(baseCfs, key, row, isNew);
                clusteringToRow.put(row.clustering(), temporalRow);
            }

            for (org.apache.cassandra.db.rows.Cell aRow : row)
            {
                temporalRow.addColumnValue(aRow, isNew);
            }
        }

        public int size()
        {
            return clusteringToRow.size();
        }
    }
}
