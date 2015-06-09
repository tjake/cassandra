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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.function.Consumer;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

// This is a class that allows comparisons based on partition key and clustering columns, and resolves existing and
// new mutation values
public class MutationUnit
{
    public static final Resolver oldValueIfUpdated = new Resolver()
    {
        public Cell resolve(Iterable<MUCell> cells)
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

            return ByteBufferUtil.compareUnsigned(initial.cell.value(), value.cell.value()) != 0 ? initial.cell : null;
        }
    };

    public static final Resolver newValueIfUpdated = new Resolver()
    {
        public Cell resolve(Iterable<MUCell> cells)
        {
            Iterator<MUCell> iterator = cells.iterator();
            if (!iterator.hasNext())
                return null;
            MUCell initial = iterator.next();
            if (!iterator.hasNext())
                return initial.cell;

            MUCell value = initial;
            while (iterator.hasNext())
                value = value.reconcile(iterator.next());

            return value.isNew && ByteBufferUtil.compareUnsigned(initial.cell.value(), value.cell.value()) != 0
                   ? value.cell
                   : null;
        }
    };

    public static final Resolver earliest = new Resolver()
    {
        public Cell resolve(Iterable<MUCell> cells)
        {
            Iterator<MUCell> iterator = cells.iterator();
            if (!iterator.hasNext())
                return null;
            return iterator.next().cell;
        }
    };

    public static final Resolver latest = new Resolver()
    {
        public Cell resolve(Iterable<MUCell> cells)
        {
            Iterator<MUCell> iterator = cells.iterator();
            if (!iterator.hasNext())
                return null;
            Cell value = iterator.next().cell;
            while (iterator.hasNext())
                value = Cells.reconcile(value, iterator.next().cell, FBUtilities.nowInSeconds());

            return value;
        }
    };

    public Clustering viewClustering(ClusteringComparator comparator, List<ColumnDefinition> clusteringColumns, Resolver resolver)
    {
        Object[] clusterings = new Object[clusteringColumns.size()];

        for (int i = 0; i < clusteringColumns.size(); i++)
        {
            clusteringValue(clusteringColumns.get(i), resolver);
        }

        return comparator.make(clusterings);
    }

    private interface PrivateResolver
    {
        Cell resolve(Iterable<MUCell> cells);
    }

    private static class MUCell
    {
        public final Cell cell;
        public final boolean isNew;

        private MUCell(Cell cell, boolean isNew)
        {
            this.cell = cell;
            this.isNew = isNew;
        }

        public MUCell reconcile(MUCell cell)
        {
            Cell reconciled = Cells.reconcile(this.cell, cell.cell, FBUtilities.nowInSeconds());
            if (reconciled == this.cell) return this;
            else return cell;
        }
    }

    public interface Resolver extends PrivateResolver
    {
    }

    final ColumnFamilyStore baseCfs;
    private final ByteBuffer partitionKey;
    private final Map<ColumnIdentifier, ByteBuffer> clusteringColumns;
    private final Map<ColumnIdentifier, Map<Clustering, SortedMap<Long, MUCell>>> columnValues = new HashMap<>();
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

        Map<Clustering, SortedMap<Long, MUCell>> innerMap = columnValues.get(identifier);

        if (!innerMap.containsKey(clustering))
            innerMap.put(clustering, new TreeMap<>());

        if (cell.isExpiring())
            ttl = Math.max(cell.livenessInfo().ttl(), ttl);

        innerMap.get(clustering).put(cell.livenessInfo().timestamp(), new MUCell(cell, isNew));
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
        Map<Clustering, SortedMap<Long, MUCell>> innerMap = columnValues.get(definition.name);
        if (innerMap == null)
            return Collections.emptyList();

        Collection<Cell> value = new ArrayList<>();
        for (SortedMap<Long, MUCell> cells: innerMap.values())
        {
            Cell cell = resolver.resolve(cells.values());
            if (cell != null)
                value.add(cell);
        }
        return value;
    }

    static class Set implements Iterable<MutationUnit>
    {
        private final ColumnFamilyStore baseCfs;
        private final HashSet<ColumnDefinition> clusteringSelectors;
        private final Map<MutationUnit, MutationUnit> mutationUnits = new HashMap<>();

        Set(ColumnFamilyStore baseCfs, List<ColumnDefinition> clusteringSelectors)
        {
            this.baseCfs = baseCfs;
            this.clusteringSelectors = new HashSet<>(clusteringSelectors);
        }

        Set(MutationUnit single, List<ColumnDefinition> clusteringSelectors)
        {
            this(single.baseCfs, clusteringSelectors);
            mutationUnits.put(single, single);
        }

        public Iterator<MutationUnit> iterator()
        {
            return mutationUnits.values().iterator();
        }

        public void forEach(Consumer<? super MutationUnit> action)
        {
            mutationUnits.values().forEach(action);
        }

        public Spliterator<MutationUnit> spliterator()
        {
            return mutationUnits.values().spliterator();
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
