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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.GlobalIndexDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.global.GlobalIndexBuilder;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Pair;
import org.apache.mina.util.ConcurrentHashSet;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class GlobalIndexManager implements IndexManager
{
    /**
     * Organizes the indexes by column name
     */
    private final ConcurrentNavigableMap<ByteBuffer, GlobalIndex> indexesByColumn;

    private final Set<GlobalIndex> allIndexes;

    private final ColumnFamilyStore baseCfs;

    public GlobalIndexManager(ColumnFamilyStore baseCfs)
    {
        this.indexesByColumn = new ConcurrentSkipListMap<>();
        this.allIndexes = Collections.newSetFromMap(new ConcurrentHashMap<GlobalIndex, Boolean>());

        this.baseCfs = baseCfs;
    }

    public void reload()
    {
        Map<ByteBuffer, GlobalIndexDefinition> newIndexesByColumn = new HashMap<>();
        for (GlobalIndexDefinition definition: baseCfs.metadata.getGlobalIndexes().values())
        {
            newIndexesByColumn.put(definition.target.bytes, definition);
        }

        for (ByteBuffer indexedColumn: indexesByColumn.keySet())
        {
            if (!newIndexesByColumn.containsKey(indexedColumn))
                removeIndexedColumn(indexedColumn);
        }

        for (ByteBuffer indexedColumn: newIndexesByColumn.keySet())
        {
            if (!indexesByColumn.containsKey(indexedColumn))
                addIndexedColumn(newIndexesByColumn.get(indexedColumn));
        }

        for (GlobalIndex index: allIndexes)
            index.reload();
    }

    private void removeIndexedColumn(ByteBuffer column)
    {
        GlobalIndex index = indexesByColumn.remove(column);

        if (index == null)
            return;

        allIndexes.remove(index);

        SystemKeyspace.setIndexRemoved(baseCfs.metadata.ksName, index.indexName);
    }

    private void addIndexedColumn(GlobalIndexDefinition definition)
    {
        GlobalIndex index = definition.resolve(baseCfs.metadata);

        indexesByColumn.put(definition.target.bytes, index);

        allIndexes.add(index);
    }

    public GlobalIndex getIndexForColumn(ByteBuffer column)
    {
        return indexesByColumn.get(column);
    }

    private List<Mutation> createMutationsInternal(ByteBuffer key, ColumnFamily cf, ConsistencyLevel consistency)
    {
        List<Mutation> tmutations = null;
        for (GlobalIndex index: allIndexes)
        {
            Collection<Mutation> mutations = index.createMutations(key, cf, consistency, false);
            if (mutations != null) {
                if (tmutations == null) {
                    tmutations = Lists.newLinkedList();
                }
                tmutations.addAll(mutations);
            }
        }
        return tmutations;
    }

    public static Collection<Mutation> createMutations(Collection<? extends IMutation> mutations, ConsistencyLevel consistency)
    {
        boolean hasCounters = false;
        List<Mutation> augmentedMutations = null;

        for (IMutation mutation : mutations)
        {
            if (mutation instanceof CounterMutation)
                hasCounters = true;

            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                GlobalIndexManager indexManager = Keyspace.open(cf.metadata().ksName)
                                                          .getColumnFamilyStore(cf.metadata().cfId).globalIndexManager;

                List<Mutation> augmentations = indexManager.createMutationsInternal(mutation.key(), cf, consistency);
                if (augmentations == null || augmentations.isEmpty())
                    continue;

                if (augmentedMutations == null)
                    augmentedMutations = new LinkedList<>();
                augmentedMutations.addAll(augmentations);
            }
        }

        if (augmentedMutations == null)
            return null;

        if (hasCounters)
            throw new InvalidRequestException("Counter mutations and global index mutations cannot be applied together atomically.");

        @SuppressWarnings("unchecked")
        Collection<Mutation> originalMutations = (Collection<Mutation>) mutations;

        return mergeMutations(Iterables.concat(originalMutations, augmentedMutations));
    }


    private static Collection<Mutation> mergeMutations(Iterable<Mutation> mutations)
    {
        Map<Pair<String, ByteBuffer>, Mutation> groupedMutations = new HashMap<>();

        for (Mutation mutation : mutations)
        {
            Pair<String, ByteBuffer> key = Pair.create(mutation.getKeyspaceName(), mutation.key());
            Mutation current = groupedMutations.get(key);
            if (current == null)
            {
                // copy in case the mutation's modifications map is backed by an immutable Collections#singletonMap().
                groupedMutations.put(key, mutation.copy());
            }
            else
            {
                current.addAll(mutation);
            }
        }

        return groupedMutations.values();
    }
}
