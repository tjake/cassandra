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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.GlobalIndexDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.global.GlobalIndexBuilder;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.utils.Pair;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalIndexManager
{
    public static GlobalIndexManager instance = new GlobalIndexManager();

    private ConcurrentHashMap<String, GlobalIndex> globalIndexMap = new ConcurrentHashMap<>();

    private GlobalIndexManager()
    {
    }

    private GlobalIndex resolveIndex(CFMetaData cfm, GlobalIndexDefinition definition)
    {
        if (globalIndexMap.containsKey(definition.indexName))
            return globalIndexMap.get(definition.indexName);

        GlobalIndex index = definition.resolve(cfm);
        GlobalIndex previousIndex = globalIndexMap.putIfAbsent(definition.indexName, index);
        if (previousIndex != null)
            return previousIndex;
        return index;
    }

    private Collection<GlobalIndex> resolveIndexes(CFMetaData cfm, Collection<GlobalIndexDefinition> definitions)
    {
        List<GlobalIndex> indexes = new ArrayList<>(definitions.size());

        for (GlobalIndexDefinition definition: definitions) {
            indexes.add(resolveIndex(cfm, definition));
        }
        return indexes;
    }

    private List<Mutation> createMutationsInternal(ByteBuffer key, ColumnFamily cf, ConsistencyLevel consistency)
    {
        Collection<GlobalIndexDefinition> indexDefs = cf.metadata().getGlobalIndexes().values();

        List<Mutation> tmutations = null;
        for (GlobalIndex index: resolveIndexes(cf.metadata(), indexDefs))
        {
            Collection<Mutation> mutations = index.createMutations(key, cf, consistency);
            if (mutations != null) {
                if (tmutations == null) {
                    tmutations = Lists.newLinkedList();
                }
                tmutations.addAll(mutations);
            }
        }
        return tmutations;
    }

    public Collection<Mutation> createMutations(Collection<? extends IMutation> mutations, ConsistencyLevel consistency)
    {
        boolean hasCounters = false;
        List<Mutation> augmentedMutations = null;

        for (IMutation mutation : mutations)
        {
            if (mutation instanceof CounterMutation)
                hasCounters = true;

            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                List<Mutation> augmentations = createMutationsInternal(mutation.key(), cf, consistency);
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



    private Collection<Mutation> mergeMutations(Iterable<Mutation> mutations)
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

    public void reload(ColumnFamilyStore columnFamilyStore)
    {
        // TODO: only remove the Column Family's GIs
        globalIndexMap.clear();

        for (GlobalIndexDefinition definition: columnFamilyStore.metadata.getGlobalIndexes().values())
        {
            ScheduledExecutors.optionalTasks.execute(build(columnFamilyStore.metadata, definition));
        }
    }

    public Runnable build(CFMetaData cfm, GlobalIndexDefinition indexDefinition)
    {
        return new GlobalIndexBuilder(indexDefinition.resolve(cfm).baseCfs, indexDefinition);
    }
}
