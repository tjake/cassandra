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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalIndexManager
{
    private final ConcurrentHashMap<String, GlobalIndex> globalIndexMap = new ConcurrentHashMap<>();
    private final ColumnFamilyStore baseCfs;

    public GlobalIndexManager(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
    }

    private GlobalIndex resolveIndex(GlobalIndexDefinition definition)
    {
        if (globalIndexMap.containsKey(definition.indexName))
            return globalIndexMap.get(definition.indexName);

        GlobalIndex index = definition.resolve(baseCfs.metadata);
        GlobalIndex previousIndex = globalIndexMap.putIfAbsent(definition.indexName, index);
        if (previousIndex != null)
            return previousIndex;
        return index;
    }

    private Collection<GlobalIndex> resolveIndexes(Collection<GlobalIndexDefinition> definitions)
    {
        List<GlobalIndex> indexes = new ArrayList<>(definitions.size());

        for (GlobalIndexDefinition definition: definitions) {
            indexes.add(resolveIndex(definition));
        }
        return indexes;
    }

    private List<Mutation> createMutationsInternal(ByteBuffer key, ColumnFamily cf, ConsistencyLevel consistency)
    {
        Collection<GlobalIndexDefinition> indexDefs = cf.metadata().getGlobalIndexes().values();

        List<Mutation> tmutations = null;
        for (GlobalIndex index: resolveIndexes(indexDefs))
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
                GlobalIndexManager indexManager = Keyspace.open(cf.metadata().ksName).getColumnFamilyStore(cf.metadata().cfId).globalIndexManager;
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

    public void reload()
    {
        globalIndexMap.clear();

        for (GlobalIndexDefinition definition: baseCfs.metadata.getGlobalIndexes().values())
        {
            ScheduledExecutors.optionalTasks.execute(build(definition));
        }
    }

    public Runnable build(GlobalIndexDefinition indexDefinition)
    {
        GlobalIndex index = resolveIndex(indexDefinition);
        index.stopBuilder();

        GlobalIndexBuilder builder = new GlobalIndexBuilder(baseCfs, indexDefinition);
        index.setBuilder(builder);
        return builder;
    }
}
