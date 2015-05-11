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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.GlobalIndexDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

public class GlobalIndexManager implements IndexManager
{
    /**
     * Organizes the indexes by column name
     */
    private final ConcurrentNavigableMap<ByteBuffer, GlobalIndex> indexesByColumn;

    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentCounterWriters() * 1024);

    private final Set<GlobalIndex> allIndexes;

    private final ColumnFamilyStore baseCfs;

    public GlobalIndexManager(ColumnFamilyStore baseCfs)
    {
        this.indexesByColumn = new ConcurrentSkipListMap<>();
        this.writeLocks = new ConcurrentSkipListMap<>();
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

    public void buildIfRequired()
    {
        for (GlobalIndex index: allIndexes)
            index.build();
    }

    private void removeIndexedColumn(ByteBuffer column)
    {
        GlobalIndex index = indexesByColumn.remove(column);

        if (index == null)
            return;

        allIndexes.remove(index);

        SystemKeyspace.setIndexRemoved(baseCfs.metadata.ksName, index.indexName);
    }

    public void addIndexedColumn(GlobalIndexDefinition definition)
    {
        ColumnDefinition targetCd = baseCfs.metadata.getColumnDefinition(definition.target);
        assert targetCd != null;

        Collection<ColumnDefinition> includedDefs = new ArrayList<>();
        for (ColumnIdentifier identifier : definition.included)
        {
            ColumnDefinition cfDef = baseCfs.metadata.getColumnDefinition(identifier);
            assert cfDef != null;
            includedDefs.add(cfDef);
        }

        GlobalIndex index = new GlobalIndex(definition, targetCd, includedDefs, baseCfs);

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
                if (tmutations == null)
                    tmutations = Lists.newLinkedList();
                tmutations.addAll(mutations);
            }
        }
        return tmutations;
    }

    public void pushReplicaMutations(ByteBuffer key, ColumnFamily cf)
    throws WriteTimeoutException
    {
        // TODO - When we are replaying the commitlog, we haven't yet joined the ring, so we can't push new mutations
        if (!StorageService.instance.isJoined()) return;

        List<Mutation> mutations = null;
        for (GlobalIndex index: allIndexes)
        {
            Collection<Mutation> indexMutations = index.createMutations(key, cf, ConsistencyLevel.ONE, false);
            if (indexMutations != null && !indexMutations.isEmpty())
            {
                if (mutations == null)
                    mutations = Lists.newLinkedList();
                mutations.addAll(indexMutations);
            }
        }
        if (mutations != null)
        {
            StorageProxy.mutateGI(key, mutations);
        }
    }

    public boolean cfModifiesIndexedColumn(ColumnFamily cf)
    {
        for (GlobalIndex index: allIndexes)
        {
            if (index.cfModifiesIndexedColumn(cf))
                return true;
        }
        return false;
    }

    public Lock acquireLockFor(ByteBuffer key)
    {
        Lock lock = LOCKS.get(Objects.hashCode(baseCfs.metadata.cfId, key));
        lock.lock();
        return lock;
    }

    public static boolean touchesIndexedColumns(Collection<? extends IMutation> mutations)
    {
        for (IMutation mutation : mutations)
        {
            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                GlobalIndexManager indexManager = Keyspace.open(cf.metadata().ksName)
                                                          .getColumnFamilyStore(cf.metadata().cfId).globalIndexManager;
                if (indexManager.cfModifiesIndexedColumn(cf))
                    return true;
            }
        }
        return false;
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
