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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.MaterializedViewDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MaterializedViewManager
{
    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentCounterWriters() * 1024);
    private static final Logger logger = LoggerFactory.getLogger(MaterializedViewManager.class);

    private final ConcurrentNavigableMap<String, MaterializedView> viewsByName;

    private final ColumnFamilyStore baseCfs;

    public MaterializedViewManager(ColumnFamilyStore baseCfs)
    {
        this.viewsByName = new ConcurrentSkipListMap<>();

        this.baseCfs = baseCfs;
    }

    public void reload()
    {
        Map<String, MaterializedViewDefinition> newViewsByName = new HashMap<>();
        for (MaterializedViewDefinition definition: baseCfs.metadata.getMaterializedViews().values())
        {
            newViewsByName.put(definition.viewName, definition);
        }

        for (String viewName: viewsByName.keySet())
        {
            if (!newViewsByName.containsKey(viewName))
                removeMaterializedView(viewName);
        }

        for (Map.Entry<String, MaterializedViewDefinition> entry : newViewsByName.entrySet())
        {
            if (!viewsByName.containsKey(entry.getKey()))
                addMaterializedView(entry.getValue());
        }

        for (MaterializedView view: viewsByName.values())
            view.reload();
    }

    public void buildIfRequired()
    {
        for (MaterializedView view: viewsByName.values())
            view.build();
    }

    private void removeMaterializedView(String name)
    {
        MaterializedView view = viewsByName.remove(name);

        if (view == null)
            return;

        SystemKeyspace.setIndexRemoved(baseCfs.metadata.ksName, view.name);
    }

    public void addMaterializedView(MaterializedViewDefinition definition)
    {
        MaterializedView view = new MaterializedView(definition, baseCfs);

        viewsByName.put(definition.viewName, view);
    }

    public void pushReplicaMutations(ByteBuffer key, ColumnFamily cf)
    throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        // This happens when we are replaying from commitlog. In that case, we have already sent this commit off to the
        // view node.
        if (!StorageService.instance.isJoined()) return;

        List<Mutation> mutations = null;
        for (MaterializedView view: viewsByName.values())
        {
            Collection<Mutation> viewMutations = view.createMutations(key, cf, ConsistencyLevel.ONE, false);
            if (viewMutations != null && !viewMutations.isEmpty())
            {
                if (mutations == null)
                    mutations = Lists.newLinkedList();
                mutations.addAll(viewMutations);
            }
        }
        if (mutations != null)
        {
            StorageProxy.mutateMV(key, mutations);
        }
    }

    public boolean cfModifiesSelectedColumn(ColumnFamily cf)
    {
        for (MaterializedView view: viewsByName.values())
        {
            if (view.cfModifiesSelectedColumn(cf))
                return true;
        }
        return false;
    }

    public Iterable<? extends MaterializedView> allViews()
    {
        return viewsByName.values();
    }

    public static Lock acquireLockFor(ByteBuffer key)
    {
        Lock lock = LOCKS.get(key);
        lock.lock();
        return lock;
    }

    public static boolean touchesSelectedColumn(Collection<? extends IMutation> mutations)
    {
        for (IMutation mutation : mutations)
        {
            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                MaterializedViewManager viewManager = Keyspace.open(cf.metadata().ksName)
                                                              .getColumnFamilyStore(cf.metadata().cfId).materializedViewManager;
                if (viewManager.cfModifiesSelectedColumn(cf))
                    return true;
            }
        }
        return false;
    }
}
