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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

// TODO: If key is only present in repaired sstables, write if it we are primary
// If key is present in unrepaired sstables, write it no matter what
public class MaterializedViewBuilder extends CompactionInfo.Holder
{
    private final ColumnFamilyStore baseCfs;
    private final MaterializedView view;
    private final UUID compactionId;
    private volatile Token prevToken = null;

    private volatile boolean isStopped = false;

    public MaterializedViewBuilder(ColumnFamilyStore baseCfs, MaterializedView view)
    {
        this.baseCfs = baseCfs;
        this.view = view;
        compactionId = UUIDGen.getTimeUUID();
    }

    private void buildKey(DecoratedKey key)
    {
        Iterator<ColumnFamily> columnFamilies = QueryPagers.pageRowLocally(baseCfs, key.getKey(), 5000);
        while (columnFamilies.hasNext())
        {
            ColumnFamily cf = columnFamilies.next();
            Collection<Mutation> mutations = view.createMutations(key.getKey(), cf, ConsistencyLevel.ONE, true);

            if (mutations != null)
            {
                int retries = 3;
                while (retries > 0)
                {
                    try
                    {
                        StorageProxy.mutateGI(key.getKey(), mutations);
                        break;
                    }
                    catch (WriteTimeoutException ex)
                    {
                        if (--retries == 0)
                            throw ex;
                    }
                }
            }
        }
    }

    public void run()
    {
        String ksname = baseCfs.metadata.ksName, viewName = view.name;

        if (SystemKeyspace.isIndexBuilt(ksname, viewName))
            return;

        Iterable<Range<Token>> ranges = StorageService.instance.getLocalRanges(baseCfs.metadata.ksName);
        final Pair<Integer, Token> buildStatus = SystemKeyspace.getMaterializedViewBuildStatus(ksname, viewName);
        ReducingKeyIterator iter;
        Token lastToken;
        // Need to figure out where to start
        if (buildStatus == null)
        {
            int generation = Integer.MIN_VALUE;
            baseCfs.forceBlockingFlush();
            Collection<SSTableReader> sstables = baseCfs.getSSTables();
            for (SSTableReader reader : sstables)
            {
                generation = Math.max(reader.descriptor.generation, generation);
            }
            SystemKeyspace.beginMaterializedViewBuild(ksname, viewName, generation);
            iter = new ReducingKeyIterator(sstables);
            lastToken = null;
        }
        else
        {
            Collection<SSTableReader> sstables = Lists.newArrayList(Iterables.filter(baseCfs.getSSTables(), new Predicate<SSTableReader>()
            {
                @Override
                public boolean apply(SSTableReader ssTableReader)
                {
                    return ssTableReader.descriptor.generation <= buildStatus.left;
                }
            }));
            iter = new ReducingKeyIterator(sstables);
            lastToken = buildStatus.right;
        }

        prevToken = lastToken;
        try
        {
            while (!isStopped && iter.hasNext())
            {
                DecoratedKey key = iter.next();
                Token token = key.getToken();
                if (lastToken == null || lastToken.compareTo(token) < 0)
                {
                    for (Range<Token> range : ranges)
                    {
                        if (range.contains(token))
                        {
                            buildKey(key);

                            if (prevToken == null || prevToken.compareTo(token) != 0)
                            {
                                SystemKeyspace.updateMaterializedViewBuildStatus(ksname, viewName, key.getToken());
                                prevToken = token;
                            }
                        }
                    }
                    lastToken = null;
                }
            }
        }
        catch (Exception e)
        {
            final MaterializedViewBuilder builder = new MaterializedViewBuilder(baseCfs, view);
            ScheduledExecutors.nonPeriodicTasks.schedule(new Runnable()
                                                         {
                                                             public void run()
                                                             {
                                                                 CompactionManager.instance.submitMaterializedViewBuilder(builder);
                                                             }
                                                         },
                                                         5,
                                                         TimeUnit.MINUTES);
            throw e;
        }

        SystemKeyspace.finishMaterializedViewBuildStatus(ksname, viewName);

        try
        {
            iter.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public CompactionInfo getCompactionInfo()
    {
        long rangesLeft = 0, rangesTotal = 0;
        Token lastToken = prevToken;
        for (Range<Token> range : StorageService.instance.getLocalRanges(baseCfs.keyspace.getName()))
        {
            rangesLeft++;
            rangesTotal++;
            if (lastToken == null || range.contains(lastToken))
                rangesLeft = 0;
        }
        return new CompactionInfo(baseCfs.metadata, OperationType.VIEW_BUILD, rangesLeft, rangesTotal, "ranges", compactionId);
    }

    public void stop()
    {
        isStopped = true;
    }
}
