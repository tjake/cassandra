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

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
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

public class MaterializedViewBuilder extends CompactionInfo.Holder
{
    private final ColumnFamilyStore baseCfs;
    private final MaterializedView view;
    private final UUID compactionId;
    private volatile Token prevToken = null;

    private static final Logger logger = LoggerFactory.getLogger(MaterializedViewBuilder.class);

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
                        StorageProxy.mutateMV(key.getKey(), mutations);
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
        Token lastToken;
        Collection<SSTableReader> sstables;
        // Need to figure out where to start
        if (buildStatus == null)
        {
            int generation = Integer.MIN_VALUE;
            baseCfs.forceBlockingFlush();
            sstables = baseCfs.getSSTables();
            for (SSTableReader reader : sstables)
            {
                generation = Math.max(reader.descriptor.generation, generation);
            }
            SystemKeyspace.beginMaterializedViewBuild(ksname, viewName, generation);
            lastToken = null;
        }
        else
        {
            sstables = Lists.newArrayList(Iterables.filter(baseCfs.getSSTables(), new Predicate<SSTableReader>()
            {
                @Override
                public boolean apply(SSTableReader ssTableReader)
                {
                    return ssTableReader.descriptor.generation <= buildStatus.left;
                }
            }));
            lastToken = buildStatus.right;
        }

        prevToken = lastToken;
        try (ReducingKeyIterator iter = new ReducingKeyIterator(sstables))
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

            SystemKeyspace.finishMaterializedViewBuildStatus(ksname, viewName);

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
            logger.warn("Materialized View failed to complete, sleeping 5 minutes before restarting", e);
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
