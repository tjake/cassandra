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
package org.apache.cassandra.db.index.global;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.GlobalIndexDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.GlobalIndex;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.utils.Pair;

// TODO: If key is only present in repaired sstables, write if it we are primary
// If key is present in unrepaired sstables, write it no matter what
public class GlobalIndexBuilder implements Runnable
{
    private final ColumnFamilyStore baseCfs;
    private final GlobalIndexDefinition definition;
    private final GlobalIndex index;

    private volatile boolean isStopped = false;

    public GlobalIndexBuilder(ColumnFamilyStore baseCfs, GlobalIndexDefinition definition)
    {
        this.baseCfs = baseCfs;
        this.definition = definition;
        this.index = definition.resolve(baseCfs.metadata);
    }

    private void indexKey(DecoratedKey key)
    {
        Iterator<ColumnFamily> columnFamilies = QueryPagers.pageRowLocally(baseCfs, key.getKey(), 5000);
        while (columnFamilies.hasNext())
        {
            ColumnFamily cf = columnFamilies.next();
            Collection<Mutation> mutations = index.createMutations(key.getKey(), cf, ConsistencyLevel.ONE, true);
            StorageProxy.mutate(mutations, ConsistencyLevel.ONE);
        }
    }

    public void run()
    {
        String ksname = baseCfs.metadata.ksName, indexname = definition.indexName;

        if (SystemKeyspace.isIndexBuilt(ksname, indexname))
            return;

        Iterable<Range<Token>> ranges = StorageService.instance.getPrimaryRanges(baseCfs.metadata.ksName);
        final Pair<Integer, ByteBuffer> indexStatus = SystemKeyspace.getGlobalIndexBuildStatus(ksname, indexname);
        ReducingKeyIterator iter;
        ByteBuffer lastKey;
        // Need to figure out where to start
        if (indexStatus == null)
        {
            int generation = Integer.MIN_VALUE;
            baseCfs.forceBlockingFlush();
            Collection<SSTableReader> sstables = baseCfs.getSSTables();
            for (SSTableReader reader: sstables)
            {
                generation = Math.max(reader.descriptor.generation, generation);
            }
            SystemKeyspace.beginGlobalIndexBuild(ksname, indexname, generation);
            iter = new ReducingKeyIterator(sstables);
            lastKey = null;
        }
        else
        {
            Collection<SSTableReader> sstables = Lists.newArrayList(Iterables.filter(baseCfs.getSSTables(), new Predicate<SSTableReader>()
            {
                @Override
                public boolean apply(SSTableReader ssTableReader)
                {
                    return ssTableReader.descriptor.generation <= indexStatus.left;
                }
            }));
            iter = new ReducingKeyIterator(sstables);
            lastKey = indexStatus.right;
        }

        while (!isStopped && iter.hasNext())
        {
            DecoratedKey key = iter.next();
            if (lastKey == null || lastKey.compareTo(key.getKey()) < 0)
            {
                lastKey = null;
                for (Range<Token> range : ranges)
                {
                    if (range.contains(key.getToken()))
                    {
                        indexKey(key);
                        SystemKeyspace.updateGlobalIndexBuildStatus(ksname, indexname, key.getKey());
                    }
                }
            }
        }

        SystemKeyspace.finishGlobalIndexBuildStatus(ksname, indexname);
    }


    public void stop()
    {
        isStopped = true;
    }
}
