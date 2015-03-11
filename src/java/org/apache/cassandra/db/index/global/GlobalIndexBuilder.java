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

import java.util.Collection;
import java.util.Iterator;

import org.apache.cassandra.config.GlobalIndexDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.GlobalIndex;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.QueryPagers;

public class GlobalIndexBuilder
{
    private final ColumnFamilyStore baseCfs;
    private final GlobalIndexDefinition definition;
    private final GlobalIndex index;
    private final ReducingKeyIterator iter;

    private volatile boolean isStopped = false;

    public GlobalIndexBuilder(ColumnFamilyStore baseCfs, GlobalIndexDefinition definition, ReducingKeyIterator iter)
    {
        this.baseCfs = baseCfs;
        this.definition = definition;
        this.index = definition.resolve(baseCfs.metadata);
        this.iter = iter;
    }

    private void indexKey(DecoratedKey key)
    {
        Iterator<ColumnFamily> columnFamilies = QueryPagers.pageRowLocally(baseCfs, key.getKey(), 5000);
        while (columnFamilies.hasNext())
        {
            ColumnFamily cf = columnFamilies.next();
            Collection<Mutation> mutations = index.createMutations(key.getKey(), cf, ConsistencyLevel.ONE);
            StorageProxy.mutate(mutations, ConsistencyLevel.ONE);
        }
    }

    public void start()
    {
        Iterable<Range<Token>> ranges = StorageService.instance.getLocalRanges(baseCfs.metadata.ksName);

        while (!isStopped && iter.hasNext())
        {
            DecoratedKey key = iter.next();
            for (Range<Token> range: ranges)
            {
                if (range.contains(key.getToken()))
                {
                    indexKey(key);
                    SystemKeyspace.updateGlobalIndexBuild(baseCfs.metadata.ksName, definition.indexName, key.getKey());
                }
            }
        }
    }


    public void stop()
    {
        isStopped = true;
    }
}
