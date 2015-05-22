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
package org.apache.cassandra.service.pager;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractQueryPager implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractQueryPager.class);

    private final ConsistencyLevel consistencyLevel;
    private final boolean localQuery;

    protected final CFMetaData cfm;
    protected final DataLimits limits;

    private int remaining;

    // This is the last key we've been reading from (or can still be reading within). This the key for
    // which remainingInPartition makes sense: if we're starting another key, we should reset remainingInPartition
    // (and this is done in PagerIterator). This can be null (when we start).
    private DecoratedKey lastKey;
    private int remainingInPartition;

    private boolean exhausted;

    protected AbstractQueryPager(ConsistencyLevel consistencyLevel,
                                 boolean localQuery,
                                 CFMetaData cfm,
                                 DataLimits limits)
    {
        this.consistencyLevel = consistencyLevel;
        this.localQuery = localQuery;

        this.cfm = cfm;
        this.limits = limits;

        this.remaining = limits.count();
        this.remainingInPartition = limits.perPartitionCount();
    }

    public PartitionIterator fetchPage(int pageSize) throws RequestValidationException, RequestExecutionException
    {
        if (isExhausted())
            return PartitionIterators.EMPTY;

        pageSize = Math.min(pageSize, remaining);
        return new PagerIterator(queryNextPage(pageSize, consistencyLevel, localQuery), limits.forPaging(pageSize));
    }

    private class PagerIterator extends CountingPartitionIterator
    {
        private final DataLimits pageLimits;

        private Row lastRow;

        private PagerIterator(PartitionIterator iter, DataLimits pageLimits)
        {
            super(iter, pageLimits);
            this.pageLimits = pageLimits;
        }

        @Override
        public RowIterator next()
        {
            RowIterator iter = super.next();
            DecoratedKey key = iter.partitionKey();
            if (lastKey == null || !lastKey.equals(key))
                remainingInPartition = limits.perPartitionCount();

            lastKey = key;
            return new RowPagerIterator(iter);
        }

        @Override
        public void close()
        {
            super.close();
            recordLast(lastKey, lastRow);

            int counted = counter.counted();
            remaining -= counted;
            remainingInPartition -= counter.countedInCurrentPartition();
            exhausted = counted < pageLimits.count();
        }

        private class RowPagerIterator extends WrappingRowIterator
        {
            RowPagerIterator(RowIterator iter)
            {
                super(iter);
            }

            @Override
            public Row next()
            {
                lastRow = super.next();
                return lastRow;
            }
        }
    }

    protected void restoreState(DecoratedKey lastKey, int remaining, int remainingInPartition)
    {
        this.lastKey = lastKey;
        this.remaining = remaining;
        this.remainingInPartition = remainingInPartition;
    }

    public boolean isExhausted()
    {
        return exhausted || remaining == 0 || ((this instanceof SinglePartitionPager) && remainingInPartition == 0);
    }

    public int maxRemaining()
    {
        return remaining;
    }

    protected int remainingInPartition()
    {
        return remainingInPartition;
    }

    protected abstract PartitionIterator queryNextPage(int pageSize, ConsistencyLevel consistency, boolean localQuery) throws RequestValidationException, RequestExecutionException;

    protected abstract void recordLast(DecoratedKey key, Row row);
}
