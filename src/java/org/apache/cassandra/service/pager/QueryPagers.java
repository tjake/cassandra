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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;

/**
 * Static utility methods to create query pagers.
 */
public class QueryPagers
{
    private QueryPagers() {};

    public static SinglePartitionPager pager(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel, ClientState cState, boolean local, PagingState state)
    {
        return new SinglePartitionPager(command, consistencyLevel, cState, local, state);
    }

    private static QueryPager pager(ReadQuery command, ConsistencyLevel consistencyLevel, ClientState cState, boolean local, PagingState state)
    {
        if (command instanceof SinglePartitionReadCommand.Group)
        {
            SinglePartitionReadCommand.Group group = (SinglePartitionReadCommand.Group)command;
            if (group.commands.size() == 1)
                return pager(group.commands.get(0), consistencyLevel, cState, local, state);

            return new MultiPartitionPager(group.commands, consistencyLevel, cState, local, state, group.limits());
        }
        else if (command instanceof SinglePartitionReadCommand)
        {
            return pager((SinglePartitionReadCommand)command, consistencyLevel, cState, local, state);
        }
        else
        {
            assert command instanceof PartitionRangeReadCommand;
            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand)command;
            if (rangeCommand.isNamesQuery())
                return new RangeNamesQueryPager(rangeCommand, consistencyLevel, local, state);
            else
                return new RangeSliceQueryPager(rangeCommand, consistencyLevel, local, state);
        }
    }

    public static QueryPager pager(ReadQuery command, ConsistencyLevel consistencyLevel, ClientState cState)
    {
        return pager(command, consistencyLevel, cState, false, null);
    }

    public static QueryPager pager(ReadQuery command, ConsistencyLevel consistencyLevel, ClientState cState, PagingState state)
    {
        return pager(command, consistencyLevel, cState, false, state);
    }

    public static QueryPager localPager(ReadQuery command)
    {
        return pager(command, null, null, true, null);
    }

    /**
     * Convenience method that count (live) cells/rows for a given slice of a row, but page underneath.
     */
    public static int countPaged(CFMetaData metadata,
                                 DecoratedKey key,
                                 PartitionFilter filter,
                                 DataLimits limits,
                                 ConsistencyLevel consistencyLevel,
                                 ClientState state,
                                 final int pageSize,
                                 int nowInSec) throws RequestValidationException, RequestExecutionException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(metadata, nowInSec, ColumnFilter.NONE, limits, key, filter);
        final SinglePartitionPager pager = new SinglePartitionPager(command, consistencyLevel, state, false);

        int count = 0;
        while (!pager.isExhausted())
        {
            CountingDataIterator iter = new CountingDataIterator(pager.fetchPage(pageSize), limits);
            DataIterators.consume(iter);
            count += iter.counter().counted();
        }
        return count;
    }
}
