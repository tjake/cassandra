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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.NamesPartitionFilter;
import org.apache.cassandra.db.filter.SlicePartitionFilter;
import org.apache.cassandra.utils.FBUtilities;

public class SinglePartitionSliceReadBuilder extends AbstractReadCommandBuilder
{
    private final DecoratedKey partitionKey;
    private Slices.Builder sliceBuilder;

    public SinglePartitionSliceReadBuilder(ColumnFamilyStore cfs, DecoratedKey key)
    {
        this(cfs, FBUtilities.nowInSeconds(), key);
    }

    public SinglePartitionSliceReadBuilder(ColumnFamilyStore cfs, int nowInSeconds, DecoratedKey key)
    {
        super(cfs, nowInSeconds);
        partitionKey = key;
        sliceBuilder = new Slices.Builder(cfs.getComparator());
    }

    public SinglePartitionSliceReadBuilder addSlice(Slice slice)
    {
        sliceBuilder.add(slice);
        return this;
    }

    @Override
    public ReadCommand build()
    {
        PartitionColumns.Builder builder = new PartitionColumns.Builder();
        if (columns.size() == 0)
        {
            // Add all non-clustering, non-pk columns if none are specified
            for (ColumnDefinition cd : cfs.metadata.allColumns())
            {
                if (cd.isRegular())
                    builder.add(cd);
            }
        }
        else
        {
            for (ByteBuffer colName : columns)
            {
                ColumnDefinition cd = cfs.metadata.getColumnDefinition(colName);
                assert cd.kind == ColumnDefinition.Kind.REGULAR || cd.kind == ColumnDefinition.Kind.STATIC;
                builder.add(cfs.metadata.getColumnDefinition(colName));
            }
        }

        if (superColumn != null)
            filter.add(cfs.metadata.compactValueColumn(), Operator.EQ, superColumn);

        SlicePartitionFilter partitionFilter = new SlicePartitionFilter(builder.build(), sliceBuilder.build(), reversed);

        DataLimits limits = DataLimits.cqlLimits(cqlLimit);
        if (pagingLimit != -1)
            limits = limits.forPaging(pagingLimit);

        return new SinglePartitionSliceCommand(cfs.metadata,
                                             nowInSeconds,
                                             filter,
                                             limits,
                                             partitionKey,
                                             partitionFilter);
    }
}
