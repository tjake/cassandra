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
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

/**
 * There is essentially 2 types of CType: the truly composite ones, and the
 * others.
 */
public interface CType extends Comparator<Composite>
{
    // Whether we really use a compositeType underneath (so isPacked implies size() == 1).
    public boolean isPacked();

    public int size();
    public AbstractType<?> subtype(int i);

    public CBuilder builder();
    public Composite fromByteBuffer(ByteBuffer bb);

    public void validate(Composite name);

    public boolean isCompatibleWith(CType previous);

    public CType setSubtype(int position, AbstractType<?> newType);

    // Use sparingly, it defeats the purpose
    public AbstractType<?> asAbstractType();

    public String getString(Composite c);

    public Comparator<Composite> reverseComparator();
    public Comparator<IndexInfo> indexComparator();
    public Comparator<IndexInfo> indexReverseComparator();

    public ISerializer<Composite> serializer();

    public ISerializer<IndexInfo> indexSerializer();
    public IVersionedSerializer<ColumnSlice> sliceSerializer();
    public IVersionedSerializer<SliceQueryFilter> sliceQueryFilterSerializer();
    public DeletionInfo.Serializer deletionInfoSerializer();
    public RangeTombstone.Serializer rangeTombstoneSerializer();
    public RowIndexEntry.Serializer rowIndexEntrySerializer();
}
