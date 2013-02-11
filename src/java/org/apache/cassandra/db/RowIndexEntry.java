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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.FileUtils;

public class RowIndexEntry
{
    public final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    public static RowIndexEntry create(long position, DeletionInfo deletionInfo, ColumnIndex index)
    {
        if (index != null && index.columnsIndex != null && index.columnsIndex.size() > 1)
            return new IndexedEntry(position, deletionInfo, index.columnsIndex);
        else
            return new RowIndexEntry(position);
    }

    public boolean isIndexed()
    {
        return !columnsIndex().isEmpty();
    }

    public DeletionInfo deletionInfo()
    {
        throw new UnsupportedOperationException();
    }

    public List<IndexHelper.IndexInfo> columnsIndex()
    {
        return Collections.emptyList();
    }

    public static class Serializer
    {
        private final CType type;

        public Serializer(CType type)
        {
            this.type = type;
        }

        public void serialize(RowIndexEntry rie, DataOutput out) throws IOException
        {
            out.writeLong(rie.position);
            if (rie.isIndexed())
            {
                out.writeInt(serializedSize(rie));
                type.deletionInfoSerializer().serializeForSSTable(rie.deletionInfo(), out);
                out.writeInt(rie.columnsIndex().size());
                ISerializer<IndexHelper.IndexInfo> idxSerializer = type.indexSerializer();
                for (IndexHelper.IndexInfo info : rie.columnsIndex())
                    idxSerializer.serialize(info, out);
            }
            else
            {
                out.writeInt(0);
            }
        }

        public RowIndexEntry deserialize(DataInput in, Descriptor.Version version) throws IOException
        {
            long position = in.readLong();
            if (version.hasPromotedIndexes)
            {
                int size = in.readInt();
                if (size > 0)
                {
                    DeletionInfo delInfo = type.deletionInfoSerializer().deserializeFromSSTable(in, version);
                    int entries = in.readInt();
                    ISerializer<IndexHelper.IndexInfo> idxSerializer = type.indexSerializer();
                    List<IndexHelper.IndexInfo> columnsIndex = new ArrayList<IndexHelper.IndexInfo>(entries);
                    for (int i = 0; i < entries; i++)
                        columnsIndex.add(idxSerializer.deserialize(in));

                    if (version.hasRowLevelBF)
                        IndexHelper.skipBloomFilter(in, version.filterType);
                    return new IndexedEntry(position, delInfo, columnsIndex);
                }
                else
                {
                    return new RowIndexEntry(position);
                }
            }
            else
            {
                return new RowIndexEntry(position);
            }
        }

        public static void skip(DataInput in, Descriptor.Version version) throws IOException
        {
            in.readLong();
            if (version.hasPromotedIndexes)
                skipPromotedIndex(in);
        }

        public static void skipPromotedIndex(DataInput in) throws IOException
        {
            int size = in.readInt();
            if (size <= 0)
                return;

            FileUtils.skipBytesFully(in, size);
        }

        public int serializedSize(RowIndexEntry rie)
        {
            TypeSizes typeSizes = TypeSizes.NATIVE;
            if (rie.isIndexed())
            {
                IndexedEntry ie = (IndexedEntry)rie;
                long size = DeletionTime.serializer.serializedSize(ie.deletionInfo().getTopLevelDeletion(), typeSizes);
                size += typeSizes.sizeof(ie.columnsIndex().size()); // number of entries
                ISerializer<IndexHelper.IndexInfo> idxSerializer = type.indexSerializer();
                for (IndexHelper.IndexInfo info : ie.columnsIndex())
                    size += idxSerializer.serializedSize(info, typeSizes);

                assert size <= Integer.MAX_VALUE;
                return (int)size;
            }
            else
            {
                return typeSizes.sizeof(rie.position);
            }
        }
    }

    /**
     * An entry in the row index for a row whose columns are indexed.
     */
    private static class IndexedEntry extends RowIndexEntry
    {
        private final DeletionInfo deletionInfo;
        private final List<IndexHelper.IndexInfo> columnsIndex;

        private IndexedEntry(long position, DeletionInfo deletionInfo, List<IndexHelper.IndexInfo> columnsIndex)
        {
            super(position);
            assert deletionInfo != null;
            assert columnsIndex != null && columnsIndex.size() > 1;
            this.deletionInfo = deletionInfo;
            this.columnsIndex = columnsIndex;
        }

        @Override
        public DeletionInfo deletionInfo()
        {
            return deletionInfo;
        }

        @Override
        public List<IndexHelper.IndexInfo> columnsIndex()
        {
            return columnsIndex;
        }
    }
}
