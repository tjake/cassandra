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
package org.apache.cassandra.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

import com.google.common.primitives.Longs;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

public class SerializingCacheProvider
{
    public ICache<RowCacheKey, IRowCacheEntry> create(long capacity)
    {
        return SerializingCache.create(capacity, new RowCacheSerializer());
    }

    public ICache<ChunkCacheKey, ChunkCacheEntry> createChunkCache(long capacity)
    {
        return SerializingCache.create(capacity, new ChunkCacheSerializer());
    }

    // Package protected for tests
    static class RowCacheSerializer implements ISerializer<IRowCacheEntry>
    {
        public void serialize(IRowCacheEntry entry, DataOutput out) throws IOException
        {
            assert entry != null; // unlike CFS we don't support nulls, since there is no need for that in the cache
            boolean isSentinel = entry instanceof RowCacheSentinel;
            out.writeBoolean(isSentinel);
            if (isSentinel)
                out.writeLong(((RowCacheSentinel) entry).sentinelId);
            else
                ColumnFamily.serializer.serialize((ColumnFamily) entry, out, MessagingService.current_version);
        }

        public IRowCacheEntry deserialize(DataInput in) throws IOException
        {
            boolean isSentinel = in.readBoolean();
            if (isSentinel)
                return new RowCacheSentinel(in.readLong());
            return ColumnFamily.serializer.deserialize(in, MessagingService.current_version);
        }

        public long serializedSize(IRowCacheEntry entry, TypeSizes typeSizes)
        {
            int size = typeSizes.sizeof(true);
            if (entry instanceof RowCacheSentinel)
                size += typeSizes.sizeof(((RowCacheSentinel) entry).sentinelId);
            else
                size += ColumnFamily.serializer.serializedSize((ColumnFamily) entry, typeSizes, MessagingService.current_version);
            return size;
        }
    }

    static class ChunkCacheSerializer implements ISerializer<ChunkCacheEntry>
    {
        public void serialize(ChunkCacheEntry entry, DataOutput out) throws IOException
        {
            assert entry != null;

            ByteBufferUtil.writeWithLength(entry.uncompressedChunk, out);

            if (entry.stats == null || entry.stats.isEmpty())
            {
                ByteBufferUtil.writeWithLength(ByteBufferUtil.EMPTY_BYTE_BUFFER,out);
            }
            else
            {
                ByteBuffer statsBuf = ByteBuffer.allocate(Longs.BYTES * entry.stats.size());

                for (int i=0; i<entry.stats.size(); i++)
                {
                    statsBuf.putLong(entry.stats.get(i));
                }

                statsBuf.flip();

                ByteBufferUtil.writeWithLength(statsBuf,out);
            }
        }

        public ChunkCacheEntry deserialize(DataInput in) throws IOException
        {
            byte[] chunk = ByteBufferUtil.readWithLength(in).array();
            ByteBuffer statsBuf = ByteBufferUtil.readWithLength(in);

            if (statsBuf.remaining() == 0)
                return new ChunkCacheEntry(chunk, null);

            LinkedList<Long> stats = new LinkedList<Long>();

            while (statsBuf.remaining() != 0)
                stats.add(statsBuf.getLong());

            return new ChunkCacheEntry(chunk, stats);

        }

        public long serializedSize(ChunkCacheEntry entry, TypeSizes typeSizes)
        {
            int siz = typeSizes.sizeofWithLength(ByteBuffer.wrap(entry.uncompressedChunk));


            if (entry.stats != null)
            {
                int ssiz = Longs.BYTES * entry.stats.size();
                ssiz += typeSizes.sizeof(ssiz);

                siz += ssiz;
            }

            return siz+1;
        }
    }
}
