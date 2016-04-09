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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.primitives.Ints;

import io.netty.util.Recycler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Column index builder used by {@link org.apache.cassandra.io.sstable.format.big.BigTableWriter}.
 * For index entries that exceed {@link org.apache.cassandra.config.Config#column_index_cache_size_in_kb},
 * this uses the serialization logic as in {@link RowIndexEntry}.
 */
public class ColumnIndex implements AutoCloseable
{

    private static final Recycler<ColumnIndex> recycler = new Recycler<ColumnIndex>()
    {
        protected ColumnIndex newObject(Handle handle)
        {
            return new ColumnIndex(handle);
        }
    };

    // used, if the row-index-entry reaches config column_index_cache_size_in_kb
    private DataOutputBuffer buffer;
    // used to track the size of the serialized size of row-index-entry (unused for buffer)
    private int indexSamplesSerializedSize;
    // used, until the row-index-entry reaches config column_index_cache_size_in_kb
    private List<IndexInfo> indexSamples = new ArrayList<>();

    public int columnIndexCount;
    private int[] indexOffsets;

    private SerializationHeader header;
    private int version;
    private SequentialWriter writer;
    private long initialPosition;
    private  ISerializer<IndexInfo> idxSerializer;
    public long headerLength;
    private long startPosition;

    private int written;
    private long previousRowStart;

    private ClusteringPrefix firstClustering;
    private ClusteringPrefix lastClustering;

    private DeletionTime openMarker;

    private Collection<SSTableFlushObserver> observers;

    private final Recycler.Handle recycleHandle;

    public static ColumnIndex create(SerializationHeader header,
                       SequentialWriter writer,
                       Version version,
                       Collection<SSTableFlushObserver> observers,
                       ISerializer<IndexInfo> indexInfoSerializer)
    {

        ColumnIndex index = recycler.get();

        index.header = header;
        index.idxSerializer = indexInfoSerializer;
        index.writer = writer;
        index.version = version.correspondingMessagingVersion();
        index.observers = observers;
        index.initialPosition = writer.position();
        index.headerLength = -1;
        index.startPosition = -1;
        index.previousRowStart = 0;
        index.columnIndexCount = 0;
        index.written = 0;
        index.indexSamplesSerializedSize = 0;

        return index;
    }

    public ColumnIndex(Recycler.Handle handle)
    {
        this.recycleHandle = handle;
    }

    public void close()
    {
        indexSamples.clear();
        firstClustering = null;
        lastClustering = null;
        openMarker = null;
        buffer = null;

        recycler.recycle(this, recycleHandle);
    }

    public void buildRowIndex(UnfilteredRowIterator iterator) throws IOException
    {
        writePartitionHeader(iterator);
        this.headerLength = writer.position() - initialPosition;

        while (iterator.hasNext())
            add(iterator.next());

        finish();
    }

    private void writePartitionHeader(UnfilteredRowIterator iterator) throws IOException
    {
        ByteBufferUtil.writeWithShortLength(iterator.partitionKey().getKey(), writer);
        DeletionTime.serializer.serialize(iterator.partitionLevelDeletion(), writer);
        if (header.hasStatic())
        {
            Row staticRow = iterator.staticRow();

            UnfilteredSerializer.serializer.serializeStaticRow(staticRow, header, writer, version);
            if (!observers.isEmpty())
                observers.forEach((o) -> o.nextUnfilteredCluster(staticRow));
        }
    }

    private long currentPosition()
    {
        return writer.position() - initialPosition;
    }

    public ByteBuffer buffer()
    {
        return buffer != null ? buffer.buffer() : null;
    }

    public List<IndexInfo> indexSamples()
    {
        if (indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0) <= DatabaseDescriptor.getColumnIndexCacheSize())
        {
            return indexSamples;
        }

        return null;
    }

    public int[] offsets()
    {
        return indexOffsets != null
               ? Arrays.copyOf(indexOffsets, columnIndexCount)
               : null;
    }

    private void addIndexBlock() throws IOException
    {
        IndexInfo cIndexInfo = new IndexInfo(firstClustering,
                                             lastClustering,
                                             startPosition,
                                             currentPosition() - startPosition,
                                             openMarker);

        // indexOffsets is used for both shallow (ShallowIndexedEntry) and non-shallow IndexedEntry.
        // For shallow ones, we need it to serialize the offsts in finish().
        // For non-shallow ones, the offsts are passed into IndexedEntry, so we don't have to
        // calculate the offsets again.

        // indexOffsets contains the offsets of the serialized IndexInfo objects.
        // I.e. indexOffsets[0] is always 0 so we don't have to deal with a special handling
        // for index #0 and always subtracting 1 for the index (which could be error-prone).
        if (indexOffsets == null)
            indexOffsets = new int[10];
        else
        {
            if (columnIndexCount >= indexOffsets.length)
                indexOffsets = Arrays.copyOf(indexOffsets, indexOffsets.length + 10);

            //This object is recycled so we need to ensure
            //the 0th element is always 0
            if (columnIndexCount == 0)
            {
                indexOffsets[columnIndexCount] = 0;
            }
            else
            {
                indexOffsets[columnIndexCount] =
                buffer != null
                ? Ints.checkedCast(buffer.position())
                : indexSamplesSerializedSize;
            }
        }
        columnIndexCount++;

        // First, we collect the IndexInfo objects until we reach Config.column_index_cache_size_in_kb in an ArrayList.
        // When column_index_cache_size_in_kb is reached, we switch to byte-buffer mode.
        if (buffer == null)
        {
            indexSamplesSerializedSize += idxSerializer.serializedSize(cIndexInfo);
            if (indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0) > DatabaseDescriptor.getColumnIndexCacheSize())
            {
                buffer = new DataOutputBuffer(DatabaseDescriptor.getColumnIndexCacheSize() * 2);
                for (IndexInfo indexSample : indexSamples)
                {
                    idxSerializer.serialize(indexSample, buffer);
                }
            }
            else
            {
                indexSamples.add(cIndexInfo);
            }
        }
        // don't put an else here...
        if (buffer != null)
        {
            idxSerializer.serialize(cIndexInfo, buffer);
        }

        firstClustering = null;
    }

    private void add(Unfiltered unfiltered) throws IOException
    {
        long pos = currentPosition();

        if (firstClustering == null)
        {
            // Beginning of an index block. Remember the start and position
            firstClustering = unfiltered.clustering();
            startPosition = pos;
        }

        UnfilteredSerializer.serializer.serialize(unfiltered, header, writer, pos - previousRowStart, version);

        // notify observers about each new row
        if (!observers.isEmpty())
            observers.forEach((o) -> o.nextUnfilteredCluster(unfiltered));

        lastClustering = unfiltered.clustering();
        previousRowStart = pos;
        ++written;

        if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        // if we hit the column index size that we have to index after, go ahead and index it.
        if (currentPosition() - startPosition >= DatabaseDescriptor.getColumnIndexSize())
            addIndexBlock();
    }

    private void finish() throws IOException
    {
        UnfilteredSerializer.serializer.writeEndOfPartition(writer);

        // It's possible we add no rows, just a top level deletion
        if (written == 0)
            return;

        // the last column may have fallen on an index boundary already.  if not, index it explicitly.
        if (firstClustering != null)
            addIndexBlock();

        // If we serialize the IndexInfo objects directly in the code above into 'buffer',
        // we have to write the offsts to these here. The offsets have already been are collected
        // in indexOffsets[]. buffer is != null, if it exceeds Config.column_index_cache_size_in_kb.
        // In the other case, when buffer==null, the offsets are serialized in RowIndexEntry.IndexedEntry.serialize().
        if (buffer != null)
            RowIndexEntry.Serializer.serializeOffsets(buffer, indexOffsets, columnIndexCount);

        // we should always have at least one computed index block, but we only write it out if there is more than that.
        assert columnIndexCount > 0 && headerLength >= 0;
    }

    public int indexInfoSerializedSize()
    {
        return buffer != null
               ? buffer.buffer().limit()
               : indexSamplesSerializedSize + columnIndexCount * TypeSizes.sizeof(0);
    }
}
