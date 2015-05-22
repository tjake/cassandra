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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.atoms.AtomIterator;
import org.apache.cassandra.db.atoms.Cell;
import org.apache.cassandra.db.atoms.CellPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * A SSTable writer that doesn't assume rows are in sorted order.
 * This writer buffers rows in memory and then write them all in sorted order.
 * To avoid loading the entire data set in memory, the amount of rows buffered
 * is configurable. Each time the threshold is met, one SSTable will be
 * created (and the buffer be reseted).
 *
 * @see SSTableSimpleWriter
 */
class SSTableSimpleUnsortedWriter extends SSTableSimpleWriter
{
    private static final Buffer SENTINEL = new Buffer();

    private Buffer buffer = new Buffer();
    private final long bufferSize;
    private long currentSize;

    private boolean needsSync = false;

    private final BlockingQueue<Buffer> writeQueue = new SynchronousQueue<Buffer>();
    private final DiskWriter diskWriter = new DiskWriter();

    SSTableSimpleUnsortedWriter(File directory, CFMetaData metadata, IPartitioner partitioner, long bufferSizeInMB)
    {
        super(directory, metadata, partitioner);
        bufferSize = bufferSizeInMB * 1024L * 1024L;
        diskWriter.start();
    }

    protected void replaceColumnFamily() throws IOException
    {
        needsSync = true;
    }


    protected void writePartition(PartitionUpdate update) throws IOException
    {
        buffer.put(update.partitionKey(), update);
    }

    protected void countColumn(long cellSize) throws IOException
    {
        currentSize += cellSize;

        // We don't want to sync in writeRow() only as this might blow up the bufferSize for wide rows.
        if (currentSize > bufferSize)
            sync();
    }

    @Override
    protected PartitionUpdate getPartitionUpdate() throws IOException
    {
        PartitionUpdate previous = buffer.get(currentKey);
        // If the CF already exist in memory, we'll just continue adding to it
        if (previous == null)
        {
            previous = createPartitionUpdate();
            buffer.put(currentKey, previous);

            // Since this new CF will be written by the next sync(), count its header. And a CF header
            // on disk is:
            //   - the row key: 2 bytes size + key size bytes
            //   - the row level deletion infos: 4 + 8 bytes
            currentSize += 14 + currentKey.getKey().remaining();
        }

        // We don't want to sync in writeRow() only as this might blow up the bufferSize for wide rows.
        if (currentSize > bufferSize)
            replaceColumnFamily();

        return previous;
    }

    protected PartitionUpdate createPartitionUpdate()
    {
        return new PartitionUpdate(metadata, currentKey, metadata.partitionColumns(), 1, FBUtilities.nowInSeconds())
        {
            @Override
            protected Writer createWriter()
            {
                return new RegularWriter()
                {
                    @Override
                    public void writeCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, LivenessInfo info, CellPath path)
                    {
                        super.writeCell(column, isCounter, value, info, path);
                        try
                        {
                            countColumn(value.remaining());
                        }
                        catch (IOException e)
                        {
                            // addColumn does not throw IOException but we want to report this to the user,
                            // so wrap it in a temporary RuntimeException that we'll catch in rawAddRow above.
                            throw new SyncException(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public void close() throws IOException
    {
        sync();
        put(SENTINEL);
        try
        {
            diskWriter.join();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        checkForWriterException();
    }

    protected void sync() throws IOException
    {
        if (buffer.isEmpty())
            return;

        update = null;
        put(buffer);
        buffer = new Buffer();
        currentSize = 0;
        update = getPartitionUpdate();
    }

    private void put(Buffer buffer) throws IOException
    {
        while (true)
        {
            checkForWriterException();
            try
            {
                if (writeQueue.offer(buffer, 1, TimeUnit.SECONDS))
                    break;
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * If we have marked that the column family is being replaced, when we start the next row,
     * we should sync out the previous partition and create a new row based on the current value.
     */
    @Override
    boolean shouldStartNewRow() throws IOException
    {
        if (needsSync)
        {
            needsSync = false;
            sync();
            return true;
        }
        return super.shouldStartNewRow();
    }

    private void checkForWriterException() throws IOException
    {
        // slightly lame way to report exception from the writer, but that should be good enough
        if (diskWriter.exception != null)
        {
            if (diskWriter.exception instanceof IOException)
                throw (IOException) diskWriter.exception;
            else
                throw Throwables.propagate(diskWriter.exception);
        }
    }

    static class SyncException extends RuntimeException
    {
        SyncException(IOException ioe)
        {
            super(ioe);
        }
    }

    //// typedef
    static class Buffer extends TreeMap<DecoratedKey, PartitionUpdate> {}

    private class DiskWriter extends Thread
    {
        volatile Throwable exception = null;

        public void run()
        {
            while (true)
            {
                try
                {
                    Buffer b = writeQueue.take();
                    if (b == SENTINEL)
                        return;

                    try (SSTableWriter writer = createWriter())
                    {
                        boolean first = true;
                        for (Map.Entry<DecoratedKey, PartitionUpdate> entry : b.entrySet())
                        {
                            if (!entry.getValue().isEmpty())
                                writer.append(entry.getValue().atomIterator());
                            else if (!first)
                                throw new AssertionError("Empty partition");
                            first = false;
                        }
                        writer.finish(false);
                    }
                }
                catch (Throwable e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    // Keep only the first exception
                    if (exception == null)
                        exception = e;
                }
            }
        }
    }
}
