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
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.RowStats;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * A SSTable writer that assumes rows are in (partitioner) sorted order.
 * Contrarily to SSTableSimpleUnsortedWriter, this writer does not buffer
 * anything into memory, however it assumes that row are added in sorted order
 * (an exception will be thrown otherwise), which for the RandomPartitioner
 * means that rows should be added by increasing md5 of the row key. This is
 * rarely possible and SSTableSimpleUnsortedWriter should most of the time be
 * prefered.
 */
class SSTableSimpleWriter
{
    protected final File directory;
    protected final CFMetaData metadata;
    protected DecoratedKey currentKey;
    protected PartitionUpdate update;
    protected ByteBuffer currentSuperColumn;
    protected final CounterId counterid = CounterId.generate();
    private SSTableFormat.Type formatType = DatabaseDescriptor.getSSTableFormat();
    protected static AtomicInteger generation = new AtomicInteger(0);
    private SSTableWriter writer_;


    protected SSTableSimpleWriter(File directory, CFMetaData metadata, IPartitioner partitioner)
    {
        this.metadata = metadata;
        this.directory = directory;
        DatabaseDescriptor.setPartitioner(partitioner);

        writer_ = null;
    }

    protected void setSSTableFormatType(SSTableFormat.Type type)
    {
        this.formatType = type;
    }

    private SSTableWriter getOrCreateWriter()
    {
        if (writer_ == null)
            writer_ = createWriter();

        return writer_;
    }

    protected SSTableWriter createWriter()
    {
        return SSTableWriter.create(createDescriptor(directory, metadata.ksName, metadata.cfName, formatType),
                                    0,
                                    ActiveRepairService.UNREPAIRED_SSTABLE,
                                    new SerializationHeader(metadata, metadata.partitionColumns(), RowStats.NO_STATS, true));
    }

    protected static Descriptor createDescriptor(File directory, final String keyspace, final String columnFamily, final SSTableFormat.Type fmt)
    {
        int maxGen = getNextGeneration(directory, columnFamily);
        return new Descriptor(directory, keyspace, columnFamily, maxGen + 1, Descriptor.Type.TEMP, fmt);
    }

    private static int getNextGeneration(File directory, final String columnFamily)
    {
        final Set<Descriptor> existing = new HashSet<>();
        directory.list(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                Pair<Descriptor, Component> p = SSTable.tryComponentFromFilename(dir, name);
                Descriptor desc = p == null ? null : p.left;
                if (desc == null)
                    return false;

                if (desc.cfname.equals(columnFamily))
                    existing.add(desc);

                return false;
            }
        });
        int maxGen = generation.getAndIncrement();
        for (Descriptor desc : existing)
        {
            while (desc.generation > maxGen)
            {
                maxGen = generation.getAndIncrement();
            }
        }
        return maxGen;
    }

    /**
     * Start a new row whose key is {@code key}.
     * @param key the row key
     */
    public void newRow(ByteBuffer key) throws IOException
    {
        if (currentKey != null && !update.isEmpty())
            writePartition(update);

        currentKey = DatabaseDescriptor.getPartitioner().decorateKey(key);
        update = getPartitionUpdate();
    }


    /**
     * Package protected for use by AbstractCQLSSTableWriter.
     * Not meant to be exposed publicly.
     */
    PartitionUpdate currentUpdate()
    {
        return update;
    }

    /**
     * Package protected for use by AbstractCQLSSTableWriter.
     * Not meant to be exposed publicly.
     */
    DecoratedKey currentKey()
    {
        return currentKey;
    }

    /**
     * Package protected for use by AbstractCQLSSTableWriter.
     * Not meant to be exposed publicly.
     */
    boolean shouldStartNewRow() throws IOException
    {
        return currentKey == null;
    }


    public void close() throws IOException
    {
        try
        {
            if (currentKey != null)
                writePartition(update);
            if (writer_ != null)
                writer_.finish(false);
        }
        catch (Throwable t)
        {
            throw Throwables.propagate(writer_ == null ? t : writer_.abort(t));
        }
    }

    protected void writePartition(PartitionUpdate update) throws IOException
    {
        getOrCreateWriter().append(update.unfilteredIterator());
    }

    protected PartitionUpdate getPartitionUpdate() throws IOException
    {
        return new PartitionUpdate(metadata, currentKey, metadata.partitionColumns(), 1, FBUtilities.nowInSeconds());
    }
}
