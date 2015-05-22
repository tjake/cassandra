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
package org.apache.cassandra.db.compaction;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.metrics.CompactionMetrics;

public class CompactionIterator extends CompactionInfo.Holder implements UnfilteredPartitionIterator
{
    private static final long UNFILTERED_TO_UPDATE_PROGRESS = 100;

    private final OperationType type;
    private final CompactionController controller;
    private final long totalBytes;
    private volatile long bytesRead;
    private final List<ISSTableScanner> scanners;
    /*
     * counters for merged rows.
     * array index represents (number of merged rows - 1), so index 0 is counter for no merge (1 row),
     * index 1 is counter for 2 rows merged, and so on.
     */
    private final AtomicLong[] mergeCounters;

    private final UnfilteredPartitionIterator mergedIterator;
    private final AtomicInteger merged = new AtomicInteger();
    private final SSTableFormat format;
    private final CompactionMetrics metrics;

    public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, CompactionController controller, SSTableFormat.Type formatType)
    {
        this(type, scanners, controller, formatType, null);
    }

    public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, CompactionController controller, SSTableFormat.Type formatType, CompactionMetrics metrics)
    {
        this.controller = controller;
        this.type = type;
        this.scanners = scanners;
        this.bytesRead = 0;

        long bytes = 0;
        for (ISSTableScanner scanner : scanners)
            bytes += scanner.getLengthInBytes();
        this.totalBytes = bytes;
        mergeCounters = new AtomicLong[scanners.size()];
        for (int i = 0; i < mergeCounters.length; i++)
            mergeCounters[i] = new AtomicLong();

        this.format = formatType.info;
        this.mergedIterator = scanners.isEmpty()
                            ? UnfilteredPartitionIterators.EMPTY
                            : new PurgingPartitionIterator(UnfilteredPartitionIterators.merge(scanners, listener()), controller);

        this.metrics = metrics;

        if (metrics != null)
            metrics.beginCompaction(this);
    }

    public boolean isForThrift()
    {
        return false;
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(controller.cfs.metadata,
                                  type,
                                  bytesRead,
                                  totalBytes);
    }

    protected void updateCounterFor(int rows)
    {
        assert rows > 0 && rows - 1 < mergeCounters.length;
        mergeCounters[rows - 1].incrementAndGet();
    }

    public long[] getMergedRowCounts()
    {
        long[] counters = new long[mergeCounters.length];
        for (int i = 0; i < counters.length; i++)
            counters[i] = mergeCounters[i].get();
        return counters;
    }

    private UnfilteredPartitionIterators.MergeListener listener()
    {
        return new UnfilteredPartitionIterators.MergeListener()
        {
            public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
            {
                int merged = 0;
                int nowInSec = 0;
                for (UnfilteredRowIterator iter : versions)
                {
                    if (iter != null)
                    {
                        merged++;
                        nowInSec = iter.nowInSec();
                    }
                }

                assert merged > 0;

                CompactionIterator.this.updateCounterFor(merged);

                /*
                 * The row level listener does 2 things:
                 *  - It updates 2ndary indexes for deleted/shadowed cells
                 *  - It updates progress regularly (every UNFILTERED_TO_UPDATE_PROGRESS)
                 */
                final SecondaryIndexManager.Updater indexer = controller.cfs.indexManager.gcUpdaterFor(partitionKey, nowInSec);
                return new UnfilteredRowIterators.MergeListener()
                {
                    private Clustering clustering;

                    public void onMergePartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
                    {
                    }

                    public void onMergingRows(Clustering clustering, LivenessInfo mergedInfo, DeletionTime mergedDeletion, Row[] versions)
                    {
                        this.clustering = clustering;
                    }

                    public void onMergedComplexDeletion(ColumnDefinition c, DeletionTime mergedCompositeDeletion, DeletionTime[] versions)
                    {
                    }

                    public void onMergedCells(Cell mergedCell, Cell[] versions)
                    {
                        if (indexer == SecondaryIndexManager.nullUpdater)
                            return;

                        for (int i = 0; i < versions.length; i++)
                        {
                            Cell version = versions[i];
                            if (version != null && (mergedCell == null || !mergedCell.equals(version)))
                                indexer.remove(clustering, version);
                        }
                    }

                    public void onRowDone()
                    {
                        int merged = CompactionIterator.this.merged.incrementAndGet();
                        if (merged % UNFILTERED_TO_UPDATE_PROGRESS == 0)
                            updateBytesRead();
                    }

                    public void onMergedRangeTombstoneMarkers(Slice.Bound bound, DeletionTime mergedDeletion, RangeTombstoneMarker[] versions)
                    {
                        int merged = CompactionIterator.this.merged.incrementAndGet();
                        if (merged % UNFILTERED_TO_UPDATE_PROGRESS == 0)
                            updateBytesRead();
                    }

                    public void close()
                    {
                    }
                };
            }

            public void close()
            {
            }
        };
    }

    private void updateBytesRead()
    {
        long n = 0;
        for (ISSTableScanner scanner : scanners)
            n += scanner.getCurrentPosition();
        bytesRead = n;
    }

    public boolean hasNext()
    {
        return mergedIterator.hasNext();
    }

    public UnfilteredRowIterator next()
    {
        return mergedIterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        try
        {
            mergedIterator.close();
        }
        finally
        {
            if (metrics != null)
                metrics.finishCompaction(this);
        }
    }

    public String toString()
    {
        return this.getCompactionInfo().toString();
    }

    private static class PurgingPartitionIterator extends FilteringPartitionIterator
    {
        private final CompactionController controller;

        private DecoratedKey currentKey;
        private long maxPurgeableTimestamp;
        private boolean hasCalculatedMaxPurgeableTimestamp;

        private PurgingPartitionIterator(UnfilteredPartitionIterator toPurge, CompactionController controller)
        {
            super(toPurge);
            this.controller = controller;
        }

        protected FilteringRow makeRowFilter()
        {
            return new FilteringRow()
            {
                @Override
                protected boolean include(LivenessInfo info)
                {
                    return !info.hasLocalDeletionTime() || !info.isPurgeable(getMaxPurgeableTimestamp(), controller.gcBefore);
                }

                @Override
                protected boolean include(DeletionTime dt)
                {
                    return includeDelTime(dt);
                }

                @Override
                protected boolean include(ColumnDefinition c, DeletionTime dt)
                {
                    return includeDelTime(dt);
                }
            };
        }

        @Override
        protected void onEmpty(DecoratedKey key)
        {
            controller.cfs.invalidateCachedPartition(key);
        }

        private boolean includeDelTime(DeletionTime dt)
        {
            return dt.isLive() || !dt.isPurgeable(getMaxPurgeableTimestamp(), controller.gcBefore);
        }

        @Override
        protected boolean shouldFilter(UnfilteredRowIterator iterator)
        {
            currentKey = iterator.partitionKey();
            hasCalculatedMaxPurgeableTimestamp = false;

            // TODO: we could be able to skip filtering if UnfilteredRowIterator was giving us some stats
            // (like the smallest local deletion time).
            return true;
        }

        @Override
        protected boolean includePartitionDeletion(DeletionTime dt)
        {
            return includeDelTime(dt);
        }

        @Override
        protected boolean includeRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            return includeDelTime(marker.deletionTime());
        }

        /*
         * Tombstones with a localDeletionTime before this can be purged. This is the minimum timestamp for any sstable
         * containing `currentKey` outside of the set of sstables involved in this compaction. This is computed lazily
         * on demand as we only need this if there is tombstones and this a bit expensive (see #8914).
         */
        private long getMaxPurgeableTimestamp()
        {
            if (!hasCalculatedMaxPurgeableTimestamp)
            {
                hasCalculatedMaxPurgeableTimestamp = true;
                maxPurgeableTimestamp = controller.maxPurgeableTimestamp(currentKey);
            }
            return maxPurgeableTimestamp;
        }
    }
}
