package org.apache.cassandra.io.sstable.format.test;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.ICompactionScanner;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.test.iterators.NameFilteredIterator;
import org.apache.cassandra.io.sstable.format.test.iterators.SliceFilteredIterator;
import org.apache.cassandra.io.sstable.format.test.iterators.TestTableScanner;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by jake on 7/28/14.
 */
public class TestTableReader extends SSTableReader
{

    private static final Logger logger = LoggerFactory.getLogger(TestTableReader.class);

    TestTableReader(Descriptor desc, Set<Component> components, CFMetaData metadata, IPartitioner partitioner, Long maxDataAge, StatsMetadata sstableMetadata, Boolean isOpenEarly)
    {
        super(desc, components, metadata, partitioner, maxDataAge, sstableMetadata, isOpenEarly);
    }

    @Override
    public RowIndexEntry getPosition(RowPosition key, Operator op, boolean updateCacheAndStats)
    {
        // first, check bloom filter
        if (op == Operator.EQ)
        {
            assert key instanceof DecoratedKey; // EQ only make sense if the key is a valid row key
            if (!bf.isPresent(((DecoratedKey)key).getKey()))
            {
                Tracing.trace("Bloom filter allows skipping sstable {}", descriptor.generation);
                return null;
            }
        }

        // next, the key cache (only make sense for valid row key)
        if ((op == Operator.EQ || op == Operator.GE) && (key instanceof DecoratedKey))
        {
            DecoratedKey decoratedKey = (DecoratedKey)key;
            KeyCacheKey cacheKey = new KeyCacheKey(metadata.cfId, descriptor, decoratedKey.getKey());
            RowIndexEntry cachedPosition = getCachedPosition(cacheKey, updateCacheAndStats);
            if (cachedPosition != null)
            {
                Tracing.trace("Key cache hit for sstable {}", descriptor.generation);
                return cachedPosition;
            }
        }

        // check the smallest and greatest keys in the sstable to see if it can't be present
        if (first.compareTo(key) > 0 || last.compareTo(key) < 0)
        {
            if (op == Operator.EQ && updateCacheAndStats)
                bloomFilterTracker.addFalsePositive();

            if (op.apply(1) < 0)
            {
                Tracing.trace("Check against min and max keys allows skipping sstable {}", descriptor.generation);
                return null;
            }
        }

        int binarySearchResult = indexSummary.binarySearch(key);
        long sampledPosition = getIndexScanPositionFromBinarySearchResult(binarySearchResult, indexSummary);
        int sampledIndex = getIndexSummaryIndexFromBinarySearchResult(binarySearchResult);

        // if we matched the -1th position, we'll start at the first position
        sampledPosition = sampledPosition == -1 ? 0 : sampledPosition;

        int effectiveInterval = indexSummary.getEffectiveIndexIntervalAfterIndex(sampledIndex);

        // scan the on-disk index, starting at the nearest sampled position.
        // The check against IndexInterval is to be exit the loop in the EQ case when the key looked for is not present
        // (bloom filter false positive). But note that for non-EQ cases, we might need to check the first key of the
        // next index position because the searched key can be greater the last key of the index interval checked if it
        // is lesser than the first key of next interval (and in that case we must return the position of the first key
        // of the next interval).
        int i = 0;
        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition);
        while (segments.hasNext() && i <= effectiveInterval)
        {
            FileDataInput in = segments.next();
            try
            {
                while (!in.isEOF() && i <= effectiveInterval)
                {
                    i++;

                    ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);

                    boolean opSatisfied; // did we find an appropriate position for the op requested
                    boolean exactMatch; // is the current position an exact match for the key, suitable for caching

                    // Compare raw keys if possible for performance, otherwise compare decorated keys.
                    if (op == Operator.EQ)
                    {
                        opSatisfied = exactMatch = indexKey.equals(((DecoratedKey) key).getKey());
                    }
                    else
                    {
                        DecoratedKey indexDecoratedKey = partitioner.decorateKey(indexKey);
                        int comparison = indexDecoratedKey.compareTo(key);
                        int v = op.apply(comparison);
                        opSatisfied = (v == 0);
                        exactMatch = (comparison == 0);
                        if (v < 0)
                        {
                            Tracing.trace("Partition index lookup allows skipping sstable {}", descriptor.generation);
                            return null;
                        }
                    }

                    if (opSatisfied)
                    {
                        // read data position from index entry
                        RowIndexEntry indexEntry = rowIndexEntrySerializer.deserialize(in, descriptor.version);
                        if (exactMatch && updateCacheAndStats)
                        {
                            assert key instanceof DecoratedKey; // key can be == to the index key only if it's a true row key
                            DecoratedKey decoratedKey = (DecoratedKey)key;

                            if (logger.isTraceEnabled())
                            {
                                // expensive sanity check!  see CASSANDRA-4687
                                FileDataInput fdi = dfile.getSegment(indexEntry.position);
                                DecoratedKey keyInDisk = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(fdi));
                                if (!keyInDisk.equals(key))
                                    throw new AssertionError(String.format("%s != %s in %s", keyInDisk, key, fdi.getPath()));
                                fdi.close();
                            }

                            // store exact match for the key
                            cacheKey(decoratedKey, indexEntry);
                        }
                        if (op == Operator.EQ && updateCacheAndStats)
                            bloomFilterTracker.addTruePositive();
                        Tracing.trace("Partition index with {} entries found for sstable {}", indexEntry.columnsIndex().size(), descriptor.generation);
                        return indexEntry;
                    }

                    RowIndexEntry.Serializer.skip(in);
                }
            }
            catch (IOException e)
            {
                markSuspect();
                throw new CorruptSSTableException(e, in.getPath());
            }
            finally
            {
                FileUtils.closeQuietly(in);
            }
        }

        if (op == SSTableReader.Operator.EQ && updateCacheAndStats)
            bloomFilterTracker.addFalsePositive();
        Tracing.trace("Partition index lookup complete (bloom filter false positive) for sstable {}", descriptor.generation);
        return null;
    }

    @Override
    public OnDiskAtomIterator iterator(DecoratedKey key, SortedSet<CellName> columns)
    {
        return new NameFilteredIterator(this, key, columns);
    }

    @Override
    public OnDiskAtomIterator iterator(FileDataInput input, DecoratedKey key, SortedSet<CellName> columns, RowIndexEntry indexEntry)
    {
        return new NameFilteredIterator(this, input, key, columns, indexEntry);
    }

    @Override
    public OnDiskAtomIterator iterator(DecoratedKey key, ColumnSlice[] slices, boolean reverse)
    {
        if (reverse)
            throw new UnsupportedOperationException();

        return new SliceFilteredIterator(this, key, slices);
    }

    @Override
    public OnDiskAtomIterator iterator(FileDataInput input, DecoratedKey key, ColumnSlice[] slices, boolean reverse, RowIndexEntry indexEntry)
    {
        if (reverse)
            throw new UnsupportedOperationException();

        return new SliceFilteredIterator(this, input, key, slices, indexEntry);
    }

    @Override
    public ICompactionScanner getScanner(Collection<Range<Token>> ranges, RateLimiter limiter)
    {
        // We want to avoid allocating a SSTableScanner if the range don't overlap the sstable (#5249)
        List<Pair<Long, Long>> positions = getPositionsForRanges(Range.normalize(ranges));
        if (positions.isEmpty())
            return new EmptyCompactionScanner(getFilename());
        else
            return new TestTableScanner(this, ranges, limiter);
    }

    @Override
    public ICompactionScanner getScanner(DataRange dataRange, RateLimiter limiter)
    {
        return new TestTableScanner(this, dataRange, limiter);
    }
}
