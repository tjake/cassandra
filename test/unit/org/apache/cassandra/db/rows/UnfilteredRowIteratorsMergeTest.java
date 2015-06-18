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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.Slice.Bound;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Unfiltered.Kind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;

public class UnfilteredRowIteratorsMergeTest
{
    static DecoratedKey partitionKey = Util.dk("key");
    static DeletionTime partitionLevelDeletion = DeletionTime.LIVE;
    static CFMetaData metadata = CFMetaData.Builder.create("UnfilteredRowIteratorsMergeTest", "Test").
            addPartitionKey("key", AsciiType.instance).
            addClusteringColumn("clustering", Int32Type.instance).
            addRegularColumn("data", Int32Type.instance).
            build();
    static Comparator<Clusterable> comparator = new ClusteringComparator(Int32Type.instance);
    static int nowInSec = FBUtilities.nowInSeconds();

    static final int RANGE = 100;
    static final int DEL_RANGE = 100;
    static final int ITERATORS = 3;
    static final int ITEMS = 10;

    public UnfilteredRowIteratorsMergeTest()
    {
    }

    @Test
    public void testTombstoneMerge()
    {
        testTombstoneMerge(false);
    }

    @Test
    public void testTombstoneMergeReversed()
    {
        testTombstoneMerge(true);
    }

    public void testTombstoneMerge(boolean reversed)
    {
        for (int seed = 1; seed <= 1; ++seed)
        {
            System.out.println("\nSeed " + seed);

            Random r = new Random(seed);
            List<List<Unfiltered>> sources = new ArrayList<>(ITERATORS);
            System.out.println("Merging");
            for (int i=0; i<ITERATORS; ++i)
                sources.add(generateSource(r, reversed));
            List<UnfilteredRowIterator> us = sources.stream().map(l -> new Source(l.iterator(), reversed)).collect(Collectors.toList());
            List<Unfiltered> merged = new ArrayList<>();
            Iterators.addAll(merged, safeIterator(UnfilteredRowIterators.merge(us)));
    
            System.out.println("results in");
            dumpList(merged);
            verifyEquivalent(sources, merged, reversed);
            if (reversed)
                Collections.reverse(merged);

            verifyValid(merged);
        }
    }

    private static List<Unfiltered> generateSource(Random r, boolean reversed)
    {
        int[] positions = new int[ITEMS + 1];
        for (int i=0; i<ITEMS; ++i)
            positions[i] = r.nextInt(RANGE);
        positions[ITEMS] = RANGE;
        Arrays.sort(positions);

        List<Unfiltered> content = new ArrayList<>(ITEMS);
        int prev = -1;
        for (int i=0; i<ITEMS; ++i)
        {
            int pos = positions[i];
            int sz = positions[i + 1] - pos;
            if (sz > 0 && r.nextBoolean())
            {
                int span = pos > prev ? r.nextInt(sz + 1) : 1 + r.nextInt(sz);
                int deltime = r.nextInt(DEL_RANGE);
                DeletionTime dt = new SimpleDeletionTime(deltime, deltime);
                content.add(new SimpleRangeTombstoneMarker(boundFor(pos, true, pos > prev ? (span > 0 ? r.nextBoolean() : true) : false), dt));
                content.add(new SimpleRangeTombstoneMarker(boundFor(pos + span, false, span < sz ? (span > 0 ? r.nextBoolean() : true) : false), dt));
            }
            else
            {
                content.add(emptyRowAt(pos));
            }
            prev = pos;
        }
        // TODO: Remove some close bounds for implicit close case.
        verifyValid(content);
        if (reversed)
            Collections.reverse(content);
        dumpList(content);
        return content;
    }

    static void verifyValid(List<Unfiltered> list)
    {
        try {
            Unfiltered prev = null;
            for (Unfiltered curr : list)
            {
                if (prev == null)
                {
                    prev = curr;
                    continue;
                }
                
                Assert.assertTrue(str(prev) + " not ordered before " + str(curr), comparator.compare(prev, curr) <= 0);
                if (curr.kind() == Kind.ROW)
                {
                    if (prev.kind() != Kind.ROW)
                        Assert.assertFalse("Row succeeds open marker " + str(prev), ((RangeTombstoneMarker) prev).clustering().isStart());
                }
                else
                {
                    Assert.assertFalse("Duplicate marker " + str(curr), prev.equals(curr));
                        
                    Bound currBound = (Bound) curr.clustering();
                    if (prev.kind() == Kind.ROW)
                    {
                        Assert.assertTrue(str(curr) + " should be preceded by open marker, found " + str(prev), currBound.isStart());
                    }
                    else
                    {
                        Bound prevBound = (Bound) prev.clustering();

                        Assert.assertFalse("Deletion time for " + str(curr) + " shouldn't be live", ((RangeTombstoneMarker)curr).deletionTime().isLive());
                        
                        if (currBound.isEnd())
                        {
                            Assert.assertTrue(str(curr) + " should be preceded by open marker, found " + str(prev), prevBound.isStart());
                            Assert.assertEquals("Deletion time mismatch for open " + str(prev) + " and close " + str(curr),
                                                ((RangeTombstoneMarker) prev).deletionTime(),
                                                ((RangeTombstoneMarker) curr).deletionTime());
                        }
                        if (currBound.isStart())                    // Remove for implicit close.
                            Assert.assertTrue(str(curr) + " follows another open marker " + str(prev), prevBound.isEnd());
                    }
                    
                }
                prev = curr;
            }
            Assert.assertFalse("Cannot end in open marker " + str(prev), prev.kind() == Kind.RANGE_TOMBSTONE_MARKER && ((Bound) prev.clustering()).isStart());
    
            } catch (AssertionError e) {
            System.out.println(e);
            dumpList(list);
            throw e;
        }
    }

    static void verifyEquivalent(List<List<Unfiltered>> sources, List<Unfiltered> merged, boolean reversed)
    {
        try
        {
            for (int i=0; i<RANGE; ++i)
            {
                Clusterable c = clusteringFor(i);
                DeletionTime dt = DeletionTime.LIVE;
                for (List<Unfiltered> source : sources)
                {
                    DeletionTime sdt = deletionFor(c, source, reversed);
                    if (sdt.supersedes(dt))
                        dt = sdt;
                }
                Assert.assertEquals("Deletion time mismatch for position " + str(c), dt, deletionFor(c, merged, reversed));
                if (dt == DeletionTime.LIVE)
                {
                    Optional<Unfiltered> sourceOpt = sources.stream().map(source -> rowFor(c, source, reversed)).filter(x -> x != null).findAny();
                    Unfiltered mergedRow = rowFor(c, merged, reversed);
                    Assert.assertEquals("Content mismatch for position " + str(c), sourceOpt.orElse(null), mergedRow);
                }
            }
        }
        catch (AssertionError e)
        {
            System.out.println(e);
            for (List<Unfiltered> list : sources)
                dumpList(list);
            System.out.println("merged");
            dumpList(merged);
            throw e;
        }
    }

    private static Unfiltered rowFor(Clusterable pointer, List<Unfiltered> list, boolean reversed)
    {
        int index = Collections.binarySearch(list, pointer, reversed ? comparator.reversed() : comparator);
        return index >= 0 ? list.get(index) : null;
    }

    static DeletionTime deletionFor(Clusterable pointer, List<Unfiltered> list, boolean reversed)
    {
        return deletionFor(pointer, list, DeletionTime.LIVE, reversed);
    }

    static DeletionTime deletionFor(Clusterable pointer, List<Unfiltered> list, DeletionTime def, boolean reversed)
    {
        if (list.isEmpty())
            return def;

        int index = Collections.binarySearch(list, pointer, reversed ? comparator.reversed() : comparator);
        if (index >= 0)     // Found equal row. Bounds cannot equal SimpleClustering.
        {
            Row row = (Row) list.get(index);
            return row.deletion() != null ? row.deletion() : def;
        }
        index = -1 - index;
        if (reversed)
            index += 1;

        if (index == 0 || index >= list.size())
            return def;

        Unfiltered unfiltered = list.get(index - 1);
        if (unfiltered.kind() == Kind.ROW)
            return def;
        RangeTombstoneMarker lower = (RangeTombstoneMarker) unfiltered;
        if (lower.clustering().isEnd())
            return def;
        return lower.deletionTime();
    }

    private static Bound boundFor(int pos, boolean start, boolean inclusive)
    {
        return Bound.create(Bound.boundKind(start, inclusive), new ByteBuffer[] {Int32Type.instance.decompose(pos)});
    }

    private static SimpleClustering clusteringFor(int i)
    {
        return new SimpleClustering(Int32Type.instance.decompose(i));
    }

    static Row emptyRowAt(int pos)
    {
        final Clustering clustering = clusteringFor(pos);
        // Using liveness time of -1 to make rows covered by tombstone ranges always disappear.
        final LivenessInfo live = SimpleLivenessInfo.forUpdate(-1, 0, nowInSec, metadata);
        return emptyRowAt(clustering, live);
    }

    public static class TestCell extends AbstractCell
    {
        private final ColumnDefinition column;
        private final ByteBuffer value;
        private final LivenessInfo info;

        public TestCell(ColumnDefinition column, ByteBuffer value, LivenessInfo info)
        {
            this.column = column;
            this.value = value;
            this.info = info.takeAlias();
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public boolean isCounterCell()
        {
            return false;
        }

        public ByteBuffer value()
        {
            return value;
        }

        public LivenessInfo livenessInfo()
        {
            return info;
        }

        public CellPath path()
        {
            return null;
        }
    }

    static Row emptyRowAt(final Clustering clustering, final LivenessInfo live)
    {
        final ColumnDefinition columnDef = metadata.getColumnDefinition(new ColumnIdentifier("data", true));
        final Cell cell = new TestCell(columnDef, clustering.get(0), live);
        
        return new AbstractRow()
        {
            public Columns columns()
            {
                return Columns.of(columnDef);
            }

            public LivenessInfo primaryKeyLivenessInfo()
            {
                return live;
            }

            public long maxLiveTimestamp()
            {
                return live.timestamp();
            }

            public int nowInSec()
            {
                return nowInSec;
            }

            public DeletionTime deletion()
            {
                return DeletionTime.LIVE;
            }

            public boolean isEmpty()
            {
                return true;
            }

            public boolean hasComplexDeletion()
            {
                return false;
            }

            public Clustering clustering()
            {
                return clustering;
            }

            public Cell getCell(ColumnDefinition c)
            {
                return c == columnDef ? cell : null;
            }

            public Cell getCell(ColumnDefinition c, CellPath path)
            {
                return null;
            }

            public Iterator<Cell> getCells(ColumnDefinition c)
            {
                return Iterators.singletonIterator(cell);
            }

            public DeletionTime getDeletion(ColumnDefinition c)
            {
                return DeletionTime.LIVE;
            }

            public Iterator<Cell> iterator()
            {
                return Iterators.<Cell>emptyIterator();
            }

            public SearchIterator<ColumnDefinition, ColumnData> searchIterator()
            {
                return new SearchIterator<ColumnDefinition, ColumnData>()
                {
                    public boolean hasNext()
                    {
                        return false;
                    }

                    public ColumnData next(ColumnDefinition column)
                    {
                        return null;
                    }
                };
            }

            public Kind kind()
            {
                return Unfiltered.Kind.ROW;
            }

            public Row takeAlias()
            {
                return this;
            }

            public String toString()
            {
                return Int32Type.instance.getString(clustering.get(0));
            }
        };

    }

    private static void dumpList(List<Unfiltered> list)
    {
        for (Unfiltered u : list)
            System.out.print(str(u) + " ");
        System.out.println();
    }

    private static String str(Clusterable prev)
    {
        String val = Int32Type.instance.getString(prev.clustering().get(0));
        if (prev instanceof RangeTombstoneMarker)
        {
            Bound b = (Bound) prev.clustering();
            if (b.isStart()) 
                val = val + (b.isInclusive() ? "<=" : "<") + "[" + ((RangeTombstoneMarker) prev).deletionTime().markedForDeleteAt() + "]";
            else
                val = (b.isInclusive() ? "<=" : "<") + val + "[" + ((RangeTombstoneMarker) prev).deletionTime().markedForDeleteAt() + "]";
        }
        return val;
    }

    static class Source extends AbstractUnfilteredRowIterator implements UnfilteredRowIterator
    {
        Iterator<Unfiltered> content;

        protected Source(Iterator<Unfiltered> content, boolean reversed)
        {
            super(UnfilteredRowIteratorsMergeTest.metadata,
                  UnfilteredRowIteratorsMergeTest.partitionKey,
                  UnfilteredRowIteratorsMergeTest.partitionLevelDeletion,
                  UnfilteredRowIteratorsMergeTest.metadata.partitionColumns(),
                  null,
                  reversed,
                  RowStats.NO_STATS,
                  UnfilteredRowIteratorsMergeTest.nowInSec);
            this.content = content;
        }

        @Override
        protected Unfiltered computeNext()
        {
            return content.hasNext() ? content.next() : endOfData();
        }
    }

    static DeletionTime safeDeletionTime(RangeTombstoneMarker m)
    {
        return new SimpleDeletionTime(m.deletionTime().markedForDeleteAt(), m.deletionTime().localDeletionTime());
    }

    static private Bound safeBound(Bound clustering)
    {
        return Bound.create(clustering.kind(), new ByteBuffer[] {clustering.get(0)});
    }

    public static UnfilteredRowIterator safeIterator(UnfilteredRowIterator iterator)
    {
        return new WrappingUnfilteredRowIterator(iterator)
        {
            public Unfiltered next()
            {
                Unfiltered next = super.next();
                return next.kind() == Unfiltered.Kind.ROW
                     ? emptyRowAt(Int32Type.instance.compose(next.clustering().get(0)))
                     : new SimpleRangeTombstoneMarker(safeBound((Bound) next.clustering()), safeDeletionTime((RangeTombstoneMarker)next));
            }
        };
    }


}
