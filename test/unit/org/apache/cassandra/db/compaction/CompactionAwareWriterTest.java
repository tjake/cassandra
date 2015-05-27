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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.primitives.Longs;
import org.junit.*;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MajorLeveledCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.compaction.writers.SplittingSizeTieredCompactionWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import static org.junit.Assert.assertEquals;

public class CompactionAwareWriterTest extends CQLTester
{
    private static final String KEYSPACE = "cawt_keyspace";
    private static final String TABLE = "cawt_table";

    private static final int ROW_PER_PARTITION = 10;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        // Disabling durable write since we don't care
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes=false");
        schemaChange(String.format("CREATE TABLE %s.%s (k int, t int, v blob, PRIMARY KEY (k, t))", KEYSPACE, TABLE));
    }

    @AfterClass
    public static void tearDownClass()
    {
        QueryProcessor.executeInternal("DROP KEYSPACE IF EXISTS " + KEYSPACE);
    }

    private ColumnFamilyStore getColumnFamilyStore()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
    }

    @Test
    public void testDefaultCompactionWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        try
        {
            int rowCount = 1000;
            cfs.disableAutoCompaction();
            populate(rowCount);
            Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
            long beforeSize = sstables.iterator().next().onDiskLength();
            CompactionAwareWriter writer = new DefaultCompactionWriter(cfs, sstables, sstables, false, OperationType.COMPACTION);
            int rows = compact(cfs, sstables, writer);
            validateData(cfs, rowCount);
            assertEquals(1, cfs.getSSTables().size());
            assertEquals(rowCount, rows);
            assertEquals(beforeSize, cfs.getSSTables().iterator().next().onDiskLength());
        }
        finally
        {
            cfs.truncateBlocking();
        }
    }

    @Test
    public void testMaxSSTableSizeWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        try
        {
            cfs.disableAutoCompaction();
            int rowCount = 1000;
            populate(rowCount);
            Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
            long beforeSize = sstables.iterator().next().onDiskLength();
            int sstableCount = (int)beforeSize/10;
            CompactionAwareWriter writer = new MaxSSTableSizeWriter(cfs, sstables, sstables, sstableCount, 0, false, OperationType.COMPACTION);
            int rows = compact(cfs, sstables, writer);
            validateData(cfs, rowCount);
            assertEquals(10, cfs.getSSTables().size());
            assertEquals(rowCount, rows);
        }
        finally
        {
            cfs.truncateBlocking();
        }
    }

    @Test
    public void testSplittingSizeTieredCompactionWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        try
        {
            cfs.disableAutoCompaction();
            int rowCount = 10000;
            populate(rowCount);
            Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
            long beforeSize = sstables.iterator().next().onDiskLength();
            CompactionAwareWriter writer = new SplittingSizeTieredCompactionWriter(cfs, sstables, sstables, OperationType.COMPACTION, 0);
            int rows = compact(cfs, sstables, writer);
            validateData(cfs, rowCount);
            long expectedSize = beforeSize / 2;
            List<SSTableReader> sortedSSTables = new ArrayList<>(cfs.getSSTables());

            Collections.sort(sortedSSTables, new Comparator<SSTableReader>()
            {
                @Override
                public int compare(SSTableReader o1, SSTableReader o2)
                {
                    return Longs.compare(o2.onDiskLength(), o1.onDiskLength());
                }
            });
            for (SSTableReader sstable : sortedSSTables)
            {
                // we dont create smaller files than this, everything will be in the last file
                if (expectedSize > SplittingSizeTieredCompactionWriter.DEFAULT_SMALLEST_SSTABLE_BYTES)
                    assertEquals(expectedSize, sstable.onDiskLength(), expectedSize / 100); // allow 1% diff in estimated vs actual size
                expectedSize /= 2;
            }
            assertEquals(rowCount, rows);
        }
        finally
        {
            cfs.truncateBlocking();
        }
    }

    @Test
    public void testMajorLeveledCompactionWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        try
        {
            cfs.disableAutoCompaction();
            int rowCount = 20000;
            int targetSSTableCount = 50;
            populate(rowCount);
            Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
            long beforeSize = sstables.iterator().next().onDiskLength();
            int sstableSize = (int)beforeSize/targetSSTableCount;
            CompactionAwareWriter writer = new MajorLeveledCompactionWriter(cfs, sstables, sstables, sstableSize, false, OperationType.COMPACTION);
            int rows = compact(cfs, sstables, writer);
            assertEquals(targetSSTableCount, cfs.getSSTables().size());
            int [] levelCounts = new int[5];
            assertEquals(rowCount, rows);
            for (SSTableReader sstable : cfs.getSSTables())
            {
                levelCounts[sstable.getSSTableLevel()]++;
            }
            assertEquals(0, levelCounts[0]);
            assertEquals(10, levelCounts[1]);
            assertEquals(targetSSTableCount - 10, levelCounts[2]); // note that if we want more levels, fix this
            for (int i = 3; i < levelCounts.length; i++)
                assertEquals(0, levelCounts[i]);
            validateData(cfs, rowCount);
        }
        finally
        {
            cfs.truncateBlocking();
        }
    }

    private int compact(ColumnFamilyStore cfs, Set<SSTableReader> sstables, CompactionAwareWriter writer)
    {
        assert sstables.size() == 1;
        int rowsWritten = 0;
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategy().getScanners(sstables);
             CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(FBUtilities.nowInSeconds()));
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller))
        {
            while (ci.hasNext())
            {
                if (writer.append(ci.next()))
                    rowsWritten++;
            }
        }
        Collection<SSTableReader> newSSTables = writer.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(sstables, newSSTables, OperationType.COMPACTION);
        return rowsWritten;
    }

    private void populate(int count) throws Throwable
    {
        byte [] payload = new byte[1000];
        new Random().nextBytes(payload);
        ByteBuffer b = ByteBuffer.wrap(payload);

        for (int i = 0; i < count; i++)
            for (int j = 0; j < ROW_PER_PARTITION; j++)
                execute(String.format("INSERT INTO %s.%s(k, t, v) VALUES (?, ?, ?)", KEYSPACE, TABLE), i, j, b);

        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.forceBlockingFlush();
        if (cfs.getSSTables().size() > 1)
        {
            // we want just one big sstable to avoid doing actual compaction in compact() above
            try
            {
                cfs.forceMajorCompaction();
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        }
        assert cfs.getSSTables().size() == 1 : cfs.getSSTables();
    }

    private void validateData(ColumnFamilyStore cfs, int rowCount) throws Throwable
    {
        for (int i = 0; i < rowCount; i++)
        {
            Object[][] expected = new Object[ROW_PER_PARTITION][];
            for (int j = 0; j < ROW_PER_PARTITION; j++)
                expected[j] = row(i, j);

            assertRows(execute(String.format("SELECT k, t FROM %s.%s WHERE k = :i", KEYSPACE, TABLE), i), expected);
        }
    }
}
