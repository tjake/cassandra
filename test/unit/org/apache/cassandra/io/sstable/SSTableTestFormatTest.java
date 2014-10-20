/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.sstable;

import com.google.common.io.Files;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


@RunWith(OrderedJUnit4ClassRunner.class)
public class SSTableTestFormatTest
{

    static class TestBuilder extends CQLSSTableWriter.Builder
    {
        public  TestBuilder withSSTableFormat(SSTableFormat.Type formatType)
        {
            this.formatType = formatType;
            return this;
        }
    }

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    @Test
    public void testSSTableSimpleUnsortedWriter() throws Exception
    {
        String KS = "cql_keyspace";
        String TABLE = "table1";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        String schema = "CREATE TABLE cql_keyspace.table1 ("
                + "  k int PRIMARY KEY,"
                + "  v1 text,"
                + "  v2 int"
                + ")";
        String insert = "INSERT INTO cql_keyspace.table1 (k, v1, v2) VALUES (?, ?, ?)";

        TestBuilder builder = (TestBuilder) new TestBuilder()
                .inDirectory(dataDir)
                .forTable(schema)
                .withPartitioner(StorageService.instance.getPartitioner())
                .using(insert);

        CQLSSTableWriter writer = builder.withSSTableFormat(SSTableFormat.Type.TEST).build();

        writer.addRow(0, "test1", 24);
        writer.addRow(1, "test2", 77);
        writer.addRow(2, "test3", 42);
       // writer.addRow(ImmutableMap.<String, Object>of("k", 3, "v2", 12));
        writer.close();

        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            public void init(String keyspace)
            {
                for (Range<Token> range : StorageService.instance.getLocalRanges("cql_keyspace"))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
                setPartitioner(StorageService.getPartitioner());
            }

            public CFMetaData getCFMetaData(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        loader.stream().get();

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace.table1;");
        assertEquals(3, rs.size());

        Iterator<UntypedResultSet.Row> iter = rs.iterator();
        UntypedResultSet.Row row;

        row = iter.next();
        assertEquals(0, row.getInt("k"));
        assertEquals("test1", row.getString("v1"));
        assertEquals(24, row.getInt("v2"));

        row = iter.next();
        assertEquals(1, row.getInt("k"));
        assertEquals("test2", row.getString("v1"));
        assertEquals(77, row.getInt("v2"));

        row = iter.next();
        assertEquals(2, row.getInt("k"));
        assertEquals("test3", row.getString("v1"));
        assertEquals(42, row.getInt("v2"));


        rs = QueryProcessor.executeInternal("SELECT v1 FROM cql_keyspace.table1 where k = 2;");
        assertEquals(1, rs.size());

       /* row = iter.next();
        assertEquals(3, row.getInt("k"));
        assertEquals(null, row.getBytes("v1")); // Using getBytes because we know it won't NPE
        assertEquals(12, row.getInt("v2"));*/
    }


    @Test
    public void testCompaction() throws Exception
    {
        String KS = "cql_keyspace";
        String TABLE = "table1";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        String schema = "CREATE TABLE cql_keyspace.table1 ("
                + "  k int PRIMARY KEY,"
                + "  v1 text,"
                + "  v2 int"
                + ")";
        String insert = "INSERT INTO cql_keyspace.table1 (k, v1, v2) VALUES (?, ?, ?)";

        for (int i=0; i < 4; i++)
        {

            TestBuilder builder = (TestBuilder) new TestBuilder()
                    .inDirectory(dataDir)
                    .forTable(schema)
                    .withPartitioner(StorageService.instance.getPartitioner())
                    .using(insert);

            CQLSSTableWriter writer = builder.withSSTableFormat(SSTableFormat.Type.TEST).build();

            for (int j = 0; j < 1024; j++)
                writer.addRow((10000*i) + j, "test" + j, j);

            writer.close();

            SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
            {
                public void init(String keyspace)
                {
                    for (Range<Token> range : StorageService.instance.getLocalRanges("cql_keyspace"))
                        addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
                    setPartitioner(StorageService.getPartitioner());
                }

                public CFMetaData getCFMetaData(String keyspace, String cfName)
                {
                    return Schema.instance.getCFMetaData(keyspace, cfName);
                }
            }, new OutputHandler.SystemOutput(false, false));

            loader.stream().get();
        }

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace.table1;");
        assertEquals(4096, rs.size());



        ColumnFamilyStore store = Keyspace.open(KS).getColumnFamilyStore(TABLE);
        int prevSize = store.getSSTables().size();

        assert prevSize > 1;

        // enable compaction, submit background and wait for it to complete
        Keyspace.open(KS).getColumnFamilyStore(TABLE).forceMajorCompaction();

        while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0)
            TimeUnit.SECONDS.sleep(1);

        // and sstable with ttl should be compacted
        assertEquals(1, store.getSSTables().size());
        assertEquals(SSTableFormat.Type.TEST, store.getSSTables().iterator().next().descriptor.formatType);

        long size = store.getSSTables().iterator().next().uncompressedLength();

        rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace.table1;");
        assertEquals(4096, rs.size());
    }


}
