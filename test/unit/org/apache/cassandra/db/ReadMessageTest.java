/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import static org.junit.Assert.*;

import java.io.*;

import com.google.common.base.Predicate;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.PartitionRangeReadBuilder;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.SinglePartitionNamesReadBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.commitlog.CommitLogTestReplayer;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;


public class ReadMessageTest
{
    private static final String KEYSPACE1 = "ReadMessageTest1";
    private static final String KEYSPACENOCOMMIT = "ReadMessageTest_NoCommit";
    private static final String CF = "Standard1";
    private static final String CF_FOR_READ_TEST = "Standard2";
    private static final String CF_FOR_COMMIT_TEST = "Standard3";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CFMetaData cfForReadMetadata = CFMetaData.Builder.create(KEYSPACE1, CF_FOR_READ_TEST)
                                                            .addPartitionKey("key", BytesType.instance)
                                                            .addClusteringColumn("col1", AsciiType.instance)
                                                            .addClusteringColumn("col2", AsciiType.instance)
                                                            .addRegularColumn("a", AsciiType.instance)
                                                            .addRegularColumn("b", AsciiType.instance).build();

        CFMetaData cfForCommitMetadata1 = CFMetaData.Builder.create(KEYSPACE1, CF_FOR_COMMIT_TEST)
                                                       .addPartitionKey("key", BytesType.instance)
                                                       .addClusteringColumn("name", AsciiType.instance)
                                                       .addRegularColumn("commit1", AsciiType.instance).build();

        CFMetaData cfForCommitMetadata2 = CFMetaData.Builder.create(KEYSPACENOCOMMIT, CF_FOR_COMMIT_TEST)
                                                            .addPartitionKey("key", BytesType.instance)
                                                            .addClusteringColumn("name", AsciiType.instance)
                                                            .addRegularColumn("commit2", AsciiType.instance).build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF),
                                    cfForReadMetadata,
                                    cfForCommitMetadata1);
        SchemaLoader.createKeyspace(KEYSPACENOCOMMIT,
                                    false,
                                    true,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACENOCOMMIT, CF),
                                    cfForCommitMetadata2);
    }

    @Test
    public void testMakeReadMessage() throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_FOR_READ_TEST);
        ReadCommand rm, rm2;

        rm = new SinglePartitionNamesReadBuilder(cfs, FBUtilities.nowInSeconds(), Util.dk("key1"))
                .addClustering("col1")
                .addClustering("col2")
                .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SinglePartitionNamesReadBuilder(cfs, FBUtilities.nowInSeconds(), Util.dk("key1"))
                .addClustering("col1")
                .addClustering("col2")
                .setReversed(true)
                .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .addClustering("col1", "col2")
                .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("key1"), ByteBufferUtil.bytes("key2"))
                .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .addColumn(ByteBufferUtil.bytes("a"))
                .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .addClustering("col1", "col2")
                .addColumn(ByteBufferUtil.bytes("a"))
                .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("key1"), null)
                .addClustering("col1", "col2")
                .addColumn(ByteBufferUtil.bytes("a"))
                .build();
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());
    }

    private ReadCommand serializeAndDeserializeReadMessage(ReadCommand rm) throws IOException
    {
        IVersionedSerializer<ReadCommand> rms = ReadCommand.serializer;
        DataOutputBuffer out = new DataOutputBuffer();
        ByteArrayInputStream bis;

        rms.serialize(rm, out, MessagingService.current_version);

        bis = new ByteArrayInputStream(out.getData(), 0, out.getLength());
        return rms.deserialize(new DataInputStream(bis), MessagingService.current_version);
    }


    @Test
    public void testGetColumn()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF);

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key1"))
                .clustering("Column1")
                .add("val", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        ColumnDefinition col = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));
        try (PartitionIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .build().executeInternal())
        {
            int found = 0;
            while (iter.hasNext())
            {
                RowIterator ri = iter.next();
                while (ri.hasNext())
                {
                    Row r = ri.next();
                    if (r.getCell(col).value().equals(ByteBufferUtil.bytes("abcd")))
                        ++found;
                }
            }
            assertEquals(1, found);
        }
    }

    @Test
    public void testNoCommitLog() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_FOR_COMMIT_TEST);

        ColumnFamilyStore cfsnocommit = Keyspace.open(KEYSPACENOCOMMIT).getColumnFamilyStore(CF_FOR_COMMIT_TEST);

        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("row"))
                .clustering("c")
                .add("commit1", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        new RowUpdateBuilder(cfsnocommit.metadata, 0, ByteBufferUtil.bytes("row"))
                .clustering("c")
                .add("commit2", ByteBufferUtil.bytes("abcd"))
                .build()
                .apply();

        Checker checker = new Checker(cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("commit1")),
                                      cfsnocommit.metadata.getColumnDefinition(ByteBufferUtil.bytes("commit2")));
        CommitLogTestReplayer.examineCommitLog(checker);

        assertTrue(checker.commitLogMessageFound);
        assertFalse(checker.noCommitLogMessageFound);
    }

    static class Checker implements Predicate<Mutation>
    {
        private final ColumnDefinition withCommit;
        private final ColumnDefinition withoutCommit;

        boolean commitLogMessageFound = false;
        boolean noCommitLogMessageFound = false;

        public Checker(ColumnDefinition withCommit, ColumnDefinition withoutCommit)
        {
            this.withCommit = withCommit;
            this.withoutCommit = withoutCommit;
        }

        public boolean apply(Mutation mutation)
        {
            for (PartitionUpdate upd : mutation.getPartitionUpdates())
            {
                Row r = upd.getRow(new SimpleClustering(ByteBufferUtil.bytes("c")));
                if (r != null)
                {
                    if (r.getCell(withCommit) != null)
                        commitLogMessageFound = true;
                    if (r.getCell(withoutCommit) != null)
                        noCommitLogMessageFound = true;
                }
            }
            return true;
        }
    }
}
