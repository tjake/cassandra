package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Row;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Simulate a slow client to verify we don't delete BB from under
 */
@RunWith(BMUnitRunner.class)
public class SlowClientTest extends SchemaLoader
{
    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;
    private static PreparedStatement pstmtI;
    private static PreparedStatement pstmt1;
    public static volatile boolean inject = false;

    @BeforeClass
    public static void setup() throws Exception
    {
        Schema.instance.clear();

        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        //Stop thrift access to enable zero-copy mmaps
        StorageService.instance.stopRPCServer();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("drop keyspace if exists junit;");
        session.execute("create keyspace junit WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };");
        session.execute("CREATE TABLE junit.mmap (\n" +
                "  id int PRIMARY KEY,\n" +
                "  cid int ,\n" +
                "  val blob \n" +
                ") with COMPRESSION = {'sstable_compression' : ''};");

        pstmtI = session.prepare("insert into junit.mmap ( id, cid, val) values (?, ?, ?)");
        pstmt1 = session.prepare("select id, cid, val from junit.mmap where id=?");
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        cluster.close();
    }

    @BMRule(name = "Pause Select Rule",
            targetClass = "org.apache.cassandra.cql3.statements.SelectStatement",
            targetMethod = "execute",
            targetLocation = "AT EXIT",
            condition = "org.apache.cassandra.cql3.SlowClientTest.inject",
            action = "com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);")
    @Test
    public void mmappedNativeReadTestExecute() throws Exception
    {
        ByteBuffer data = ByteBuffer.allocate(100000);
        new Random().nextBytes(data.array());

        inject = false;

        ColumnFamilyStore cfs = Keyspace.open("junit").getColumnFamilyStore("mmap");

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        inject = true;

        ResultSetFuture future = session.executeAsync(pstmt1.bind(1));

        Assert.assertTrue(cfs.getSSTables().size() > 1);

        while(cfs.getSSTables().size() != 1)
            cfs.forceMajorCompaction();

        com.datastax.driver.core.ResultSet result = future.getUninterruptibly();

        Assert.assertArrayEquals(data.array(), ByteBufferUtil.getArray(result.one().getBytes("val")));
    }

    @BMRule(name = "Pause Select Rule",
            targetClass = "org.apache.cassandra.cql3.statements.SelectStatement",
            targetMethod = "execute",
            targetLocation = "AT EXIT",
            condition = "org.apache.cassandra.cql3.SlowClientTest.inject",
            action = "com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);")
    @Test
    public void mmappedNativeReadTestQuery() throws Exception
    {
        ByteBuffer data = ByteBuffer.allocate(100000);
        new Random().nextBytes(data.array());

        inject = false;

        ColumnFamilyStore cfs = Keyspace.open("junit").getColumnFamilyStore("mmap");

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        inject = true;

        ResultSetFuture future = session.executeAsync("select id, cid, val from junit.mmap where id=1");

        Assert.assertTrue(cfs.getSSTables().size() > 1);

        while(cfs.getSSTables().size() != 1)
            cfs.forceMajorCompaction();

        com.datastax.driver.core.ResultSet result = future.getUninterruptibly();

        Assert.assertArrayEquals(data.array(), ByteBufferUtil.getArray(result.one().getBytes("val")));
    }


    @Test
    @BMRule(name = "Pause Select Rule",
            targetClass = "StorageProxy",
            targetMethod = "read",
            targetLocation = "AT EXIT",
            condition = "true",
            action = "com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);")
    public void mmappedMessageServiceReadTest() throws Exception
    {
        ByteBuffer data = ByteBuffer.allocate(100000);
        new Random().nextBytes(data.array());

        inject = false;

        ExecutorService workThread = Executors.newSingleThreadExecutor();
        final ColumnFamilyStore cfs = Keyspace.open("junit").getColumnFamilyStore("mmap");

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        session.execute(pstmtI.bind(1, 1, data));

        cfs.forceBlockingFlush();

        Assert.assertTrue(cfs.getSSTables().size() > 1);

        final CellName cellName = cfs.getComparator().makeCellName(ByteBufferUtil.bytes("val"));

        Callable<org.apache.cassandra.db.Row> callable = new Callable<org.apache.cassandra.db.Row>()
        {
            @Override
            public org.apache.cassandra.db.Row call() throws Exception
            {
                 return StorageProxy.read(Collections.<ReadCommand>singletonList(
                         new SliceByNamesReadCommand("junit", ByteBufferUtil.bytes(1), "mmap", System.currentTimeMillis(),
                                 new NamesQueryFilter(FBUtilities.singleton(cellName, cfs.getComparator()), true))),
                         ConsistencyLevel.ONE)
                         .iterator()
                         .next();
            }
        };


        StorageProxy.OPTIMIZE_LOCAL_REQUESTS = false;


        Future<org.apache.cassandra.db.Row> result = workThread.submit(callable);

        while(cfs.getSSTables().size() != 1)
            cfs.forceMajorCompaction();


        org.apache.cassandra.db.Row row = result.get();
        Assert.assertNotNull(row);
        Assert.assertNotNull(row.cf);
        Assert.assertArrayEquals(data.array(), ByteBufferUtil.getArray(row.cf.getColumn(cellName).value()));
    }

}