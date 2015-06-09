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

package org.apache.cassandra.cql3;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.apache.cassandra.db.BatchlogManager;
import org.apache.cassandra.utils.WrappedRunnable;
//import org.jboss.byteman.contrib.bmunit.BMRule;
// import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

//@RunWith(BMUnitRunner.class)
public class MaterializedViewLongTest extends CQLTester
{
    int protocolVersion = 3;

    public static int slowdown = 3;

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }

    @Test
    /*@BMRule(name = "Pause Lock Acquisition Rule",
           targetClass = "org.apache.cassandra.db.view.MaterializedViewManager",
           targetMethod = "acquireLockFor",
           targetLocation = "AT EXIT",
           condition = "TRUE",
           action = "com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly(org.apache.cassandra.cql3.MaterializedViewLongTest.slowdown, TimeUnit.SECONDS);")
    */public void testSlowLockAcquisition() throws Throwable
    {
        final int writers = 96;
        final int insertsPerWriter = 50;
        final Map<Integer, Exception> failedWrites = new ConcurrentHashMap<>();

        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "CREATE MATERIALIZED VIEW " + keyspace() + ".mv_1 AS SELECT * FROM %s PRIMARY KEY (c)");

        CyclicBarrier semaphore = new CyclicBarrier(writers);

        Thread[] threads = new Thread[writers];
        for (int i = 0; i < writers; i++)
        {
            final int writer = i;
            Thread t = new Thread(new WrappedRunnable()
            {
                public void runMayThrow()
                {
                    try
                    {
                        int writerOffset = writer * insertsPerWriter;
                        semaphore.await();
                        for (int i = 0; i < insertsPerWriter; i++)
                        {
                            try
                            {
                                executeNet(protocolVersion, "INSERT INTO %s (a, b, c) VALUES (?, ?, ?)",
                                           1,
                                           1,
                                           i + writerOffset);
                            }
                            catch (NoHostAvailableException|WriteTimeoutException e)
                            {
                                failedWrites.put(i + writerOffset, e);
                            }
                        }
                    }
                    catch (Throwable e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            });
            t.start();
            threads[i] = t;
        }

        for (int i = 0; i < writers; i++)
            threads[i].join();

        slowdown = 0;

        for (int i = 0; i < writers * insertsPerWriter; i++)
        {
            if (executeNet(protocolVersion, "SELECT COUNT(*) FROM system.batchlog").one().getLong(0) == 0)
                break;
            try
            {
                BatchlogManager.instance.startBatchlogReplay().get();
            }
            catch (Throwable ignore)
            {

            }
        }

        int value = executeNet(protocolVersion, "SELECT c FROM %s WHERE a = 1 AND b = 1").one().getInt("c");

        List<Row> rows = executeNet(protocolVersion, "SELECT c FROM " + keyspace() + ".mv_1").all();

        boolean containsC = false;
        StringBuilder others = new StringBuilder();
        StringBuilder overlappingFailedWrites = new StringBuilder();
        for (Row row : rows)
        {
            int c = row.getInt("c");
            if (c == value)
                containsC = true;
            else
            {
                if (others.length() != 0)
                    others.append(' ');
                others.append(c);
                if (failedWrites.containsKey(c))
                {
                    if (overlappingFailedWrites.length() != 0)
                        overlappingFailedWrites.append(' ');
                    overlappingFailedWrites.append(c)
                                           .append(':')
                                           .append(failedWrites.get(c).getMessage());
                }
            }
        }

        if (rows.size() > 1)
        {
            throw new AssertionError(String.format("Expected 1 row, but found %d; %s c = %d, and (%s) of which (%s) failed to insert", rows.size(), containsC ? "found row with" : "no rows contained", value, others, overlappingFailedWrites));
        }
        else if (rows.isEmpty())
        {
            throw new AssertionError(String.format("Could not find row with c = %d", value));
        }
        else if (rows.size() == 1 && !containsC)
        {
            throw new AssertionError(String.format("Single row had c = %d, expected %d", rows.get(0).getInt("c"), value));
        }
    }
}
