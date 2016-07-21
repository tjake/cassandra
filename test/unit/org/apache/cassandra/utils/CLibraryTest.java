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
package org.apache.cassandra.utils;

import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FileUtils;
import org.hyperic.sigar.CpuInfo;

public class CLibraryTest
{
    private static Logger logger = LoggerFactory.getLogger(CLibrary.class);

    @Test
    public void testSkipCache()
    {
        File file = FileUtils.createTempFile("testSkipCache", "1");

        CLibrary.trySkipCache(file.getPath(), 0, 0);
    }

    @Test
    public void testThreadId()
    {
        if (!FBUtilities.isWindows())
        {
            long tid = CLibrary.tryGetThreadId();
            Assert.assertTrue(tid > 0);
        }
    }


    @Test
    public void testThreadAffinity()
    {
        if (!FBUtilities.isWindows())
        {
            int numCores = Runtime.getRuntime().availableProcessors();
            long tid = CLibrary.tryGetThreadId();

            for (int i = 0; i < numCores; i++)
            {
                CLibrary.trySetAffinity(tid, 1L << i);
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                Assert.assertEquals(i, CLibrary.tryGetCpu());
            }
        }
    }


    @Test
    public void testThreadAffinityRange()
    {
        if (!FBUtilities.isWindows())
        {
            int numCores = Runtime.getRuntime().availableProcessors();
            long tid = CLibrary.tryGetThreadId();

            //Burn baby burn
            for (int i = 0; i < numCores/2; i++)
                new Thread(() -> {while(true);}).start();


            logger.info("Starting on core {}", CLibrary.tryGetCpu());
            long mask = 0L;
            for (int i = 0; i < numCores; i++)
            {
                mask |= 1L << i;
                CLibrary.trySetAffinity(tid, mask);
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                int cpu = CLibrary.tryGetCpu();
                logger.info("Running on core {}", cpu);
                Assert.assertTrue(cpu <= i);
            }
        }
    }
}
