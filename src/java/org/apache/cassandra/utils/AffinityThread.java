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

package org.apache.cassandra.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocalThread;

/**
 * A thread that is aware of it's linux thread id
 */
public class AffinityThread extends FastThreadLocalThread
{
    private static Logger logger = LoggerFactory.getLogger(AffinityThread.class);
    private long nativeThreadId = -1;
    private volatile long currentMask = AffinityThreadGroup.ALL_CORES;

    public AffinityThread(ThreadGroup threadGroup, Runnable runnable, String name)
    {
        super(threadGroup, runnable, name);
    }

    public void run()
    {
        nativeThreadId = CLibrary.tryGetThreadId();

        if (getThreadGroup() instanceof AffinityThreadGroup)
            setAffinity(((AffinityThreadGroup) getThreadGroup()).getAffinityMask());

        super.run();
    }

    public long getNativeThreadId()
    {
        return nativeThreadId;
    }

    public boolean setAffinity(long affinity)
    {
        if (affinity == currentMask)
            return false;

        if (nativeThreadId == -1)
        {
            logger.error("FAILED Missing threadid thread {}", getName());
            return false;
        }

        currentMask = affinity;
        boolean succeded = CLibrary.trySetAffinity(nativeThreadId, affinity);

        if (!succeded)
            logger.error("FAILED Setting thread {} affinity to {}", getName(), Long.toBinaryString(affinity));
        else
            logger.debug("Setting thread {} tid {} affinity to {}", getName(), nativeThreadId, Long.toBinaryString(affinity));

        return succeded;
    }

}
