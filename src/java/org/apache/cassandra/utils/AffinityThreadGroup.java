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

/**
 * A thread group that can have its thread affinity set
 */
public class AffinityThreadGroup extends ThreadGroup
{
    private static final Logger logger = LoggerFactory.getLogger(AffinityThreadGroup.class);
    public static final long ALL_CORES = ~0L;
    private long affinityMask = ALL_CORES;

    public AffinityThreadGroup(String name)
    {
        super(name);
    }

    public long getAffinityMask()
    {
        return affinityMask;
    }

    public void setAffinityMask(long affinityMask)
    {
        if (this.affinityMask == affinityMask)
            return;

        this.affinityMask = affinityMask;

        Thread[] threads = new Thread[activeCount()];
        int totalThreads;
        while((totalThreads = enumerate(threads, false)) == threads.length)
        {
            threads = new Thread[threads.length * 2];
        }

        for (int i = 0; i < totalThreads; i++)
        {
            Thread t = threads[i];
            if (!(t instanceof AffinityThread))
            {
                logger.warn("Thread in Affinity group isn't an AffinityThread: {}", t.getName());
                continue;
            }

            ((AffinityThread)t).setAffinity(affinityMask);
        }
    }
}
