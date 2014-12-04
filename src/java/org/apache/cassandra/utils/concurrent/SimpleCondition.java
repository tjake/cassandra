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
package org.apache.cassandra.utils.concurrent;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;

// fulfils the Condition interface without spurious wakeup problems
// (or lost notify problems either: that is, even if you call await()
// _after_ signal(), it will work as desired.)
public class SimpleCondition implements Condition
{
    private CountDownLatch latch = new CountDownLatch(1);

    public void await() throws InterruptedException
    {
        latch.await();
    }

    public boolean await(long time, TimeUnit unit) throws InterruptedException
    {
        return latch.await(time, unit);
    }

    public void signal()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isSignaled()
    {
        return latch.getCount() == 0;
    }

    public void signalAll()
    {
        latch.countDown();
    }

    public void awaitUninterruptibly()
    {
        throw new UnsupportedOperationException();
    }

    public long awaitNanos(long nanosTimeout) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    public boolean awaitUntil(Date deadline) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }
}
