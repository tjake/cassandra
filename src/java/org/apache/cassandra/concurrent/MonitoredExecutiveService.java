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
package org.apache.cassandra.concurrent;

import com.google.common.collect.Lists;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Executor Service based on idea from Martin Thompson.
 *
 * Threads look for work in their queue. If they can't find any
 * work they park themselves.
 *
 * A single background monitor thread watches the queue for each executor in a loopr.
 * When the queue is not empty it unparks a thread then sleeps.
 *
 * This service incorporates work stealing by splitting Threads from Queues. Meaning,
 * any thread can fetch from any other executor queue once it's finished with it's local
 * queue.
 */
public class MonitoredExecutiveService extends AbstractTracingAwareExecutorService
{
    private static final Logger logger = LoggerFactory.getLogger(MonitoredExecutiveService.class);

    private static Thread monitorThread;

    private static final List<MonitoredExecutiveService> monitoredExecutiveServices = Lists.newCopyOnWriteArrayList();
    private static final ThreadFactory threadFactory = new NamedThreadFactory("monitored-executor-service-worker");
    private static final int globalMaxThreads = 256;
    private static ThreadWorker[] allWorkers;
    private static Thread[] allThreads;

    final String name;
    final int maxThreads;
    final int maxQueuedItems;
    final Queue<FutureTask<?>> workQueue;

    // Tracks the number of items in the queued and number running
    final AtomicInteger queuedItems = new AtomicInteger(0);
    final AtomicInteger activeItems = new AtomicInteger(0);
    int lastNumberQueued = 0;

    volatile boolean shuttingDown = false;
    final SimpleCondition shutdown = new SimpleCondition();

    public MonitoredExecutiveService(String name, int maxThreads, int maxQueuedItems)
    {
        super(name);

        workQueue = new ConcurrentLinkedQueue<>();
        this.name = name;
        this.maxQueuedItems = maxQueuedItems;
        this.maxThreads = maxThreads;

        startMonitoring(this);
    }

    /**
     * Kicks off the monitoring of a new ES.  Also spawns worker threads and monitor when first called.
     * @param service
     */
    private static synchronized void startMonitoring(MonitoredExecutiveService service)
    {
        monitoredExecutiveServices.add(service);

        // Start workers on first call
        if (allWorkers == null)
        {
            allWorkers = new ThreadWorker[globalMaxThreads];
            allThreads = new Thread[globalMaxThreads];

            for (int i = 0; i < globalMaxThreads; i++)
            {
                allWorkers[i] = new ThreadWorker(i);
                allThreads[i] = threadFactory.newThread(allWorkers[i]);
            }

            for (int i = 0; i < globalMaxThreads; i++)
            {
                allThreads[i].setDaemon(true);
                allThreads[i].start();
            }
        }

        // Start monitor on first call
        if (monitorThread == null)
        {
            monitorThread = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    while (true)
                    {
                        for (int i = 0, length = monitoredExecutiveServices.size(); i < length; i++)
                        {
                            monitoredExecutiveServices.get(i).checkQueue();
                        }

                        LockSupport.parkNanos(1);
                    }
                }
            });

            monitorThread.setName("monitor-executor-service-thread");
            monitorThread.setDaemon(true);
            monitorThread.start();
        }
    }

    /**
     * @return A unit of work if there are idle slots and queued work
     */
    FutureTask<?> takeWorkPermit()
    {
        if (activeItems.incrementAndGet() > maxThreads)
        {
            activeItems.decrementAndGet();
            return null;
        }

        FutureTask<?> work = workQueue.poll();
        if (work == null)
        {
            returnWorkPermit();
        }
        else
        {
            queuedItems.decrementAndGet();
        }

        return work;
    }

    /**
     * Give a work slot back
     */
    void returnWorkPermit()
    {
        activeItems.decrementAndGet();
    }

    static class ThreadWorker implements Runnable
    {
        enum State
        {
            PARKED, WORKING
        }

        public volatile State state;
        public volatile MonitoredExecutiveService primary;
        public final int threadId;

        ThreadWorker(int threadId)
        {
            this.threadId = threadId;
            this.state = State.WORKING;
        }

        @Override
        public void run()
        {

            while (true)
            {
                FutureTask<?> t;

                //deal with spurious wakeups
                if (state == State.PARKED)
                {
                    park();
                }
                else
                {
                    //Find/Steal work then park self
                    while ((t = findWork()) != null)
                    {
                        t.run();
                    }

                    park();
                }
            }
        }

        /**
         * Looks for work on a initial queue.  If nothing is found
         * steals work from other queues.
         *
         * @return A task to work on
         */
        private FutureTask findWork()
        {

            // Work on the requested queue
            if (primary != null)
            {
                FutureTask<?> work = primary.takeWorkPermit();
                primary = null;
                if (work != null)
                    return work;
            }

            // Steal from all other executor queues
            for (int i = 0, length = monitoredExecutiveServices.size(); i < length; i++)
            {
                // avoid all threads checking in the same order
                int idx = (threadId + i) % length;
                MonitoredExecutiveService executor = monitoredExecutiveServices.get(idx);

                FutureTask<?> work = executor.takeWorkPermit();
                if (work != null)
                    return work;
            }

            return null;
        }

        public void park()
        {
            state = State.PARKED;
            LockSupport.park();
        }

        public void unpark(MonitoredExecutiveService executor)
        {
            state = State.WORKING;
            this.primary = executor;
            LockSupport.unpark(allThreads[threadId]);
        }
    }

    private void checkQueue()
    {
        if (activeItems.get() >= maxThreads)
            return;

        int numberQueued = queuedItems.get();

        if (numberQueued > 0 && (numberQueued >= lastNumberQueued || activeItems.get() == 0))
        {
            int unparked = 0;
            for (int i = 0; i < allWorkers.length; i++) {
                ThreadWorker t = allWorkers[i];
                if (t.state == ThreadWorker.State.PARKED)
                {
                    t.unpark(this);
                    if (lastNumberQueued == numberQueued || unparked++ >= numberQueued)
                        break;
                }
            }
        }

        lastNumberQueued = numberQueued;
    }

    @Override
    protected void addTask(FutureTask<?> futureTask)
    {
        if (shuttingDown == true)
            throw new RuntimeException("ExecutorService has shutdown : " + name);

        int queueLength = queuedItems.incrementAndGet();

        if (queueLength <= maxQueuedItems)
        {
            workQueue.add(futureTask);
        }
        else
        {
            queuedItems.decrementAndGet();
            throw new RuntimeException("Queue is full for ExecutorService : " + name);
        }
    }

    @Override
    protected void onCompletion()
    {
        returnWorkPermit();
    }

    @Override
    public void maybeExecuteImmediately(Runnable command)
    {
        FutureTask<?> work = newTaskFor(command, null);

        if (activeItems.incrementAndGet() <= maxThreads)
        {
            work.run();
        }
        else
        {
            activeItems.decrementAndGet();
            addTask(work);
        }

    }

    @Override
    public void shutdown()
    {
        shuttingDown = true;
        if (queuedItems.get() == 0 && activeItems.get() == 0)
            shutdown.signalAll();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        shutdown();
        List<Runnable> aborted = new ArrayList<>();
        FutureTask<?> work;
        while ((work = workQueue.poll()) != null)
            aborted.add(work);

        return aborted;
    }

    @Override
    public boolean isShutdown()
    {
        return shuttingDown;
    }

    @Override
    public boolean isTerminated()
    {
        return shuttingDown && shutdown.isSignaled();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        shutdown.await(timeout, unit);
        return isTerminated();
    }
}
